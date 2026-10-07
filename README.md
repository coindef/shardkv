# ShardKV

A small distributed in-memory key-value cache in C++17: Redis-protocol server
nodes with a lock-striped LRU store, TTLs, and an append-only log, plus a
client library that spreads keys over the nodes with consistent hashing and
replication. It uses POSIX sockets and `std::thread` only, with no third-party
libraries.

## Design

```
      kvcli / kvbench / your program
                    |
        client.h: shardkv::Cluster
        hash ring, 100 virtual nodes per server
        key -> preference list = first R distinct nodes clockwise
          |                  |                  |
   +--------------+   +--------------+   +--------------+
   | kvserver :A  |   | kvserver :B  |   | kvserver :C  |
   +--------------+   +--------------+   +--------------+

   inside one kvserver
   +-------------------------------------------------------------+
   | accept loop -> one thread per connection                    |
   |   RESP parser (handles pipelining and partial reads)        |
   |        |                                                     |
   | Store: 16 shards, each = mutex + LRU list + hash map        |
   |   hash(key) % 16 picks the shard; capacity split per shard  |
   |        | every SET/DEL is logged while the shard lock is held|
   | AOF: write() per record; fsync thread every --fsync-ms      |
   | sweeper thread: drops expired keys once per second          |
   +-------------------------------------------------------------+
```

| File | What it does |
|------|--------------|
| `src/resp.h` | RESP2 encoder/parser. The parser never consumes a partial frame and reports `kIncomplete` |
| `src/store.h` | Sharded LRU with TTL, lazy and active expiry, and stats |
| `src/aof.h` | Append-only log with CRC'd records, group fsync, and torn-tail recovery |
| `src/kvserver.cpp` | TCP server and command dispatch |
| `src/client.h` | Header-only cluster client: hash ring, replication, failover, reconnect |
| `src/kvcli.cpp` | CLI built on the client library |
| `src/kvbench.cpp` | Multithreaded load generator that reports ops/s and p50/p95/p99 |

### Storage engine
- **Lock striping.** Keys hash to one of N shards (default 16). Each shard has
  its own `std::mutex`, so threads working on different shards never contend.
- **O(1) LRU per shard.** A `std::list` is kept in recency order next to an
  `unordered_map<string_view, list::iterator>`. The map's keys point into the
  list nodes, so each key is stored once. A hit moves the node to the front
  with `splice`. When a shard goes over its byte budget (`--max-mb` / shards),
  entries are evicted from the back.
- **TTL.** Each entry holds an absolute wall-clock expiry time. Reads expire
  keys lazily, and a sweeper thread scans for expired keys once per second.
  Because expiry times are absolute, they still hold after an AOF replay.

### Durability (AOF)
- Record format: `[u32 len][u32 crc32][payload]`. The payload is the command
  encoded as a RESP array (`SET k v [PXAT ms]` or `DEL k`).
- Each record is `write()`n to the file right away, so a process crash
  (SIGKILL) loses nothing. A background thread `fsync`s every `--fsync-ms`
  (default 1000), so a power loss loses at most that window. One fsync covers
  every append since the previous one (group commit).
- The log append happens inside the shard lock. This keeps the log order for
  each key the same as the in-memory order.
- **Replay** runs on startup and re-applies records in order. It stops at the
  first record that is truncated or fails its CRC, which is what a crash in
  the middle of a write leaves behind. It then `ftruncate`s the file to the
  last good record so new appends follow valid data.

### Cluster and consistency model
- **Placement.** Each server gets 100 virtual nodes on a 64-bit ring (FNV-1a
  plus the murmur3 finalizer). A key's preference list is the first R
  distinct servers clockwise from its hash. R defaults to 2. When a node is
  added, only about 1/N of the keys move, and they all move to the new node.
  The unit tests check this.
- **Writes** go to all R replicas one after another and succeed if at least
  one acks (W=1).
- **Reads** try the preference list in order. A replica that answers wins,
  whether with a hit or a miss. On a connect/IO error or timeout (default
  500 ms), the client fails over to the next replica.
- **Reconnect.** A failed node is skipped for 1 s, then retried with a fresh
  connection. A pooled connection that went stale (for example because the
  node restarted) gets one immediate retry on a new socket.
- This gives **eventual consistency at best, with no conflict resolution**.
  Each replica keeps whatever it last received, and concurrent writers can
  leave replicas with different values.

## Build, run, test

Requires clang++ (C++17), make, and python3. Builds on macOS and Linux.

```sh
make              # build/kvserver build/kvcli build/kvbench build/unit_test
make test         # unit tests + 3-node integration test
make tsan         # same tests, everything built with -fsanitize=thread
```

```sh
build/kvserver --port 7001 --aof n1.aof &
build/kvserver --port 7002 --aof n2.aof &
build/kvserver --port 7003 --aof n3.aof &
N=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
build/kvcli --nodes $N set user:1 alice
build/kvcli --nodes $N get user:1
build/kvcli --nodes $N set session:9 xyz 5000   # TTL in ms
build/kvbench --nodes $N --threads 8 --keys 100000 --value-size 100 --get-ratio 0.9 --duration 10
redis-cli -p 7001 info                          # any single node speaks RESP
```

Server flags: `--port 6380 --bind 127.0.0.1 --aof FILE --fsync-ms 1000 --max-mb 256 --shards 16`.

**What the tests cover.** `tests/unit_test.cpp` uses plain asserts. It covers
the RESP parser on every partial prefix and on malformed input, LRU eviction
order, lazy and active TTL expiry, AOF replay after a truncated or corrupt
record, the balance and minimal key movement of the hash ring, and an 8-thread
stress test of the store. `tests/integration_test.py` does the following:
1. Starts 3 nodes.
2. Checks pipelining, partial reads, and malformed input on the raw protocol.
3. Writes 300 keys through `kvcli` and checks that each key is on exactly 2
   nodes.
4. `SIGKILL`s one node and reads every key back through failover.
5. Restarts that node and checks that AOF replay restored exactly its keys.
6. Runs a short `kvbench`.

## Protocol

This is a RESP2 subset. Requests must be arrays of bulk strings, which is what
redis-cli and every Redis client send. Inline commands are not supported.

| Command | Reply |
|---------|-------|
| `PING` | `+PONG` |
| `GET key` | bulk string or `$-1` (nil) |
| `SET key value [PX ms \| PXAT unix-ms]` | `+OK` |
| `DEL key [key ...]` | `:n` keys removed |
| `DBSIZE` | `:n` (may include expired keys the sweeper has not reached yet) |
| `INFO` / `STATS` | bulk string with `keys`, `used_bytes`, `hits`, `misses`, `evictions`, `expired` |

When the server gets malformed framing, it replies `-ERR Protocol error` and
closes the connection, as Redis does. Unknown commands and wrong arity get an
`-ERR` and the connection stays open. Limits: 1024 arguments, 16 MB per
key/value, 64 MB buffered per unfinished request.

## Benchmarks

_TODO: measured numbers go here (hardware, node count, R, threads, value size,
GET ratio, ops/s, p50/p95/p99)._

## Limitations

- **No anti-entropy or read repair.** A replica that missed writes while it
  was down never catches up on its own. After it restarts it can serve stale
  values or false misses for those keys, because reads stop at the first
  replica that answers.
- **W=1 writes, with no quorum, versioning, or conflict resolution.**
  Concurrent writers can leave replicas permanently different, and a write
  that reached only a node that later loses its disk is gone.
- **Static membership.** The node list is fixed when the client is created.
  There is no rebalancing or data migration when nodes join or leave.
- **Thread per connection.** This is simple, but it will not scale past a
  few thousand clients. An epoll/kqueue event loop is the upgrade path.
- **The AOF is never compacted.** It grows with every write, and replay time
  grows with it. Evictions and expirations are not logged, so a replay
  re-runs them.
- **Approximate memory accounting.** Each entry is charged its key and value
  bytes plus a flat 64 bytes, not what the allocator actually uses.
- **The sweeper scans the whole keyspace** once per second, one shard at a
  time. Redis-style random sampling would be better for very large
  keyspaces.
- **Replica writes are sequential.** Write latency is about R round trips.
- **No auth or TLS.** The server binds to 127.0.0.1 by default.
