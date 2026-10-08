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
   | AOF: batched write()s; fsync thread every --fsync-ms        |
   | sweeper thread: drops expired keys once per second          |
   +-------------------------------------------------------------+
```

| File | What it does |
|------|--------------|
| `src/resp.h` | RESP2 encoder/parser. The parser never consumes a partial frame and reports `kIncomplete` |
| `src/store.h` | Sharded LRU with TTL, lazy and active expiry, and stats |
| `src/aof.h` | Append-only log with CRC'd records, batched writes, periodic fsync, and torn-tail recovery |
| `src/kvserver.cpp` | TCP server and command dispatch |
| `src/client.h` | Header-only cluster client: hash ring, replication, failover, reconnect |
| `src/kvcli.cpp` | CLI built on the client library |
| `src/kvbench.cpp` | Multithreaded load generator that reports ops/s and p50/p95/p99 |

### Storage engine
- **Lock striping.** Keys hash to one of N shards (default 16). Each shard has
  its own `std::mutex`, so threads working on different shards don't contend
  on the store. With the AOF on, writes on every shard also share one log
  (see Durability), so writes scale less well than reads.
- **O(1) LRU per shard.** A `std::list` is kept in recency order next to an
  `unordered_map<string_view, list::iterator>`. The map's keys point into the
  list nodes, so each key is stored once. A hit moves the node to the front
  with `splice`. When a shard goes over its byte budget (`--max-mb` / shards),
  entries are evicted from the back. A SET whose entry alone is bigger than
  a shard's budget gets `-ERR` and is not stored. Any old value for that key
  is dropped too, as if the new entry had been evicted right away.
- **TTL.** Each entry holds an absolute wall-clock expiry time. Reads expire
  keys lazily, and a sweeper thread scans for expired keys once per second.
  Because expiry times are absolute, they still hold after an AOF replay.

### Durability (AOF)
- Record format: `[u32 len][u32 crc32][payload]`. The payload is the command
  encoded as a RESP array (`SET k v [PXAT ms]` or `DEL k`).
- A write is acknowledged only after its record has been `write()`n to the
  file, so a process crash (SIGKILL) loses no acknowledged write. Appends
  that arrive while a `write()` is in flight queue up, and the next one
  writes the whole queue in a single `write()`. A background thread syncs the
  file every `--fsync-ms` (default 1000; `F_FULLFSYNC` on macOS, where plain
  `fsync` leaves data in the drive cache), so a power loss loses at most that
  window. A failed write or sync stops the server rather than keep acking.
- The log append happens inside the shard lock. This keeps the log order for
  each key the same as the in-memory order.
- **What is logged.** Every SET, every DEL (even of a key that is no longer
  in memory), and every eviction (as a DEL). GETs are not logged, so replay
  cannot recompute LRU order. Instead it applies the logged evictions and
  never evicts on its own. That makes the replayed keyspace exactly what
  was live at the crash, so a deleted key never comes back. If `--max-mb`
  shrank between runs, the shards are trimmed once replay is done.
  Expirations are not logged. They don't need to be, because deadlines are
  absolute and replay skips entries that are already dead.
- **Replay** runs on startup and re-applies records in order. A bad record
  (truncated or failing its CRC) with no intact record after it is what a
  crash in the middle of a write leaves behind. Replay `ftruncate`s the file
  there so new appends follow valid data. A bad record with intact records
  after it is corruption, not a crash. The server then refuses to start
  rather than throw that data away.

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
- **Reconnect.** A failed node is skipped for 1 s after the failure, then
  retried with a fresh connection. A pooled connection that went stale (for
  example because the node restarted) gets one immediate retry on a new
  socket.
- This is **best-effort replication: W=1, first responder wins on reads,
  and replicas are not guaranteed to converge**. Each replica keeps whatever
  it last received. Concurrent writers, or a node that missed writes while
  it was down, can leave replicas with different values (see Limitations).
- The client throws `std::invalid_argument` for a key or value over 16 MB
  instead of sending it, because the server would hang up on it. It also
  throws for a node that is listed twice.

## Build, run, test

Requires clang++ (C++17), make, and python3. Builds on macOS and Linux.

```sh
make              # build/kvserver build/kvcli build/kvbench build/unit_test
make test         # unit tests + 3-node integration test
make tsan         # same tests, everything built with -fsanitize=thread
make asan         # same tests under -fsanitize=address,undefined
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
the following:
- the RESP parser on every partial prefix and on malformed input
- a seeded randomized parser test: encode/parse round trips; streams fed in
  random 1-17 byte chunks through the same loop as the server, which must
  give back the same commands; and randomly mutated streams, where every
  prefix must stay `kIncomplete` until the first `kOk` or `kError` and then
  keep that answer. It found a reply line over 1024 bytes that parsed `kOk`
  when it arrived whole and `kError` when split before its CR
- LRU eviction order and oversized entries
- lazy and active TTL expiry
- AOF replay: a torn tail is truncated, mid-file corruption is refused, and
  replay after evictions and a DEL of an evicted key gives back exactly the
  live keys
- the balance and minimal key movement of the hash ring
- client failover backoff against a node that hangs
- an 8-thread stress test of the store with the AOF on, checking that
  replaying its log reproduces the live store

`tests/integration_test.py` does the following:
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
key/value, 64 MB buffered per unfinished request. Pipelined replies are sent
every 64 KB, so they don't pile up in memory. A client that stops reading
its replies is disconnected once a send makes no progress for 10 s.

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
  few thousand clients. Once the OS thread limit is reached, new
  connections are closed right away. An epoll/kqueue event loop is the
  upgrade path.
- **AOF writes share one log.** Concurrent appends are batched into one
  `write()`, but each SET/DEL still waits for that write while it holds its
  shard lock. On one node with 16 client threads doing only SETs, the AOF
  costs about a quarter of the throughput: about 145k ops/s with it and
  190k without. Before batching it was 120k.
- **The AOF is never compacted.** It grows with every write and every
  eviction, and replay time grows with it.
- **Approximate memory accounting.** Each entry is charged its key and value
  bytes plus a flat 64 bytes, not what the allocator actually uses.
- **The sweeper scans the whole keyspace** once per second, one shard at a
  time. Redis-style random sampling would be better for very large
  keyspaces.
- **Replica writes are sequential.** Write latency is about R round trips.
- **No auth or TLS.** The server binds to 127.0.0.1 by default.
