# ShardKV

![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg) [![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A small distributed in-memory key-value cache in C++17: Redis-protocol server
nodes with a lock-striped LRU store, TTLs, and an append-only log, plus a
client library that spreads keys over the nodes with consistent hashing and
replicates them with versioned last-writer-wins writes, tombstones, and read
repair. It uses POSIX sockets and `std::thread` only, with no third-party
libraries.

## Highlights

- **190k SET ops/s on one node** with 16 client threads (142k with the AOF
  on). A single client (90% GET) sees a 14 µs p50 and 22 µs p99.
- **92k ops/s on a 3-node cluster at R=2** (90% GET, AOF on) with a 125 µs
  p99, where every GET asks both replicas. Numbers are from `make bench` on
  an Apple M5; see [Benchmarks](#benchmarks).
- **Replication:** consistent hashing with 100 virtual nodes per server
  (about 1/N of the keys move when a node joins, checked in the tests), plus
  last-writer-wins versions, tombstones, and read repair, so a restarted
  replica catches up and deleted keys stay deleted.
- **Storage:** a 16-way lock-striped O(1) LRU, and a CRC32-checked
  append-only log with torn-tail recovery, tested by truncating the log and
  by SIGKILLing a node.
- **Testing:** ThreadSanitizer, AddressSanitizer + UBSan, and a seeded
  randomized parser property test, run in GitHub Actions CI on Ubuntu and
  macOS.

## Design

```
      kvcli / kvbench / your program
                    |
        client.h: shardkv::Cluster
        hash ring, 100 virtual nodes per server
        key -> preference list = first R distinct nodes clockwise
        writes: versioned SET / DELV to all R, ok if >= W ack
        reads:  GETV from all R, newest version wins, stale ones repaired
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
| `src/store.h` | Sharded LRU with TTL, lazy and active expiry, versions and tombstones, and stats |
| `src/aof.h` | Append-only log with CRC'd records, batched writes, periodic fsync, and torn-tail recovery |
| `src/kvserver.cpp` | TCP server and command dispatch |
| `src/client.h` | Header-only cluster client: hash ring, versioned replication, write quorum, read repair, reconnect |
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
- **Versions and tombstones.** An entry can carry a version (0 means
  unversioned). A versioned write is ignored if the key holds a live entry
  with an equal or higher version. A tombstone is an entry with no value
  that records a versioned delete. GET treats it as a miss, but it still
  counts in `DBSIZE`, uses LRU budget, and can be evicted. Every tombstone
  expires 10 minutes after it is written (the grace period), and the normal
  expiry path then removes it. That is the tombstone GC.

### Durability (AOF)
- Record format: `[u32 len][u32 crc32][payload]`. The payload is the command
  encoded as a RESP array (`SET k v [PXAT ms] [VER n] [TOMB 1]` or `DEL k`).
- A write is acknowledged only after its record has been `write()`n to the
  file, so a process crash (SIGKILL) loses no acknowledged write. Appends
  that arrive while a `write()` is in flight queue up, and the next one
  writes the whole queue in a single `write()`. A background thread syncs the
  file every `--fsync-ms` (default 1000; `F_FULLFSYNC` on macOS, where plain
  `fsync` leaves data in the drive cache), so a power loss loses at most that
  window. A failed write or sync stops the server rather than keep acking.
- The log append happens inside the shard lock. This keeps the log order for
  each key the same as the in-memory order.
- **What is logged.** Every SET, every DELV (as a SET with `TOMB 1`), every
  DEL (even of a key that is no longer in memory), and every eviction (as a
  DEL). A versioned write that loses to an equal or newer version changed
  nothing, so it is not logged. GETs are not logged, so replay cannot
  recompute LRU order. Instead it applies the logged evictions and
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
- **Versions.** The client stamps every write with a 64-bit version:
  `[unix ms:48][per-ms sequence:6][client id:10]`. The client id is random,
  and versions strictly increase per client. Each replica keeps the highest
  version it has seen (last writer wins, LWW) and ignores an older or equal
  one, which still gets `+OK`.
- **Writes** (`SET ... VER n`) go to all R replicas one after another and
  succeed if at least W ack. W defaults to 1 (`--write-quorum` in kvcli).
- **Deletes** write a tombstone with a new version (`DELV key ver`) instead
  of removing the key. Without it, a replica that missed the delete would
  still hold the old value, and read repair would copy it back. Tombstones
  are kept for a 10-minute grace period and then garbage-collected.
- **Reads** ask all R replicas for `(version, value, expiry)` with `GETV`.
  A replica that fails or times out (default 500 ms) is skipped, and at
  least one must answer. The highest version wins, and ties go to the
  earliest replica in the preference list. A tombstone or no entry reads as
  a miss.
- **Read repair.** Each replica that answered with a lower version gets the
  winner written back (`SET key val [PXAT at] VER v`, or `DELV key v` for a
  tombstone). This is best effort: failures are ignored. Because equal
  versions are ignored, repeating a repair changes nothing.
- **Unversioned writes** (plain `SET` / `DEL` from redis-cli, or AOF records
  written before versions existed) always apply and store version 0. Any
  versioned entry beats them during read repair.
- **Eviction is local, deletion is global.** An LRU eviction removes the key
  from that node only and never creates a tombstone.
- **Reconnect.** A failed node is skipped for 1 s after the failure, then
  retried with a fresh connection. A pooled connection that went stale (for
  example because the node restarted) gets one immediate retry on a new
  socket.
- **Read-your-writes.** A read needs only one replica to answer, so its
  read quorum is 1. In Dynamo's terms (read quorum + W > replicas) reads are
  guaranteed to see the latest acknowledged write when W = R. With W=1, a
  read returns an older value only if every replica that took the write is
  unreachable at that moment.
- The client throws `std::invalid_argument` for a key or value over 16 MB
  instead of sending it, because the server would hang up on it. It also
  throws for a node that is listed twice.

## Build, run, test

Requires clang++ or g++ (C++17), make, and python3. CI builds and tests on Ubuntu and macOS.

```sh
make              # build/kvserver build/kvcli build/kvbench build/unit_test
make test         # unit tests + 3-node integration test
make tsan         # same tests, everything built with -fsanitize=thread
make asan         # same tests under -fsanitize=address,undefined
make bench        # benchmark table for the README (about 2.5 min, not run in CI)
```

```sh
build/kvserver --port 7001 --aof n1.aof &
build/kvserver --port 7002 --aof n2.aof &
build/kvserver --port 7003 --aof n3.aof &
N=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
build/kvcli --nodes $N set user:1 alice
build/kvcli --nodes $N get user:1
build/kvcli --nodes $N set session:9 xyz 5000   # TTL in ms
build/kvcli --nodes $N del user:1               # writes a tombstone on both replicas
build/kvcli --nodes $N --write-quorum 2 set user:2 bob   # fails unless both replicas ack
build/kvbench --nodes $N --threads 8 --keys 100000 --value-size 100 --get-ratio 0.9 --duration 10
redis-cli -p 7001 info                          # any single node speaks RESP
```

Server flags: `--port 6380 --bind 127.0.0.1 --aof FILE --fsync-ms 1000 --max-mb 256 --shards 16`.

**What the tests cover.** `tests/unit_test.cpp` uses plain asserts. It covers
the following:
- the RESP parser on every partial prefix and on malformed input, and
  array replies (no nesting)
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
- last-writer-wins: older and equal versions are ignored and not logged, a
  tombstone hides the key from GET and blocks older writes until its grace
  period ends and the sweeper drops it, unversioned writes always apply, and
  AOF replay restores every key's value and version
- the balance and minimal key movement of the hash ring
- client failover backoff against a node that hangs
- an 8-thread stress test of the store with the AOF on, mixing plain,
  versioned and tombstone writes, checking that replaying its log
  reproduces the live store, versions included

`tests/integration_test.py` does the following:
1. Starts 3 nodes.
2. Checks pipelining, partial reads, malformed input, and `SET ... VER` /
   `GETV` / `DELV` on the raw protocol.
3. Writes 300 keys through `kvcli` and checks that each key is on exactly 2
   nodes.
4. `SIGKILL`s one node and reads every key back through failover. While it
   is down, updates 10 of its keys and deletes 10 more, and checks that a
   `--write-quorum 2` write fails.
5. Restarts that node and checks that AOF replay restored exactly its keys,
   which are now stale.
6. Reads the updated and deleted keys through `kvcli`, then checks that read
   repair brought the restarted node up to date, that no node returns a
   deleted key, and that the node holds the tombstone.
7. Runs a short `kvbench`.

## Protocol

This is a RESP2 subset. Requests must be arrays of bulk strings, which is what
redis-cli and every Redis client send. Inline commands are not supported.

| Command | Reply |
|---------|-------|
| `PING` | `+PONG` |
| `GET key` | bulk string or `$-1` (nil) |
| `SET key value [PX ms \| PXAT unix-ms] [VER n]` | `+OK`, also when a `VER` write loses to an equal or newer version and is ignored |
| `DEL key [key ...]` | `:n` keys removed (a tombstone is removed but not counted) |
| `GETV key` | `*3` array: `:version` (0 = no entry), the value or `$-1` (nil with a version > 0 is a tombstone), `:expire_at` (unix ms, 0 = none) |
| `DELV key ver` | `+OK`; stores a tombstone with that version for 10 minutes |
| `DBSIZE` | `:n`, counting tombstones and expired keys the sweeper has not reached yet |
| `INFO` / `STATS` | bulk string with `keys`, `used_bytes`, `hits`, `misses`, `evictions`, `expired` |

When the server gets malformed framing, it replies `-ERR Protocol error` and
closes the connection, as Redis does. Unknown commands and wrong arity get an
`-ERR` and the connection stays open. Limits: 1024 arguments, 16 MB per
key/value, 64 MB buffered per unfinished request. Pipelined replies are sent
every 64 KB, so they don't pile up in memory. A client that stops reading
its replies is disconnected once a send makes no progress for 10 s.

## Benchmarks

Apple M5 (10 cores), macOS 26.2 arm64, 2026-10-08. Reproduce with
`make bench` (`tests/bench.py`).

| nodes | R | AOF | threads | GET % | ops/s | p50 µs | p99 µs |
|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 1 | on | 1 | 90 | 70986 | 14 | 22 |
| 1 | 1 | on | 16 | 0 | 142159 | 99 | 294 |
| 1 | 1 | off | 16 | 0 | 189534 | 83 | 113 |
| 3 | 2 | on | 8 | 90 | 92225 | 84 | 125 |
| 3 | 2 | off | 8 | 90 | 94499 | 83 | 118 |
| 3 | 2 | on | 8 | 0 | 73856 | 105 | 169 |
| 3 | 2 | off | 8 | 0 | 94351 | 83 | 117 |
| 3 | 1 | on | 8 | 90 | 182288 | 42 | 70 |

**Method.** Each row starts fresh nodes and prefills 10,000 keys with
100 B values. It then runs `kvbench` 3 times for 5 s each and shows the
run with the median throughput, with that run's p50 and p99. kvbench is
closed-loop: each thread sends its next request when the previous one
returns. The client and the servers run on the same machine and share its
cores, so these numbers measure the code path, not a network. Expect about
±10% between runs. A GET at R=2 asks both replicas, one after the other,
which is why the R=1 row does about twice the ops/s. AOF on means the
default `--fsync-ms 1000`.

`make bench` also compares one kvserver with one redis-server using
`redis-benchmark` when both are installed. They were not installed for
this run, so there is no Redis comparison yet.

## Limitations

- **Read repair only.** There is no anti-entropy (Merkle-tree sync) or
  hinted handoff, so a replica that missed writes catches up only on the
  keys that get read. A key that is never read stays stale there. If a
  replica that missed a delete stays down longer than the 10-minute
  tombstone grace period, or the other replica evicts the tombstone, read
  repair copies the deleted value back (Cassandra's `gc_grace_seconds` has
  the same trade-off).
- **LWW on client wall clocks.** Versions come from each client's clock, so
  clock skew between clients can let an older write win. Hybrid logical
  clocks are the upgrade path. A write that fails its W quorum is not rolled
  back: the replicas that took it keep it, and a later read can return it.
  Reads are guaranteed to see the latest acknowledged write only when W = R
  (see Read-your-writes above).
- **Static membership.** The node list is fixed when the client is created.
  There is no rebalancing or data migration when nodes join or leave.
- **Thread per connection.** This is simple, but it will not scale past a
  few thousand clients. Once the OS thread limit is reached, new
  connections are closed right away. An epoll/kqueue event loop is the
  upgrade path.
- **AOF writes share one log.** Concurrent appends are batched into one
  `write()`, but each SET/DEL still waits for that write while it holds its
  shard lock. On one node with 16 client threads doing only SETs, the AOF
  costs about a quarter of the throughput (see the two single-node,
  16-thread rows in Benchmarks).
- **The AOF is never compacted.** It grows with every write and every
  eviction, and replay time grows with it.
- **Approximate memory accounting.** Each entry is charged its key and value
  bytes plus a flat 64 bytes, not what the allocator actually uses.
- **The sweeper scans the whole keyspace** once per second, one shard at a
  time. Redis-style random sampling would be better for very large
  keyspaces.
- **Replica reads and writes are sequential.** Each GET and each SET/DEL
  costs about R round trips.
- **No auth or TLS.** The server binds to 127.0.0.1 by default.
