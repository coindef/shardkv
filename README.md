# ShardKV

[![CI](https://github.com/coindef/shardkv/actions/workflows/ci.yml/badge.svg)](https://github.com/coindef/shardkv/actions/workflows/ci.yml) ![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg) [![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A small distributed in-memory key-value cache in C++17: Redis-protocol server
nodes with a lock-striped LRU store, TTLs, and an append-only log, plus a
client library that spreads keys over the nodes with consistent hashing and
replicates them with versioned last-writer-wins writes, tombstones, and read
repair. It uses POSIX sockets and `std::thread` only, with no third-party
libraries.

## Highlights

- **184k ops/s on one node** with 8 client threads at 90:10 GET:SET over
  100k preloaded keys, with a 42 µs p50 and 67 µs p99.
- **93k ops/s on a 3-node cluster at R=2** (90:10, 118 µs p99), where every
  GET asks both replicas. 95k ops/s at 50:50 with 16 threads.
- **Failover:** SIGKILLing one of the 3 nodes in the middle of a 20 s run
  caused 0 failed reads or writes. Numbers are from `make bench` on an
  Apple M5, with all nodes and clients on one machine over localhost; see
  [Benchmarks](#benchmarks).
- **Replication:** consistent hashing with 100 virtual nodes per server
  (about 1/N of the stored copies move when a node joins), plus
  last-writer-wins versions, tombstones, and read repair, so a restarted
  replica catches up on the keys that get read, and a delete beats a replica
  that missed it if the key is read within the 10-minute tombstone grace
  period.
- **Storage:** a 16-way lock-striped O(1) LRU, and a CRC32-checked
  append-only log with torn-tail recovery, tested by truncating the log and
  by SIGKILLing a node.
- **Testing:** ThreadSanitizer, AddressSanitizer + UBSan, and a seeded
  randomized parser property test, with a GitHub Actions workflow for
  Ubuntu and macOS included.

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
| `src/kvbench.cpp` | Multithreaded load generator that reports ops/s, p50/p95/p99, and GET hit rate |

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
  expiry path then removes it. That is the tombstone GC. When a versioned
  value's TTL runs out, it becomes a tombstone with the same version for the
  grace period. Otherwise a replica that missed that write could win the
  next read with an older value, and read repair would copy it back.

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
  was live at the crash, so a deleted key never comes back. If the shard
  budget shrank between runs (a smaller `--max-mb` or more `--shards`),
  replay drops entries now too big for a shard and the shards are trimmed
  once replay is done. Both are logged as DELs, so those keys stay gone if
  the budget grows back.
  Expirations are not logged. They don't need to be, because deadlines are
  absolute and replay expires entries that are already dead the same way
  (a versioned value within its grace period comes back as its tombstone).
- **Replay** runs on startup and re-applies records in order. A crash in the
  middle of a write leaves a bad record (truncated or failing its CRC) that
  is the start of one record cut off by the end of the file (its length runs
  past the end and its payload is an unfinished command), or that has no
  intact record after it. Replay `ftruncate`s the file there so new appends
  follow valid data. A value that happens to contain a valid record does not
  change this. Any other bad record with intact records after it is
  corruption, not a crash. The server then refuses to start rather than
  throw that data away.

### Cluster and consistency model
- **Placement.** Each server gets 100 virtual nodes on a 64-bit ring (FNV-1a
  plus the murmur3 finalizer). A key's preference list is the first R
  distinct servers clockwise from its hash. R defaults to 2. When a node is
  added, about 1/N of the stored copies move, all of them to the new node:
  about 1/N of the keys get it as their primary and about R/N as one of
  their replicas. The unit tests check the primaries.
- **Versions.** The client stamps every write with a 64-bit version:
  `[unix ms:48][per-ms sequence:6][client id:10]`. The client id is random,
  and versions strictly increase per client. A client that writes more than
  64 times in one ms borrows from the next ms, so its clock can run ahead of
  wall time. Each replica keeps the highest version it has seen (last writer
  wins, LWW). A write that loses to a higher version, or to a different
  write with the same version, is ignored and answered `-STALE <winning
  version>`. The same write again (a retry or a repeated read repair) gets
  `+OK` and changes nothing.
- **Writes** (`SET ... VER n`) go to all R replicas one after another and
  succeed if at least W ack. W defaults to 1 (`--write-quorum` in kvcli) and
  must be between 1 and R. If any replica answers `-STALE`, the client moves
  its clock past that version (keeping its own id, as a hybrid logical clock
  does) and redoes the write with a new version. So a write that comes after
  another client's does not get `+OK` and then lose to it, even when that
  client's clock is ahead or both wrote in the same millisecond. Writers
  racing on one key can keep beating each other, so after 5 rounds the
  write reports failure.
- **Deletes** write a tombstone with a new version (`DELV key ver`) instead
  of removing the key. Without it, a replica that missed the delete would
  still hold the old value, and read repair would copy it back. Tombstones
  are kept for a 10-minute grace period and then garbage-collected (a
  tombstone that read repair copies to another replica starts a new grace
  period there).
- **Reads** ask all R replicas for `(version, value, expiry)` with `GETV`.
  A replica that fails or times out (default 500 ms) is skipped, and at
  least one must answer. The highest version wins. On a tie a value beats a
  miss (no entry and an unversioned value both report version 0), and then
  the earliest replica in the preference list wins. A tombstone or no entry
  reads as a miss.
- **Read repair.** Each replica that answered with a lower version gets the
  winner written back (`SET key val [PXAT at] VER v`, or `DELV key v` for a
  tombstone). This is best effort: failures are ignored. Because equal
  versions are ignored, repeating a repair changes nothing.
- **Unversioned writes** (plain `SET` / `DEL` from redis-cli, or AOF records
  written before versions existed) always apply and store version 0. Any
  versioned entry beats them during read repair.
- **Eviction is local, deletion is global.** An LRU eviction removes the key
  from that node only and never creates a tombstone. So if the replica that
  took an overwrite evicts it, a replica that missed the overwrite wins the
  next read with the older value, and read repair copies that value back.
- **Reconnect.** A failed node is skipped for 1 s after the failure, then
  retried with a fresh connection. A pooled connection that went stale (for
  example because the node restarted) gets one immediate retry on a new
  socket. A node that hangs (it still accepts connections but never replies)
  therefore costs two timeouts, 1 s by default, before it is skipped.
- **Read-your-writes.** A read needs only one replica to answer, so its
  read quorum is 1. In Dynamo's terms (read quorum + W > replicas) reads are
  guaranteed to see the latest acknowledged write when W = R. Every replica
  took that write, and a replica holding a higher version would have made
  the writer move past it. With W < R, a read can also return an older
  value in three cases. Every replica that took the write may be
  unreachable at that moment. They may have evicted the key. Or the write
  may have reached none of the replicas that hold an older write stamped
  with a higher version (see Limitations).
- The client throws `std::invalid_argument` for a key or value over 16 MB
  instead of sending it, because the server would hang up on it. It also
  throws for a `host:port` string that is listed twice. It compares the
  strings only, so `127.0.0.1:7001` and `localhost:7001` count as two nodes
  and would put two replicas on one server.

## Build, run, test

Requires clang++ or g++ (C++17), make, and python3. `make` uses clang++ when
it is installed and the system `c++` otherwise; pick one with `make CXX=g++`.
Tested on macOS. `.github/workflows/ci.yml` is a GitHub Actions workflow that
builds and tests on Ubuntu and macOS.

```sh
make              # build/kvserver build/kvcli build/kvbench build/unit_test
make test         # unit tests + 3-node integration test
make tsan         # same tests, everything built with -fsanitize=thread
make asan         # same tests under -fsanitize=address,undefined
make bench        # benchmark tables for the README (about 3 min, not run in CI)
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
- AOF replay: a torn tail is truncated (also when the torn value holds a
  valid record), mid-file corruption is refused, replay after evictions
  and a DEL of an evicted key gives back exactly the live keys, and an entry
  too big for a shrunk budget stays gone when the budget grows back
- last-writer-wins: an older version, or a different value at an equal
  version, is ignored, not logged, and reported as stale, while the same
  write repeated is a quiet no-op. A tombstone hides the key from GET and
  blocks older writes until its grace period ends and the sweeper drops it.
  A versioned value whose TTL runs out leaves a tombstone with its version.
  Unversioned writes always apply, and AOF replay restores every key's value
  and version
- the balance and minimal key movement of the hash ring
- client failover backoff against a node that hangs, and refusing a write
  quorum above R
- an 8-thread stress test of the store with the AOF on, mixing plain,
  versioned and tombstone writes, checking that replaying its log
  reproduces the live store, versions included

`tests/integration_test.py` does the following:
1. Starts 3 nodes.
2. Checks pipelining, partial reads, malformed input, and `SET ... VER` /
   `GETV` / `DELV` on the raw protocol, `-STALE` replies included.
3. Writes 300 keys through `kvcli` and checks that each key is on exactly 2
   nodes.
4. `SIGKILL`s one node and reads every key back through failover. While it
   is down, updates 10 of its keys, deletes 10 more, overwrites one with a
   200 ms TTL, and checks that a `--write-quorum 2` write fails.
5. Restarts that node and checks that AOF replay restored exactly its keys,
   which are now stale.
6. Reads the updated and deleted keys through `kvcli`, then checks that read
   repair brought the restarted node up to date, that no node returns a
   deleted key, and that the node holds the tombstone. Once the TTL has run
   out, checks that the node's older value for that key does not come back.
7. Writes a key on every node with a version a minute ahead, as a client
   whose clock is ahead would. Then checks that a `--write-quorum 2` write
   through `kvcli` goes past it on both replicas.
8. Runs a short `kvbench`.

## Protocol

This is a RESP2 subset. Requests must be arrays of bulk strings, which is what
redis-cli and every Redis client send. Inline commands are not supported.

| Command | Reply |
|---------|-------|
| `PING` | `+PONG` |
| `GET key` | bulk string or `$-1` (nil) |
| `SET key value [PX ms \| PXAT unix-ms] [VER n]` | `+OK`, or `-STALE ver` when a `VER` write lost to version `ver` (higher, or equal with a different value) and was ignored. The same write repeated gets `+OK` |
| `DEL key [key ...]` | `:n` keys removed (a tombstone is removed but not counted) |
| `GETV key` | `*3` array: `:version` (0 = no entry or an unversioned value), the value or `$-1` (nil with a version > 0 is a tombstone, which is also what a versioned value becomes when its TTL runs out), `:expire_at` (unix ms; 0 = no TTL, and always 0 for nil) |
| `DELV key ver` | `+OK`, or `-STALE ver` as for SET; stores a tombstone with that version for 10 minutes |
| `DBSIZE` | `:n`, counting tombstones and expired keys the sweeper has not reached yet |
| `INFO` / `STATS` | bulk string with `keys`, `used_bytes`, `hits`, `misses`, `evictions`, `expired` (values whose TTL ran out; tombstone GC isn't counted) |

When the server gets malformed framing, it replies `-ERR Protocol error` and
closes the connection, as Redis does. Unknown commands and wrong arity get an
`-ERR` and the connection stays open. Limits: 1024 arguments, 16 MB per
key/value, 64 MB buffered per unfinished request. Pipelined replies are sent
every 64 KB, so they don't pile up in memory. A client that stops reading
its replies is disconnected once a send makes no progress for 10 s.

## Benchmarks

**Machine.** `sysctl -n machdep.cpu.brand_string` = Apple M5,
`sysctl -n hw.ncpu` = 10. macOS 26.2 arm64, Apple clang 17.0.0, built with
plain `make` (`-O2`, no sanitizers). Run on 2026-10-08. Other processes were
running too: the load average was 4.6 when the run started.

**All nodes and all client threads ran on this one machine and talked over
localhost (127.0.0.1).** They share the 10 cores, and no real network is
involved. So these numbers measure the code path, not a deployment.

| run | nodes | R | threads | GET:SET | ops/s (median) | p50 µs | p95 µs | p99 µs | GET hit rate | ops/s of all 3 runs |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---|
| a | 1 | 1 | 8 | 90:10 | 184,327 | 42.0 | 57.8 | 67.1 | 100.00% | 184,281 / 184,327 / 184,638 |
| b | 3 | 2 | 8 | 90:10 | 93,483 | 84.1 | 105.7 | 118.3 | 100.00% | 93,015 / 93,483 / 93,623 |
| c | 3 | 2 | 16 | 50:50 | 95,030 | 167.0 | 192.6 | 208.2 | 100.00% | 93,864 / 95,030 / 95,181 |

**Commands.** `make bench` runs `tests/bench.py build`. For each row it
starts fresh nodes with the default flags (no AOF, `--max-mb 256 --shards 16`),
runs kvbench 3 times, and stops the nodes:

```sh
build/kvserver --port $P &        # one per node
build/kvbench --nodes $NODES --replicas $R --threads $T --keys 100000 \
  --value-size 128 --get-ratio $G --duration 15 --preload 1   # x3
```

**Method.** `--preload 1` first SETs all 100,000 keys through the cluster
client, so every key is on all R of its replicas. This step is not timed,
and it runs before each of the 3 runs. The measured part is closed-loop:
each thread picks a uniformly random key and sends its next request as soon
as the last one returns. The row shows the run with the median throughput,
with that run's percentiles. The 3 runs were within 2% of each other. GET hit
rate is the share of successful GETs that returned a value. At R=2 one
request includes both replica round trips, one after the other. That is
why rows b and c are about half of row a's ops/s.

**Failover check.** 3 nodes, R=2, 8 threads, 90:10. One 20 s run, and
`tests/bench.py` SIGKILLs node 2 of 3 10.01 s after the measured part
starts:

```sh
build/kvbench --nodes $NODES --replicas 2 --threads 8 --keys 100000 --value-size 128 \
  --get-ratio 0.9 --duration 20 --preload 1 --interval 1
```

| window | mean ops/s | GET errors | SET errors |
|---|---:|---:|---:|
| 0-10 s, before the kill | 93,446 | 0 | 0 |
| 10-11 s, contains the kill | 143,404 | 0 | 0 |
| 11-20 s, after the kill | 144,951 | 0 | 0 |
| whole run (2,382,430 ops) | 119,086 | 0 | 0 |

Reads kept succeeding, with 0 errors and a 100.00% GET hit rate over the
whole run. Throughput went up after the kill, not down. A client that sees a
node fail skips it for 1 s and then retries. So the keys that had a copy on
the dead node (about 2/3 of them) cost one round trip instead of two, and one less
server shares the cores. The cost is redundancy: writes to those keys landed
on one replica only (W=1). When the node comes back, read repair catches it
up only on the keys that get read.

`make bench` also compares one kvserver with one redis-server using
`redis-benchmark` when both are installed. They were not installed for
this run, so there is no Redis comparison.

## Limitations

- **Read repair only.** There is no anti-entropy (Merkle-tree sync) or
  hinted handoff, so a replica that missed writes catches up only on the
  keys that get read. A key that is never read stays stale there. That
  includes deletes: if a replica missed a delete (or a TTL'd overwrite),
  however briefly it was down, and the key is not read within the 10-minute
  tombstone grace period after the delete (or the TTL deadline), the
  tombstone is garbage-collected first. The next read then finds only the
  older value, and read repair copies it back to every replica, so an
  expired key can come back without its TTL. The same happens if the other replica evicts the tombstone
  (Cassandra's `gc_grace_seconds` has the same trade-off, which is why it
  needs repair within that window). Eviction of a value is the same: if the
  replica that took an overwrite evicts it, the older value on a replica
  that missed the overwrite wins the next read.
- **LWW on client clocks.** Versions come from each client's clock. A write
  that finds a higher version on a replica moves past it, so with W = R no
  acknowledged write is lost. With W < R, a write can reach none of the
  replicas that hold a higher version. An older write then still wins over
  it. That happens when clocks are skewed, when a client writing more than
  64 times per ms has run ahead of wall time, or when both writes came in
  the same ms. A write that fails its W quorum is not rolled back: the
  replicas that took it keep it, and a later read can return it. Reads are
  guaranteed to see the latest acknowledged write only when W = R (see
  Read-your-writes above).
- **Static membership.** The node list is fixed when the client is created.
  There is no rebalancing or data migration when nodes join or leave.
- **Thread per connection.** This is simple, but it will not scale past a
  few thousand clients. Once the OS thread limit is reached, new
  connections are closed right away. An epoll/kqueue event loop is the
  upgrade path.
- **AOF writes share one log.** Concurrent appends are batched into one
  `write()`, but each SET/DEL still waits for that write while it holds its
  shard lock, so write throughput drops with the AOF on. The Benchmarks
  above run with the AOF off.
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
