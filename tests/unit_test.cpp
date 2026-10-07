// Unit tests (plain asserts): RESP codec, LRU eviction, TTL expiry, AOF replay
// and torn-tail recovery, hash-ring placement, client failover backoff, and a
// concurrent smoke test that gives ThreadSanitizer something to chew on.
#undef NDEBUG
#include <sys/stat.h>

#include <atomic>
#include <cassert>
#include <cstdio>
#include <fstream>
#include <random>
#include <thread>

#include "client.h"
#include "store.h"

using Args = std::vector<std::string>;
using namespace std::chrono_literals;

static void test_resp() {
  const std::string a = resp::encode({"SET", "k", "v"}), stream = a + resp::encode({"GET", "k"});
  Args args;
  size_t used = 0;
  for (size_t i = 0; i < a.size(); i++)  // every strict prefix just means "need more bytes"
    assert(resp::parse_request(stream.data(), i, args, used) == resp::kIncomplete);
  assert(resp::parse_request(stream.data(), stream.size(), args, used) == resp::kOk);
  assert(used == a.size() && args == Args({"SET", "k", "v"}));
  assert(resp::parse_request(stream.data() + used, stream.size() - used, args, used) == resp::kOk);
  assert(args == Args({"GET", "k"}));
  for (std::string bad : {"GET k\r\n", "*0\r\n", "*1\r\n:1\r\n", "*1\r\n$-1\r\n", "*1\r\n$2\r\nabc\r\n",
                          "*x\r\n", "*1\r\n$99999999999\r\n", "*1\r\n$1\r\na\n\n"})
    assert(resp::parse_request(bad.data(), bad.size(), args, used) == resp::kError);
  for (std::string bad : {std::string("*1\0x\r\n", 6), std::string("*+1\r\n$1\r\na\r\n"),
                          std::string("*1\r\n$ 1\r\na\r\n")})  // lengths are digits, nothing else
    assert(resp::parse_request(bad.data(), bad.size(), args, used) == resp::kError);

  resp::Reply r;
  auto reply = [&](const std::string& s) { return resp::parse_reply(s.data(), s.size(), r, used); };
  assert(reply("+OK\r\n") == resp::kOk && r.type == '+' && r.str == "OK");
  assert(reply("-ERR boom\r\n") == resp::kOk && r.type == '-' && r.str == "ERR boom");
  assert(reply(":42\r\n") == resp::kOk && r.num == 42);
  assert(reply("$-1\r\n") == resp::kOk && r.nil);
  assert(reply("$5\r\nhello\r\n") == resp::kOk && r.str == "hello" && used == 11);
  assert(reply("$5\r\nhel") == resp::kIncomplete);
  assert(reply("?\r\n") == resp::kError);
}

static void test_lru() {
  Store s(1, 3 * 66);  // one shard with room for exactly three 1-byte keys/values
  std::string v;
  s.set("a", "1", 0);
  s.set("b", "2", 0);
  s.set("c", "3", 0);
  assert(s.get("a", &v) && v == "1");  // a becomes most recently used
  s.set("d", "4", 0);                  // evicts b, the least recently used
  assert(!s.get("b", &v));
  assert(s.get("a", &v) && s.get("c", &v) && s.get("d", &v));
  Store::Stats st = s.stats();
  assert(st.keys == 3 && st.evictions == 1 && st.hits == 4 && st.misses == 1);
  s.set("a", "9", 0);  // overwrite keeps the entry count
  assert(s.get("a", &v) && v == "9" && s.stats().keys == 3);
  assert(s.del("a") && !s.del("a") && s.stats().keys == 2);
  // An entry bigger than the whole shard is refused, and the old value goes too.
  assert(!s.set("c", std::string(200, 'x'), 0) && !s.get("c", &v));
  assert(s.stats().keys == 1 && s.stats().bytes <= 3 * 66);
}

static void test_ttl() {
  Store s(4, 1 << 20);
  std::string v;
  s.set("short", "x", now_ms() + 50);
  s.set("long", "y", now_ms() + 60000);
  s.set("forever", "z", 0);
  assert(s.get("short", &v) && v == "x");
  std::this_thread::sleep_for(80ms);
  assert(!s.get("short", &v));  // lazy expiry on access
  assert(s.get("long", &v) && s.get("forever", &v));
  assert(s.stats().expired == 1);

  for (int i = 0; i < 100; i++) s.set("k" + std::to_string(i), "v", now_ms() + 20);
  std::this_thread::sleep_for(50ms);
  assert(s.sweep() == 100);  // what the background sweeper calls
  assert(s.stats().keys == 2 && s.stats().expired == 101);

  s.set("past", "v", now_ms() - 1);  // a deadline already in the past stores nothing
  assert(!s.get("past", &v));
}

static std::string tmp_path(const std::string& name) {
  const char* tmp = getenv("TMPDIR");
  return std::string(tmp ? tmp : "/tmp") + "/shardkv_" + name + "_" + std::to_string(getpid()) + ".aof";
}

static size_t replay_into(Store& s, const std::string& path) {
  Aof aof(path, 1000);
  return aof.replay([&](Args& cmd) { s.apply(cmd); });
}

static void test_aof() {
  const std::string path = tmp_path("unit");
  unlink(path.c_str());
  std::string v;
  {
    Store s;
    Aof aof(path, 10);
    assert(aof.replay([&](Args& cmd) { s.apply(cmd); }) == 0);
    s.attach_aof(&aof);
    s.set("a", "1", 0);
    s.set("b", "2", 0);
    s.del("a");
    s.set("ttl", "3", now_ms() + 60000);
    s.set("c", "4", 0);
  }  // ~Aof fsyncs and closes
  {
    Store s;
    assert(replay_into(s, path) == 5);
    assert(!s.get("a", &v) && s.get("b", &v) && v == "2");
    assert(s.get("ttl", &v) && v == "3" && s.get("c", &v) && v == "4");
  }

  // Crash mid-write: the last record ("SET c 4") is cut short.
  struct stat st;
  assert(stat(path.c_str(), &st) == 0 && truncate(path.c_str(), st.st_size - 3) == 0);
  {
    Store s;
    assert(replay_into(s, path) == 4);  // torn record detected and skipped
    assert(!s.get("c", &v) && s.get("b", &v));
  }
  // Replay truncated the torn bytes, so a new append is readable afterwards.
  {
    Store s;
    Aof aof(path, 10);
    aof.replay([&](Args& cmd) { s.apply(cmd); });
    s.attach_aof(&aof);
    s.set("d", "5", 0);
  }
  {
    Store s;
    assert(replay_into(s, path) == 5);
    assert(s.get("d", &v) && v == "5" && !s.get("c", &v));
  }

  // Flip a byte inside record 2: that is corruption, not a torn tail, so replay
  // refuses to start rather than truncate the intact records after it.
  {
    const size_t rec1 = 8 + resp::encode({"SET", "a", "1"}).size();
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(rec1 + 8 + 4);
    f.put('X');
  }
  {
    assert(stat(path.c_str(), &st) == 0);
    const off_t size = st.st_size;
    Store s;
    bool threw = false;
    try {
      replay_into(s, path);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    assert(threw && stat(path.c_str(), &st) == 0 && st.st_size == size);
  }

  // Replay must rebuild exactly what was live. GETs aren't logged, so it can't
  // re-run eviction itself; the log carries each eviction as a DEL, and a DEL
  // of a key that is no longer in memory is logged too.
  unlink(path.c_str());
  {
    Store s(1, 3 * 66);
    Aof aof(path, 10);
    s.attach_aof(&aof);
    s.set("a", "1", 0);
    s.set("b", "2", 0);
    s.set("c", "3", 0);
    assert(s.get("a", &v));  // b is now the coldest
    s.set("d", "4", 0);      // evicts b
    assert(!s.del("b"));     // a cache invalidation that finds nothing
  }
  for (size_t cap : {3 * 66, 1 << 20}) {  // same budget, and a bigger one
    Store s(1, cap);
    replay_into(s, path);
    assert(!s.get("b", &v) && s.get("a", &v) && s.get("c", &v) && s.get("d", &v));
  }
  {
    Store s(1, 2 * 66);  // the budget shrank: replay keeps all 3, attach trims to 2
    replay_into(s, path);
    s.attach_aof(nullptr);
    assert(s.stats().keys == 2 && s.stats().bytes <= 2 * 66);
  }
  unlink(path.c_str());
}

static void test_ring() {
  const std::vector<std::string> three = {"10.0.0.1:7000", "10.0.0.2:7000", "10.0.0.3:7000"};
  std::vector<std::string> four = three;
  four.push_back("10.0.0.4:7000");
  shardkv::Cluster c3(three, 2), c4(four, 2);
  const int kKeys = 30000;
  int load[3] = {}, moved = 0;
  for (int i = 0; i < kKeys; i++) {
    const std::string key = "key" + std::to_string(i);
    auto p = c3.preference_list(key);
    assert(p.size() == 2 && p[0] != p[1]);
    load[p[0]]++;
    size_t q = c4.preference_list(key)[0];
    if (q != p[0]) {
      assert(q == 3);  // adding a node only moves keys onto the new node
      moved++;
    }
  }
  for (int n : load) assert(n > kKeys / 3 * 0.8 && n < kKeys / 3 * 1.2);  // roughly balanced
  assert(moved > kKeys / 4 * 0.8 && moved < kKeys / 4 * 1.2);            // ~1/N of keys move
  printf("ring: primaries %d/%d/%d, %d of %d keys moved when adding a 4th node\n", load[0], load[1],
         load[2], moved, kKeys);
}

// One node that answers a single command and then hangs, like a SIGSTOPped
// server: the kernel still accepts connections, but nothing ever replies.
static void test_client() {
  int lfd = socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  socklen_t alen = sizeof addr;
  assert(bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) == 0 && listen(lfd, 8) == 0 &&
         getsockname(lfd, reinterpret_cast<sockaddr*>(&addr), &alen) == 0);
  std::thread node([lfd] {
    int fd = accept(lfd, nullptr, nullptr);
    char buf[256];
    assert(read(fd, buf, sizeof buf) > 0 && write(fd, "+OK\r\n", 5) == 5);
    std::this_thread::sleep_for(1500ms);
    close(fd);
  });
  const std::string me = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));
  bool threw = false;
  try {
    shardkv::Cluster({me, me});
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  assert(threw);

  shardkv::Cluster c({me}, 1, 10, 200);
  assert(c.set("k", "v"));
  threw = false;
  try {  // the server would hang up on it, so the client refuses to send it
    c.set("k", std::string(resp::kMaxBulk + 1, 'x'));
  } catch (const std::invalid_argument&) {
    threw = true;
  }
  assert(threw);
  assert(!c.set("k", "v"));  // pooled connection times out, then a fresh one does
  // The 1 s backoff counts from that failure, not from when the attempt began
  // (two timeouts earlier), so the node is still skipped without waiting.
  std::this_thread::sleep_for(700ms);
  const auto t0 = std::chrono::steady_clock::now();
  assert(!c.set("k", "v") && std::chrono::steady_clock::now() - t0 < 100ms);
  node.join();
  close(lfd);
}

static void test_concurrent() {
  const std::string path = tmp_path("concurrent");
  unlink(path.c_str());
  Store s(8, 1 << 14);  // small, so eviction runs too
  Aof aof(path, 5);
  s.attach_aof(&aof);
  std::atomic<uint64_t> gets{0};
  std::vector<std::thread> ts;
  for (int t = 0; t < 8; t++)
    ts.emplace_back([&, t] {
      std::mt19937 rng(t);
      std::string v;
      for (int i = 0; i < 20000; i++) {
        const std::string k = "k" + std::to_string(rng() % 500);
        switch (rng() % 3) {
          case 0: s.set(k, "value", rng() % 2 ? 0 : now_ms() + 1); break;
          case 1: s.get(k, &v), gets++; break;
          default: s.del(k);
        }
      }
    });
  std::thread sweeper([&] {
    for (int i = 0; i < 50; i++, std::this_thread::sleep_for(1ms)) s.sweep();
  });
  for (auto& t : ts) t.join();
  sweeper.join();
  Store::Stats st = s.stats();
  assert(st.hits + st.misses == gets && st.bytes <= (1 << 14) && st.evictions > 0);

  // Replaying what 8 racing writers logged gives back exactly the live store.
  std::this_thread::sleep_for(5ms);  // let every 1 ms TTL lapse in both
  Store r(8, 1 << 14);
  replay_into(r, path);
  for (int i = 0; i < 500; i++) {
    const std::string k = "k" + std::to_string(i);
    std::string a, b;
    assert(s.get(k, &a) == r.get(k, &b) && a == b);
  }
  unlink(path.c_str());
}

int main() {
  test_resp();
  test_lru();
  test_ttl();
  test_aof();
  test_ring();
  test_client();
  test_concurrent();
  printf("unit tests: PASS\n");
}
