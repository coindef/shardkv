#pragma once
// Sharded in-memory LRU cache with TTLs. Keys hash to one of N shards; each
// shard has its own mutex, so threads touching different shards don't contend
// on the store (with an AOF, writes still share its log; see Aof::append).
// Each shard is an O(1) LRU: a list ordered by recency plus a hash map from
// key to list node.
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <list>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "aof.h"

// Wall-clock ms. Expiry deadlines are absolute so they stay valid across restarts.
inline int64_t now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

class Store {
 public:
  struct Stats {
    uint64_t keys = 0, bytes = 0, hits = 0, misses = 0, evictions = 0, expired = 0;
  };

  explicit Store(size_t shards = 16, size_t max_bytes = size_t(256) << 20)
      : shards_(shards ? shards : 1), shard_cap_(max_bytes / shards_.size()) {}

  // Call once replay is done. From now on every mutation, evictions included,
  // is logged while its shard lock is held, so the log order for a key always
  // matches the in-memory order.
  void attach_aof(Aof* aof) {
    aof_ = aof;
    for (Shard& s : shards_) {  // replay doesn't evict: trim shards over a shrunk budget
      std::string log;
      std::lock_guard<std::mutex> lk(s.mu);
      trim(s, log);
      if (aof_ && !log.empty()) aof_->append(log);
    }
  }

  bool get(const std::string& key, std::string* val) {
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    auto it = s.map.find(key);
    if (it != s.map.end() && dead(*it->second, now_ms())) {  // lazy expiry
      remove(s, it->second);
      s.expired++;
      it = s.map.end();
    }
    if (it == s.map.end()) {
      s.misses++;
      return false;
    }
    s.lru.splice(s.lru.begin(), s.lru, it->second);  // now most recently used
    s.hits++;
    *val = it->second->val;
    return true;
  }

  // expire_at: absolute unix time in ms, 0 = never. Returns false if the entry
  // alone is bigger than a shard's budget: it is not stored, and any old value
  // for the key is dropped, as if the new one had been evicted at once.
  bool set(const std::string& key, std::string val, int64_t expire_at, bool evict = true) {
    const bool fits = key.size() + val.size() + kOverhead <= shard_cap_;
    std::string log;  // encoded before taking the shard lock
    if (aof_ && fits) {
      std::vector<std::string> rec{"SET", key, val};
      if (expire_at) rec.insert(rec.end(), {"PXAT", std::to_string(expire_at)});
      log = Aof::record(rec);
    } else if (aof_) {
      log = Aof::record({"DEL", key});
    }
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    if (auto it = s.map.find(key); it != s.map.end()) remove(s, it->second);
    // A deadline already in the past (e.g. replaying an old TTL) stores nothing.
    if (fits && !(expire_at && expire_at <= now_ms())) {
      s.lru.push_front(Entry{key, std::move(val), expire_at});
      s.map.emplace(s.lru.front().key, s.lru.begin());
      s.bytes += cost(s.lru.front());
      if (evict) trim(s, log);
    }
    if (aof_) aof_->append(log);
    return fits;
  }

  // True if a live key was removed. The DEL is logged even when the key isn't
  // in memory, so no older SET in the log can bring it back on replay.
  bool del(const std::string& key) {
    const std::string log = aof_ ? Aof::record({"DEL", key}) : "";
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    if (aof_) aof_->append(log);
    auto it = s.map.find(key);
    if (it == s.map.end()) return false;
    bool live = !dead(*it->second, now_ms());
    if (!live) s.expired++;
    remove(s, it->second);
    return live;
  }

  // Re-executes a command read back from the AOF (format: see set/del).
  // Evictions are in the log as DELs, so replay must not pick its own victims:
  // GETs aren't logged, so its LRU order differs from the live server's.
  void apply(std::vector<std::string>& cmd) {
    if (cmd[0] == "SET" && cmd.size() >= 3)
      set(cmd[1], std::move(cmd[2]), cmd.size() == 5 ? strtoll(cmd[4].c_str(), nullptr, 10) : 0,
          false);
    else if (cmd[0] == "DEL" && cmd.size() == 2)
      del(cmd[1]);
  }

  // Drops all expired entries; the server runs this from a sweeper thread.
  // ponytail: O(n) scan, one shard locked at a time; sample keys like Redis
  // does if keyspaces get large enough for the scan to hurt tail latency.
  size_t sweep() {
    size_t n = 0;
    int64_t now = now_ms();
    for (Shard& s : shards_) {
      std::lock_guard<std::mutex> lk(s.mu);
      for (auto it = s.lru.begin(); it != s.lru.end();) {
        auto next = std::next(it);
        if (dead(*it, now)) {
          remove(s, it);
          s.expired++;
          n++;
        }
        it = next;
      }
    }
    return n;
  }

  Stats stats() {
    Stats t;
    for (Shard& s : shards_) {
      std::lock_guard<std::mutex> lk(s.mu);
      t.keys += s.map.size();
      t.bytes += s.bytes;
      t.hits += s.hits;
      t.misses += s.misses;
      t.evictions += s.evictions;
      t.expired += s.expired;
    }
    return t;
  }

 private:
  struct Entry {
    std::string key, val;
    int64_t expire_at;
  };
  using List = std::list<Entry>;
  struct Shard {
    std::mutex mu;
    List lru;  // front = most recently used
    std::unordered_map<std::string_view, List::iterator> map;  // keys view into lru nodes
    size_t bytes = 0;
    uint64_t hits = 0, misses = 0, evictions = 0, expired = 0;
  };

  // ponytail: flat 64 B per-entry overhead estimate, not exact allocator usage.
  static constexpr size_t kOverhead = 64;
  static size_t cost(const Entry& e) { return e.key.size() + e.val.size() + kOverhead; }
  static bool dead(const Entry& e, int64_t now) { return e.expire_at && e.expire_at <= now; }
  Shard& shard(const std::string& key) {
    return shards_[std::hash<std::string>{}(key) % shards_.size()];
  }
  // Evicts from the cold end until the shard fits its budget, adding a DEL
  // record per victim to `log`.
  void trim(Shard& s, std::string& log) {
    while (s.bytes > shard_cap_) {
      auto victim = std::prev(s.lru.end());
      if (aof_) log += Aof::record({"DEL", victim->key});
      remove(s, victim);
      s.evictions++;
    }
  }
  static void remove(Shard& s, List::iterator e) {
    s.bytes -= cost(*e);
    s.map.erase(e->key);  // drop the view before the string it points into
    s.lru.erase(e);
  }

  std::vector<Shard> shards_;
  size_t shard_cap_;
  Aof* aof_ = nullptr;
};
