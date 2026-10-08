#pragma once
// Sharded in-memory LRU cache with TTLs. Keys hash to one of N shards; each
// shard has its own mutex, so threads touching different shards don't contend
// on the store (with an AOF, writes still share its log; see Aof::append).
// Each shard is an O(1) LRU: a list ordered by recency plus a hash map from
// key to list node. Entries can carry a version for last-writer-wins
// replication, and a tombstone is an entry that records a versioned delete.
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

constexpr int64_t kTombGraceMs = 600000;  // how long a delete is remembered (tombstone GC)

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

  // True only for a live value. *ver gets the entry's version, value or
  // tombstone (0 if there is no entry), *expire_at the value's deadline.
  bool get(const std::string& key, std::string* val, uint64_t* ver = nullptr,
           int64_t* expire_at = nullptr) {
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    auto it = s.map.find(key);
    if (it != s.map.end() && expire(s, it->second, now_ms())) it = s.map.end();  // lazy expiry
    if (ver) *ver = it == s.map.end() ? 0 : it->second->ver;
    if (it == s.map.end() || it->second->tomb) {
      s.misses++;
      return false;
    }
    s.lru.splice(s.lru.begin(), s.lru, it->second);  // now most recently used
    s.hits++;
    *val = it->second->val;
    if (expire_at) *expire_at = it->second->expire_at;
    return true;
  }

  // expire_at: absolute unix time in ms, 0 = never. Returns false if the entry
  // alone is bigger than a shard's budget: it is not stored, and any old value
  // for the key is dropped, as if the new one had been evicted at once.
  // ver > 0 makes it a last-writer-wins write: it is ignored (and not logged,
  // so replay stays exact) if the key holds a live entry, value or tombstone,
  // with a version >= ver. Unless it was the same write again (equal version
  // and value), *newer then gets the version that beat it. ver 0 always
  // applies. tomb stores a tombstone.
  bool set(const std::string& key, std::string val, int64_t expire_at, bool evict = true,
           uint64_t ver = 0, bool tomb = false, uint64_t* newer = nullptr) {
    const bool fits = key.size() + val.size() + kOverhead <= shard_cap_;
    std::string log;  // encoded before taking the shard lock
    if (aof_ && fits) {
      std::vector<std::string> rec{"SET", key, val};
      if (expire_at) rec.insert(rec.end(), {"PXAT", std::to_string(expire_at)});
      if (ver) rec.insert(rec.end(), {"VER", std::to_string(ver)});
      if (tomb) rec.insert(rec.end(), {"TOMB", "1"});
      log = Aof::record(rec);
    } else if (aof_) {
      log = Aof::record({"DEL", key});
    }
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    const int64_t now = now_ms();
    auto it = s.map.find(key);
    if (it != s.map.end() && expire(s, it->second, now)) it = s.map.end();
    if (ver && it != s.map.end() && it->second->ver >= ver) {
      const Entry& e = *it->second;  // a retry or repeated read repair is a no-op, not a loss
      if (newer && (e.ver > ver || e.tomb != tomb || e.val != val)) *newer = e.ver;
      return true;
    }
    if (it != s.map.end()) remove(s, it->second);
    if (fits) {
      s.lru.push_front(Entry{key, std::move(val), expire_at, ver, tomb});
      s.map.emplace(s.lru.front().key, s.lru.begin());
      s.bytes += cost(s.lru.front());
      expire(s, s.lru.begin(), now);  // a deadline already in the past, e.g. an old TTL replayed
      if (evict) trim(s, log);
    }
    if (aof_) aof_->append(log);
    return fits;
  }

  // True if a live value was removed (a tombstone doesn't count). The DEL is
  // logged even when the key isn't in memory, so no older SET in the log can
  // bring it back on replay.
  bool del(const std::string& key) {
    const std::string log = aof_ ? Aof::record({"DEL", key}) : "";
    Shard& s = shard(key);
    std::lock_guard<std::mutex> lk(s.mu);
    if (aof_) aof_->append(log);
    auto it = s.map.find(key);
    if (it == s.map.end() || expire(s, it->second, now_ms())) return false;
    const bool live = !it->second->tomb;
    remove(s, it->second);
    return live;
  }

  // Re-executes a command read back from the AOF (format: see set/del).
  // Evictions are in the log as DELs, so replay must not pick its own victims:
  // GETs aren't logged, so its LRU order differs from the live server's.
  void apply(std::vector<std::string>& cmd) {
    if (cmd[0] == "SET" && cmd.size() >= 3) {
      int64_t at = 0;
      uint64_t ver = 0;
      bool tomb = false;
      for (size_t i = 3; i + 1 < cmd.size(); i += 2) {  // option pairs; unknown ones are skipped
        if (cmd[i] == "PXAT") at = strtoll(cmd[i + 1].c_str(), nullptr, 10);
        else if (cmd[i] == "VER") ver = strtoull(cmd[i + 1].c_str(), nullptr, 10);
        else if (cmd[i] == "TOMB") tomb = cmd[i + 1] == "1";
      }
      set(cmd[1], std::move(cmd[2]), at, false, ver, tomb);
    } else if (cmd[0] == "DEL" && cmd.size() == 2) {
      del(cmd[1]);
    }
  }

  // Expires every entry past its deadline (see expire()); the server runs this
  // from a sweeper thread. Returns how many expired.
  // ponytail: O(n) scan, one shard locked at a time; sample keys like Redis
  // does if keyspaces get large enough for the scan to hurt tail latency.
  size_t sweep() {
    size_t n = 0;
    int64_t now = now_ms();
    for (Shard& s : shards_) {
      std::lock_guard<std::mutex> lk(s.mu);
      for (auto it = s.lru.begin(); it != s.lru.end();) {
        auto next = std::next(it);
        n += dead(*it, now);
        expire(s, it, now);
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
    uint64_t ver = 0;   // 0 = unversioned
    bool tomb = false;  // a delete marker: GET misses; always has an expire_at
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
  // Handles an entry past its deadline; true if it was removed. A versioned
  // value becomes a tombstone that keeps its version for the grace period, or
  // a replica still holding an older version would win the next read and read
  // repair would copy that older value back here.
  static bool expire(Shard& s, List::iterator e, int64_t now) {
    if (!dead(*e, now)) return false;
    s.expired++;
    if (e->ver && !e->tomb && e->expire_at + kTombGraceMs > now) {
      s.bytes -= e->val.size();
      std::string().swap(e->val);
      e->tomb = true;
      e->expire_at += kTombGraceMs;
      return false;
    }
    remove(s, e);
    return true;
  }
  Shard& shard(const std::string& key) {
    return shards_[std::hash<std::string>{}(key) % shards_.size()];
  }
  // Evicts from the cold end until the shard fits its budget, adding a DEL
  // record per victim to `log`.
  void trim(Shard& s, std::string& log) {
    while (s.bytes > shard_cap_) {
      auto victim = std::prev(s.lru.end());
      if (aof_) log += Aof::record({"DEL", victim->key});
      (dead(*victim, now_ms()) ? s.expired : s.evictions)++;
      remove(s, victim);
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
