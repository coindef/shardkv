// kvbench: closed-loop load generator. Each thread owns a Cluster client and
// issues GET/SET back-to-back for --duration seconds, timing every request.
// --preload 1 first SETs every key once (untimed) so GETs can hit, and
// --interval S prints per-interval counts while it runs (e.g. during failover).
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <random>
#include <thread>

#include "client.h"

// Per-thread counters, read by the main thread for --interval lines.
struct alignas(64) Counts {
  std::atomic<uint64_t> get_ok{0}, get_hit{0}, get_err{0}, set_ok{0}, set_err{0};
};

int main(int argc, char** argv) {
  std::string nodes = "127.0.0.1:6380";
  int replicas = 2, threads = 8, keys = 100000, value_size = 100, preload = 0;
  double get_ratio = 0.9, duration = 10, interval = 0;
  bool ok = argc % 2 == 1;
  for (int i = 1; ok && i + 1 < argc; i += 2) {
    const std::string f = argv[i];
    const char* v = argv[i + 1];
    if (f == "--nodes") nodes = v;
    else if (f == "--replicas") replicas = atoi(v);
    else if (f == "--threads") threads = atoi(v);
    else if (f == "--keys") keys = atoi(v);
    else if (f == "--value-size") value_size = atoi(v);
    else if (f == "--get-ratio") get_ratio = atof(v);
    else if (f == "--duration") duration = atof(v);
    else if (f == "--preload") preload = atoi(v);
    else if (f == "--interval") interval = atof(v);
    else ok = false;
  }
  if (!ok || threads <= 0 || keys <= 0 || value_size < 0 || get_ratio < 0 || get_ratio > 1 ||
      interval < 0) {
    fprintf(stderr,
            "usage: kvbench [--nodes h:p,...] [--replicas 2] [--threads 8] [--keys 100000]\n"
            "               [--value-size 100] [--get-ratio 0.9] [--duration 10]\n"
            "               [--preload 0|1] [--interval 0 (seconds; 0 = off)]\n");
    return 2;
  }

  const auto addrs = shardkv::split(nodes, ',');
  std::vector<shardkv::Cluster> clients;  // one per thread: Cluster is not thread-safe
  try {
    for (int t = 0; t < threads; t++) clients.emplace_back(addrs, replicas);
  } catch (const std::exception& e) {
    fprintf(stderr, "kvbench: %s\n", e.what());
    return 2;
  }
  const std::string value(value_size, 'x');

  if (preload) {  // every key once, split across the threads; not timed
    std::atomic<int> next{0};
    std::atomic<uint64_t> failed{0};
    std::vector<std::thread> pool;
    const auto t0 = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; t++)
      pool.emplace_back([&, t] {
        for (int k; (k = next++) < keys;) {
          bool done = false;
          try {
            done = clients[t].set("key:" + std::to_string(k), value);
          } catch (const std::exception&) {
          }
          if (!done) failed++;
        }
      });
    for (auto& th : pool) th.join();
    printf("preload: %d keys written in %.1f s, %llu failed\n", keys,
           std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(),
           (unsigned long long)failed.load());
    fflush(stdout);  // bench.py starts its failover clock on this line
  }

  std::atomic<bool> stop{false};
  std::vector<std::vector<uint64_t>> lat_ns(threads);  // per thread, so no sharing
  std::vector<Counts> counts(threads);
  std::vector<std::thread> pool;
  const auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < threads; t++)
    pool.emplace_back([&, t] {
      std::mt19937_64 rng(t + 1);
      std::bernoulli_distribution is_get(get_ratio);
      Counts& c = counts[t];
      while (!stop.load(std::memory_order_relaxed)) {
        const std::string key = "key:" + std::to_string(rng() % keys);
        const bool get = is_get(rng);
        const auto t0 = std::chrono::steady_clock::now();
        bool done = true, hit = false;
        try {
          if (get) hit = clients[t].get(key).has_value();
          else done = clients[t].set(key, value);
        } catch (const std::exception&) {
          done = false;
        }
        const auto dt = std::chrono::steady_clock::now() - t0;
        if (done) lat_ns[t].push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count());
        auto& n = get ? (done ? c.get_ok : c.get_err) : (done ? c.set_ok : c.set_err);
        n.fetch_add(1, std::memory_order_relaxed);
        if (hit) c.get_hit.fetch_add(1, std::memory_order_relaxed);
      }
    });

  // {get_ok, get_hit, get_err, set_ok, set_err} summed over threads
  auto totals = [&] {
    std::array<uint64_t, 5> s{};
    for (const Counts& c : counts) {
      s[0] += c.get_ok;
      s[1] += c.get_hit;
      s[2] += c.get_err;
      s[3] += c.set_ok;
      s[4] += c.set_err;
    }
    return s;
  };
  using dsec = std::chrono::duration<double>;
  const auto end = start + std::chrono::duration_cast<std::chrono::steady_clock::duration>(dsec(duration));
  if (interval > 0) {
    auto prev = totals();
    for (int i = 1;; i++) {
      const auto tick = start + std::chrono::duration_cast<std::chrono::steady_clock::duration>(dsec(interval * i));
      if (tick > end) break;
      std::this_thread::sleep_until(tick);
      const auto now = totals();
      printf("t=%5.1f s  GET ok %llu err %llu  SET ok %llu err %llu  %.0f ops/s\n", interval * i,
             (unsigned long long)(now[0] - prev[0]), (unsigned long long)(now[2] - prev[2]),
             (unsigned long long)(now[3] - prev[3]), (unsigned long long)(now[4] - prev[4]),
             (now[0] - prev[0] + now[3] - prev[3]) / interval);
      fflush(stdout);
      prev = now;
    }
  }
  std::this_thread::sleep_until(end);
  stop = true;
  for (auto& th : pool) th.join();
  const double secs = dsec(std::chrono::steady_clock::now() - start).count();

  std::vector<uint64_t> all;
  for (int t = 0; t < threads; t++) all.insert(all.end(), lat_ns[t].begin(), lat_ns[t].end());
  const auto s = totals();
  std::sort(all.begin(), all.end());
  auto pct_us = [&](double p) {
    return all.empty() ? 0.0 : all[std::min(all.size() - 1, size_t(p * all.size()))] / 1000.0;
  };
  printf("kvbench: %d threads, %zu nodes, R=%d, %d keys, %d B values, %.0f%% GET, %.1f s\n",
         threads, addrs.size(), replicas, keys, value_size, get_ratio * 100, secs);
  printf("ops %zu  errors %llu  throughput %.0f ops/s\n", all.size(),
         (unsigned long long)(s[2] + s[4]), all.size() / secs);
  printf("GET ok %llu  hits %llu  hit rate %.2f%%  errors %llu;  SET ok %llu  errors %llu\n",
         (unsigned long long)s[0], (unsigned long long)s[1], s[0] ? 100.0 * s[1] / s[0] : 0.0,
         (unsigned long long)s[2], (unsigned long long)s[3], (unsigned long long)s[4]);
  printf("latency p50 %.1f us  p95 %.1f us  p99 %.1f us\n", pct_us(0.50), pct_us(0.95),
         pct_us(0.99));
  return all.empty() ? 1 : 0;
}
