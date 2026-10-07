// kvbench: closed-loop load generator. Each thread owns a Cluster client and
// issues GET/SET back-to-back for --duration seconds, timing every request.
#include <algorithm>
#include <atomic>
#include <cstdio>
#include <random>
#include <thread>

#include "client.h"

int main(int argc, char** argv) {
  std::string nodes = "127.0.0.1:6380";
  int replicas = 2, threads = 8, keys = 100000, value_size = 100;
  double get_ratio = 0.9, duration = 10;
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
    else ok = false;
  }
  if (!ok || threads <= 0 || keys <= 0 || value_size < 0 || get_ratio < 0 || get_ratio > 1) {
    fprintf(stderr,
            "usage: kvbench [--nodes h:p,...] [--replicas 2] [--threads 8] [--keys 100000]\n"
            "               [--value-size 100] [--get-ratio 0.9] [--duration 10]\n");
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

  std::atomic<bool> stop{false};
  std::vector<std::vector<uint64_t>> lat_ns(threads);  // per thread, so no sharing
  std::vector<uint64_t> errors(threads);
  std::vector<std::thread> pool;
  const auto start = std::chrono::steady_clock::now();
  for (int t = 0; t < threads; t++)
    pool.emplace_back([&, t] {
      std::mt19937_64 rng(t + 1);
      std::bernoulli_distribution is_get(get_ratio);
      const std::string value(value_size, 'x');
      while (!stop.load(std::memory_order_relaxed)) {
        const std::string key = "key:" + std::to_string(rng() % keys);
        const auto t0 = std::chrono::steady_clock::now();
        bool done = true;
        try {
          if (is_get(rng)) clients[t].get(key);
          else done = clients[t].set(key, value);
        } catch (const std::exception&) {
          done = false;
        }
        const auto dt = std::chrono::steady_clock::now() - t0;
        if (done) lat_ns[t].push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(dt).count());
        else errors[t]++;
      }
    });
  std::this_thread::sleep_for(std::chrono::duration<double>(duration));
  stop = true;
  for (auto& th : pool) th.join();
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  std::vector<uint64_t> all;
  uint64_t errs = 0;
  for (int t = 0; t < threads; t++) {
    all.insert(all.end(), lat_ns[t].begin(), lat_ns[t].end());
    errs += errors[t];
  }
  std::sort(all.begin(), all.end());
  auto pct_us = [&](double p) {
    return all.empty() ? 0.0 : all[std::min(all.size() - 1, size_t(p * all.size()))] / 1000.0;
  };
  printf("kvbench: %d threads, %zu nodes, R=%d, %d keys, %d B values, %.0f%% GET, %.1f s\n",
         threads, addrs.size(), replicas, keys, value_size, get_ratio * 100, secs);
  printf("ops %zu  errors %llu  throughput %.0f ops/s\n", all.size(), (unsigned long long)errs,
         all.size() / secs);
  printf("latency p50 %.1f us  p95 %.1f us  p99 %.1f us\n", pct_us(0.50), pct_us(0.95),
         pct_us(0.99));
  return all.empty() ? 1 : 0;
}
