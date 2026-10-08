#!/usr/bin/env python3
"""Reproducible benchmark. For each config it starts fresh kvserver nodes,
prefills 10k keys, runs kvbench 3 times and prints the median run as a
Markdown row. If redis-server and redis-benchmark are installed, it also
compares one kvserver with one redis-server. Not run in CI.

Usage: bench.py [BIN_DIR] [--seconds 5]   (default BIN_DIR: build)
"""
import argparse
import csv
import datetime
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from integration_test import call, free_port  # noqa: E402

KEYS, VALUE_SIZE, RUNS = 10000, 100, 3
# (nodes, R, threads, GET ratio, AOF)
CONFIGS = [
    (1, 1, 1, 0.9, True),  # single-client latency
    (1, 1, 16, 0.0, True),  # write throughput with and without the AOF
    (1, 1, 16, 0.0, False),
    (3, 2, 8, 0.9, True),
    (3, 2, 8, 0.9, False),
    (3, 2, 8, 0.0, True),
    (3, 2, 8, 0.0, False),
    (3, 1, 8, 0.9, True),  # vs the R=2 row: the cost of reading both replicas
]


def start(cmd, port, tmp):
    with open(os.path.join(tmp, "server%d.log" % port), "ab") as log:
        p = subprocess.Popen(cmd, stdout=log, stderr=log)
    deadline = time.time() + 15
    while time.time() < deadline:
        assert p.poll() is None, "%s exited early, see %s" % (cmd[0], tmp)
        try:
            if call(port, "PING") == "PONG":
                return p
        except OSError:
            time.sleep(0.05)
    raise SystemExit("%s on port %d never came up" % (cmd[0], port))


def stop(p):
    p.send_signal(signal.SIGTERM)
    assert p.wait(timeout=60) == 0, "server exit code %s" % p.returncode


def kvserver(bin_dir, port, tmp, aof):
    cmd = [os.path.join(bin_dir, "kvserver"), "--port", str(port)]
    return start(cmd + (["--aof", os.path.join(tmp, "%d.aof" % port)] if aof else []), port, tmp)


def kvbench(bin_dir, addrs, r, threads, get_ratio, seconds):
    out = subprocess.run(
        [os.path.join(bin_dir, "kvbench"), "--nodes", addrs, "--replicas", str(r), "--threads",
         str(threads), "--keys", str(KEYS), "--value-size", str(VALUE_SIZE), "--get-ratio",
         str(get_ratio), "--duration", str(seconds)],
        capture_output=True, text=True, timeout=seconds + 120).stdout
    m = re.search(r"errors (\d+)\s+throughput (\d+) ops/s.*p50 ([\d.]+) us.*p99 ([\d.]+) us", out, re.S)
    if not m:
        sys.exit("cannot parse kvbench output:\n" + out)
    if int(m[1]):
        sys.exit("kvbench reported errors:\n" + out)
    return int(m[2]), float(m[3]), float(m[4])


def cpu():
    if sys.platform == "darwin":
        return subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                              text=True).stdout.strip()
    with open("/proc/cpuinfo") as f:
        return next((l.split(":", 1)[1].strip() for l in f if l.startswith("model name")), "unknown")


def redis_benchmark(port):
    """Median-rps run per test: {test: (rps, p50 ms, p99 ms)}; latencies '-' if not reported."""
    runs = {}
    for _ in range(RUNS):
        out = subprocess.run(["redis-benchmark", "-p", str(port), "-t", "set,get", "-n", "200000",
                              "-c", "50", "-r", str(KEYS), "-d", str(VALUE_SIZE), "--csv"],
                             capture_output=True, text=True, timeout=600).stdout
        rows = [r for r in csv.reader(out.splitlines()) if r]
        head = rows.pop(0) if rows and rows[0][0] == "test" else []
        for r in rows:
            col = lambda name: r[head.index(name)] if name in head else "-"  # noqa: E731
            runs.setdefault(r[0], []).append((float(r[1]), col("p50_latency_ms"), col("p99_latency_ms")))
    return {t: sorted(v)[len(v) // 2] for t, v in runs.items()}


def compare_redis(bin_dir):
    if not (shutil.which("redis-server") and shutil.which("redis-benchmark")):
        print("redis-server/redis-benchmark not found; skipping Redis comparison")
        return
    print("\n`redis-benchmark -t set,get -n 200000 -c 50 -r %d -d %d`, one server, median of %d runs\n"
          % (KEYS, VALUE_SIZE, RUNS))
    print("| server | AOF | test | req/s | p50 ms | p99 ms |\n|---|---|---|---:|---:|---:|")
    notes = []
    for aof in (False, True):
        res = {}
        for name in ("kvserver", "redis-server"):
            tmp, port = tempfile.mkdtemp(prefix="shardkv_bench_"), free_port()
            if name == "kvserver":
                p = kvserver(bin_dir, port, tmp, aof)
            else:
                p = start(["redis-server", "--port", str(port), "--save", "", "--appendonly",
                           "yes" if aof else "no", "--appendfsync", "everysec", "--dir", tmp], port, tmp)
            res[name] = redis_benchmark(port)
            stop(p)
            shutil.rmtree(tmp)
            for t, (rps, p50, p99) in sorted(res[name].items(), reverse=True):
                print("| %s | %s | %s | %.0f | %s | %s |" % (name, "on" if aof else "off", t, rps, p50, p99))
        for t in ("SET", "GET"):
            if t in res["kvserver"] and t in res["redis-server"]:
                notes.append("kvserver %s throughput is %.0f%% of redis-server's, AOF %s" % (
                    t, 100 * res["kvserver"][t][0] / res["redis-server"][t][0], "on" if aof else "off"))
    print("\n" + "\n".join(notes))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("bin_dir", nargs="?", default="build")
    ap.add_argument("--seconds", type=float, default=5)
    a = ap.parse_args()
    print("%s, %s, %s, %d cores" % (datetime.date.today(), platform.platform(), cpu(), os.cpu_count()))
    print("kvbench: %d keys prefilled, %d B values, median of %d x %g s runs; client and servers on "
          "this machine\n" % (KEYS, VALUE_SIZE, RUNS, a.seconds))
    print("| nodes | R | AOF | threads | GET % | ops/s | p50 µs | p99 µs |\n"
          "|---:|---:|---|---:|---:|---:|---:|---:|", flush=True)
    for nodes, r, threads, get_ratio, aof in CONFIGS:
        tmp = tempfile.mkdtemp(prefix="shardkv_bench_")
        ports = [free_port() for _ in range(nodes)]
        procs = [kvserver(a.bin_dir, port, tmp, aof) for port in ports]
        addrs = ",".join("127.0.0.1:%d" % port for port in ports)
        kvbench(a.bin_dir, addrs, r, 8, 0, 2)  # prefill, so GETs hit
        runs = sorted(kvbench(a.bin_dir, addrs, r, threads, get_ratio, a.seconds) for _ in range(RUNS))
        ops, p50, p99 = runs[len(runs) // 2]
        print("| %d | %d | %s | %d | %d | %d | %.0f | %.0f |" % (
            nodes, r, "on" if aof else "off", threads, round(get_ratio * 100), ops, p50, p99), flush=True)
        for p in procs:
            stop(p)
        shutil.rmtree(tmp)
    compare_redis(a.bin_dir)


if __name__ == "__main__":
    main()
