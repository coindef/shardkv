#!/usr/bin/env python3
"""Reproducible benchmark. For each config it starts fresh kvserver nodes
(no AOF), runs kvbench 3 times (each run first writes all 100k keys with
--preload 1, then measures) and prints the median-throughput run as a
Markdown row. Then a failover check: 3 nodes, R=2, a 20 s kvbench with one
node SIGKILLed at about 10 s. If redis-server and redis-benchmark are
installed, it also compares one kvserver with one redis-server. Not run in CI.

Usage: bench.py [BIN_DIR] [--seconds 15]   (default BIN_DIR: build)
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

KEYS, VALUE_SIZE, RUNS = 100000, 128, 3
# (name, nodes, R, threads, GET ratio)
CONFIGS = [
    ("a", 1, 1, 8, 0.9),
    ("b", 3, 2, 8, 0.9),
    ("c", 3, 2, 16, 0.5),
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


def kvbench_cmd(bin_dir, addrs, r, threads, get_ratio, seconds, *extra):
    return [os.path.join(bin_dir, "kvbench"), "--nodes", addrs, "--replicas", str(r), "--threads",
            str(threads), "--keys", str(KEYS), "--value-size", str(VALUE_SIZE), "--get-ratio",
            str(get_ratio), "--duration", str(seconds), "--preload", "1"] + list(extra)


def kvbench(bin_dir, addrs, r, threads, get_ratio, seconds):
    """(ops/s, p50, p95, p99, GET hit %) of one run; exits on any error."""
    out = subprocess.run(kvbench_cmd(bin_dir, addrs, r, threads, get_ratio, seconds),
                         capture_output=True, text=True, timeout=seconds + 300).stdout
    m = re.search(r"preload: \d+ keys written in [\d.]+ s, (\d+) failed.*errors (\d+)\s+throughput "
                  r"(\d+) ops/s.*hit rate ([\d.]+)%.*p50 ([\d.]+) us\s+p95 ([\d.]+) us\s+p99 ([\d.]+) us",
                  out, re.S)
    if not m:
        sys.exit("cannot parse kvbench output:\n" + out)
    if int(m[1]) or int(m[2]):
        sys.exit("kvbench reported errors:\n" + out)
    return int(m[3]), float(m[5]), float(m[6]), float(m[7]), float(m[4])


def failover(bin_dir, seconds=20, kill_at=10):
    """3 nodes, R=2, 8 threads, 90% GET; SIGKILL one node about kill_at s in."""
    tmp = tempfile.mkdtemp(prefix="shardkv_bench_")
    ports = [free_port() for _ in range(3)]
    procs = [kvserver(bin_dir, port, tmp, False) for port in ports]
    addrs = ",".join("127.0.0.1:%d" % port for port in ports)
    cmd = kvbench_cmd(bin_dir, addrs, 2, 8, 0.9, seconds, "--interval", "1")
    print("\nFailover: `%s`, SIGKILL node 2 of 3 at about %d s\n" % (
        " ".join(os.path.basename(c) if i == 0 else c for i, c in enumerate(cmd)), kill_at))
    b = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    out = []
    for line in b.stdout:  # the measured run starts right after the preload line
        out.append(line)
        if line.startswith("preload:"):
            break
    t0 = time.time()
    time.sleep(kill_at)
    procs[1].kill()
    procs[1].wait()
    killed = time.time() - t0
    out += b.stdout.readlines()
    assert b.wait() == 0, "".join(out)
    print("```\n%s```" % "".join(out))
    ticks = [(float(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5]), float(m[6])) for m in re.finditer(
        r"t=\s*([\d.]+) s\s+GET ok (\d+) err (\d+)\s+SET ok (\d+) err (\d+)\s+(\d+) ops/s", "".join(out))]
    before = [t for t in ticks if t[0] <= killed]  # intervals that ended before the kill
    after = [t for t in ticks if t[0] - 1 >= killed]  # intervals that started after it
    mean = lambda ts: sum(t[5] for t in ts) / len(ts)  # noqa: E731
    print("killed at %.2f s; mean ops/s: %.0f over %d s before, %.0f over %d s after; "
          "GET errors %d, SET errors %d over the whole run" % (
              killed, mean(before), len(before), mean(after), len(after),
              sum(t[2] for t in ticks), sum(t[4] for t in ticks)))
    for i in (0, 2):
        stop(procs[i])
    shutil.rmtree(tmp)


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
    ap.add_argument("--seconds", type=float, default=15)
    a = ap.parse_args()
    print("%s, %s, %s, %d cores" % (datetime.date.today(), platform.platform(), cpu(), os.cpu_count()))
    print("kvbench: %d keys preloaded before each run, %d B values, median of %d x %g s runs; "
          "client and servers on this machine\n" % (KEYS, VALUE_SIZE, RUNS, a.seconds))
    print("| run | nodes | R | threads | GET:SET | ops/s | p50 µs | p95 µs | p99 µs | GET hit rate "
          "| ops/s of all %d runs |\n|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---|" % RUNS,
          flush=True)
    for name, nodes, r, threads, get_ratio in CONFIGS:
        tmp = tempfile.mkdtemp(prefix="shardkv_bench_")
        ports = [free_port() for _ in range(nodes)]
        procs = [kvserver(a.bin_dir, port, tmp, False) for port in ports]
        addrs = ",".join("127.0.0.1:%d" % port for port in ports)
        runs = sorted(kvbench(a.bin_dir, addrs, r, threads, get_ratio, a.seconds) for _ in range(RUNS))
        ops, p50, p95, p99, hit = runs[len(runs) // 2]
        g = round(get_ratio * 100)
        print("| %s | %d | %d | %d | %d:%d | %d | %.1f | %.1f | %.1f | %.2f%% | %s |" % (
            name, nodes, r, threads, g, 100 - g, ops, p50, p95, p99, hit,
            ", ".join(str(x[0]) for x in runs)), flush=True)
        for p in procs:
            stop(p)
        shutil.rmtree(tmp)
    failover(a.bin_dir)
    compare_redis(a.bin_dir)


if __name__ == "__main__":
    main()
