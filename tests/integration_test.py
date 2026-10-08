#!/usr/bin/env python3
"""End-to-end test: raw protocol edge cases, a 3-node cluster written through
kvcli, SIGKILL of one node with failover reads, then restart + AOF recovery.

Usage: integration_test.py [BIN_DIR]   (default: build)
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

BIN = sys.argv[1] if len(sys.argv) > 1 else "build"
N_KEYS = 300


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def encode(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


def read_reply(f):
    """'+OK' -> 'OK', '-ERR x' -> '-ERR x', ':5' -> 5, '$..' -> str or None."""
    line = f.readline()
    assert line.endswith(b"\r\n"), "connection closed or bad reply: %r" % line
    kind, body = line[:1], line[1:-2].decode()
    if kind == b"$":
        n = int(body)
        return None if n < 0 else f.read(n + 2)[:-2].decode()
    if kind == b":":
        return int(body)
    return "-" + body if kind == b"-" else body


def pipeline(port, cmds):
    """Sends all commands in one write, then reads every reply."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        s.sendall(b"".join(encode(*c) for c in cmds))
        f = s.makefile("rb")
        return [read_reply(f) for _ in cmds]


def call(port, *args):
    return pipeline(port, [args])[0]


def send_raw(port, *chunks):
    with socket.create_connection(("127.0.0.1", port), timeout=10) as s:
        for c in chunks:
            s.sendall(c)
            time.sleep(0.05)
        return read_reply(s.makefile("rb"))


def kvcli(nodes, lines):
    r = subprocess.run([os.path.join(BIN, "kvcli"), "--nodes", nodes],
                       input="\n".join(lines) + "\n", capture_output=True, text=True, timeout=120)
    return r.stdout.splitlines()


class Node:
    def __init__(self, tmp, i):
        self.port = free_port()
        self.aof = os.path.join(tmp, "node%d.aof" % i)
        self.log = os.path.join(tmp, "node%d.log" % i)
        self.proc = None

    def start(self):
        with open(self.log, "ab") as log:
            self.proc = subprocess.Popen(
                [os.path.join(BIN, "kvserver"), "--port", str(self.port), "--aof", self.aof,
                 "--fsync-ms", "100"], stderr=log)
        deadline = time.time() + 15
        while time.time() < deadline:
            assert self.proc.poll() is None, "kvserver exited early, see " + self.log
            try:
                if call(self.port, "PING") == "PONG":
                    return
            except OSError:
                time.sleep(0.05)
        raise AssertionError("kvserver on port %d never came up" % self.port)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            self.proc.wait(timeout=10)


def test_protocol(port):
    assert pipeline(port, [("PING",), ("SET", "a", "1"), ("GET", "a"), ("DEL", "a"), ("GET", "a"),
                           ("DBSIZE",)]) == ["PONG", "OK", "1", 1, None, 0]
    # one command split across several TCP writes (partial reads on the server)
    assert send_raw(port, b"*3\r\n$3\r\nSE", b"T\r\n$1\r\nk\r", b"\n$5\r\nhello\r\n") == "OK"
    assert call(port, "GET", "k") == "hello"
    assert call(port, "NOPE").startswith("-ERR")
    assert call(port, "GET").startswith("-ERR")
    assert call(port, "SET", "k", "v", "PX", "abc").startswith("-ERR")
    for junk in [b"hello\r\n", b"*-5\r\n", b"*1\r\n$3\r\nGETxx\r\n", b"*2\r\n$999999999999\r\n"]:
        assert send_raw(port, junk).startswith("-ERR"), junk
    assert call(port, "PING") == "PONG"  # still alive after the garbage
    assert call(port, "SET", "t", "v", "PX", "1000") == "OK"  # wide: TSan on a shared CI runner
    assert call(port, "GET", "t") == "v"
    time.sleep(1.2)
    assert call(port, "GET", "t") is None
    assert "keys:" in call(port, "INFO")


def main():
    tmp = tempfile.mkdtemp(prefix="shardkv_it_")
    nodes = [Node(tmp, i) for i in range(3)]
    try:
        for n in nodes:
            n.start()
        addrs = ",".join("127.0.0.1:%d" % n.port for n in nodes)
        test_protocol(nodes[0].port)
        print("protocol checks ok")

        keys = {"key%d" % i: "val%d" % i for i in range(N_KEYS)}
        assert kvcli(addrs, ["set %s %s" % kv for kv in keys.items()]) == ["OK"] * N_KEYS
        held = [pipeline(n.port, [("GET", k) for k in keys]) for n in nodes]
        for i, (k, v) in enumerate(keys.items()):  # R=2: every key on exactly 2 of 3 nodes
            assert sum(h[i] == v for h in held) == 2, k
        victim = nodes[1]
        owned = {k: v for (k, v), got in zip(keys.items(), held[1]) if got == v}
        assert call(victim.port, "DBSIZE") == len(owned)
        print("wrote %d keys; victim node holds %d" % (N_KEYS, len(owned)))

        victim.proc.kill()  # SIGKILL: no shutdown hook runs
        victim.proc.wait()
        assert kvcli(addrs, ["get " + k for k in keys]) == list(keys.values())
        assert kvcli(addrs, ["set outage yes", "get outage"]) == ["OK", "yes"]
        print("all keys readable via failover with one node killed")

        victim.start()
        assert call(victim.port, "DBSIZE") == len(owned)
        assert pipeline(victim.port, [("GET", k) for k in owned]) == list(owned.values())
        assert kvcli(addrs, ["set back again", "get back", "get key7"]) == ["OK", "again", "val7"]
        print("restarted node recovered %d keys from its AOF" % len(owned))

        r = subprocess.run([os.path.join(BIN, "kvbench"), "--nodes", addrs, "--threads", "4",
                            "--keys", "1000", "--duration", "1"],
                           capture_output=True, text=True, timeout=60)
        assert r.returncode == 0 and "ops/s" in r.stdout, r.stdout + r.stderr
        print(r.stdout.strip())
    finally:
        for n in nodes:
            n.stop()
    for n in nodes:
        assert n.proc.returncode == 0, "kvserver exit code %s, see %s" % (n.proc.returncode, n.log)
        with open(n.log) as f:
            log = f.read()
        assert "Sanitizer" not in log and "runtime error" not in log, "sanitizer report, see " + n.log
    print("integration test: PASS")


if __name__ == "__main__":
    main()
