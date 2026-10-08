#!/usr/bin/env python3
"""End-to-end test: raw protocol edge cases, a 3-node cluster written through
kvcli, SIGKILL of one node with failover reads and writes, then restart + AOF
recovery + read repair of what the node missed, deletes included.

Usage: integration_test.py [BIN_DIR]   (default: build)
"""
import os
import shutil
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
    """'+OK' -> 'OK', '-ERR x' -> '-ERR x', ':5' -> 5, '$..' -> str or None, '*..' -> list."""
    line = f.readline()
    assert line.endswith(b"\r\n"), "connection closed or bad reply: %r" % line
    kind, body = line[:1], line[1:-2].decode()
    if kind == b"*":
        return [read_reply(f) for _ in range(int(body))]
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


def kvcli(nodes, lines, *flags):
    r = subprocess.run([os.path.join(BIN, "kvcli"), "--nodes", nodes, *flags],
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
    # last writer wins: a write that loses says -STALE with the winning version
    # (the same write again is a no-op), and a tombstone reads as a miss
    assert pipeline(port, [("SET", "w", "new", "VER", "20"), ("SET", "w", "old", "px", "9000", "VER", "10"),
                           ("SET", "w", "new", "VER", "20"), ("SET", "w", "other", "VER", "20"),
                           ("GETV", "w"), ("DELV", "w", "30"), ("DELV", "w", "25"), ("GET", "w"), ("GETV", "w"),
                           ("SET", "w", "x", "VER", "0"), ("DELV", "w", "-1")]) == \
        ["OK", "-STALE 20", "OK", "-STALE 20", [20, "new", 0], "OK", "-STALE 30", None, [30, None, 0],
         "-ERR syntax error or invalid expire time", "-ERR invalid version"]
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
        upd, gone, last = list(owned)[:10], list(owned)[10:20], list(owned)[-1]
        assert kvcli(addrs, ["set %s new-%s" % (k, k) for k in upd] + ["del " + k for k in gone]) == ["OK"] * 20
        ttl = list(owned)[21]  # the victim keeps the old value; the TTL'd overwrite must not let it back
        assert kvcli(addrs, ["set %s short 200" % ttl]) == ["OK"]
        # W=2 can't be met with one of the key's two replicas down
        assert kvcli(addrs, ["set %s w2" % list(owned)[20]], "--write-quorum", "2")[0].startswith("ERR")

        victim.start()
        assert call(victim.port, "DBSIZE") == len(owned)
        assert pipeline(victim.port, [("GET", k) for k in owned]) == list(owned.values())  # stale
        assert kvcli(addrs, ["set back again", "get back", "get " + last]) == ["OK", "again", owned[last]]
        print("restarted node recovered %d keys from its AOF" % len(owned))
        # Reads ask both replicas, return the newest version and repair the victim.
        assert kvcli(addrs, ["get " + k for k in upd + gone]) == ["new-" + k for k in upd] + ["(nil)"] * 10
        assert pipeline(victim.port, [("GET", k) for k in upd + gone]) == ["new-" + k for k in upd] + [None] * 10
        for n in nodes:  # the tombstones keep the deleted keys from coming back
            assert pipeline(n.port, [("GET", k) for k in gone]) == [None] * 10
        assert call(victim.port, "GETV", gone[0])[0] > 0
        print("read repair brought the victim up to date: %d updates, %d deletes" % (len(upd), len(gone)))
        time.sleep(0.3)  # the TTL has run out; its version must still beat the victim's old value
        assert kvcli(addrs, ["get " + ttl]) == ["(nil)"]
        for n in nodes:
            assert call(n.port, "GET", ttl) is None
        print("an expired overwrite kept the victim's older value from coming back")

        # Another client's clock is a minute ahead: its write holds a higher
        # version on every replica. A later write must move past it, not get
        # +OK and be dropped.
        ahead = (int(time.time() * 1000) + 60000) << 16
        for n in nodes:
            assert call(n.port, "SET", "lww", "theirs", "VER", ahead) == "OK"
        assert kvcli(addrs, ["set lww mine", "get lww"], "--write-quorum", "2") == ["OK", "mine"]
        held = [call(n.port, "GETV", "lww") for n in nodes]
        assert sum(h[0] > ahead and h[1] == "mine" for h in held) == 2, held  # both replicas
        print("a write after a newer-stamped one went past it on both replicas")

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
    shutil.rmtree(tmp)  # kept on failure: CI prints the node logs from it
    print("integration test: PASS")


if __name__ == "__main__":
    main()
