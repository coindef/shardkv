#pragma once
// Header-only ShardKV cluster client.
// Keys are placed on a consistent-hash ring with virtual nodes; each key lives
// on the first R distinct nodes clockwise from its hash (its preference list).
//   writes: sent to all R replicas, succeed if at least one acks (W=1)
//   reads:  the first replica that answers wins; on a connect/IO error or
//           timeout the next replica in the list is tried (failover)
// A Cluster keeps one connection per node and is NOT thread-safe: use one
// Cluster per thread.
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "resp.h"

namespace shardkv {

// FNV-1a plus the murmur3 finalizer, so near-identical strings ("n1#1",
// "n1#2") still land far apart on the ring.
inline uint64_t hash64(const std::string& s) {
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : s) h = (h ^ c) * 1099511628211ULL;
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  return h ^ (h >> 33);
}

inline std::vector<std::string> split(const std::string& s, char sep) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  for (std::string part; std::getline(ss, part, sep);)
    if (!part.empty()) out.push_back(part);
  return out;
}

// One blocking TCP connection with connect/read/write timeouts.
class Conn {
 public:
  Conn() = default;
  Conn(const Conn&) = delete;
  Conn& operator=(const Conn&) = delete;
  ~Conn() { close(); }

  bool connected() const { return fd_ >= 0; }
  void close() {
    if (fd_ >= 0) ::close(fd_);
    fd_ = -1;
    buf_.clear();
  }

  bool connect(const std::string& host, const std::string& port, int timeout_ms) {
    close();
    addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0) return false;
    fd_ = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    bool ok = fd_ >= 0 && connect_with_timeout(res->ai_addr, res->ai_addrlen, timeout_ms);
    freeaddrinfo(res);
    if (!ok) {
      close();
      return false;
    }
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    int one = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    return true;
  }

  // Sends one command and reads one reply. Any IO error, timeout or protocol
  // error closes the connection and returns false.
  bool call(const std::vector<std::string>& cmd, resp::Reply& r) {
    const std::string req = resp::encode(cmd);
    if (resp::write_all(fd_, req.data(), req.size())) {
      for (;;) {
        size_t used = 0;
        resp::Status st = resp::parse_reply(buf_.data(), buf_.size(), r, used);
        if (st == resp::kOk) {
          buf_.erase(0, used);
          return true;
        }
        if (st == resp::kError) break;
        char tmp[16 << 10];
        ssize_t n = ::read(fd_, tmp, sizeof tmp);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) break;  // EOF, error, or SO_RCVTIMEO timeout
        buf_.append(tmp, n);
      }
    }
    close();
    return false;
  }

 private:
  bool connect_with_timeout(const sockaddr* addr, socklen_t len, int timeout_ms) {
    int flags = fcntl(fd_, F_GETFL);
    fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    int rc = ::connect(fd_, addr, len);
    if (rc < 0 && errno == EINPROGRESS) {
      pollfd p{fd_, POLLOUT, 0};
      int err = 0;
      socklen_t elen = sizeof err;
      rc = poll(&p, 1, timeout_ms) == 1 &&
                   getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &elen) == 0 && err == 0
               ? 0
               : -1;
    }
    fcntl(fd_, F_SETFL, flags);
    return rc == 0;
  }

  int fd_ = -1;
  std::string buf_;
};

class Cluster {
 public:
  // addrs: "host:port" per node.
  explicit Cluster(const std::vector<std::string>& addrs, int replicas = 2, int vnodes = 100,
                   int timeout_ms = 500)
      : nodes_(addrs.size()), timeout_ms_(timeout_ms) {
    if (addrs.empty()) throw std::invalid_argument("no nodes given");
    replicas_ = std::min<size_t>(std::max(replicas, 1), addrs.size());
    signal(SIGPIPE, SIG_IGN);  // a dead peer should be an EPIPE error, not kill us
    for (size_t i = 0; i < addrs.size(); i++) {
      size_t colon = addrs[i].rfind(':');
      if (colon == std::string::npos) throw std::invalid_argument("expected host:port: " + addrs[i]);
      if (std::find(addrs.begin(), addrs.begin() + i, addrs[i]) != addrs.begin() + i)
        throw std::invalid_argument("node listed twice: " + addrs[i]);
      nodes_[i].host = addrs[i].substr(0, colon);
      nodes_[i].port = addrs[i].substr(colon + 1);
      for (int v = 0; v < std::max(vnodes, 1); v++)
        ring_.emplace_back(hash64(addrs[i] + "#" + std::to_string(v)), i);
    }
    std::sort(ring_.begin(), ring_.end());
  }

  // Indexes of the R distinct nodes responsible for key, in failover order.
  std::vector<size_t> preference_list(const std::string& key) const {
    std::vector<size_t> out;
    size_t start = std::lower_bound(ring_.begin(), ring_.end(), std::make_pair(hash64(key), size_t(0))) -
                   ring_.begin();
    for (size_t k = 0; k < ring_.size() && out.size() < replicas_; k++) {
      size_t node = ring_[(start + k) % ring_.size()].second;
      if (std::find(out.begin(), out.end(), node) == out.end()) out.push_back(node);
    }
    return out;
  }

  // The value, or nullopt if the key does not exist. Throws if no replica
  // answers. Every call throws std::invalid_argument for a key or value over
  // the server's 16 MB limit, which the server would answer by hanging up.
  std::optional<std::string> get(const std::string& key) {
    resp::Reply r;
    for (size_t n : preference_list(key))
      if (call(n, {"GET", key}, r) && r.type == '$')
        return r.nil ? std::nullopt : std::optional<std::string>(std::move(r.str));
    throw std::runtime_error("GET " + key + ": no replica reachable");
  }

  // True if at least one replica acked. ttl_ms <= 0 means no expiry.
  bool set(const std::string& key, const std::string& val, int64_t ttl_ms = 0) {
    std::vector<std::string> cmd{"SET", key, val};
    if (ttl_ms > 0) cmd.insert(cmd.end(), {"PX", std::to_string(ttl_ms)});
    return write_replicas(key, cmd);
  }

  bool del(const std::string& key) { return write_replicas(key, {"DEL", key}); }

 private:
  struct Node {
    std::string host, port;
    Conn conn;
    std::chrono::steady_clock::time_point retry_at;  // skip the node until then
  };

  // ponytail: replicas are written one after another; pipeline the fan-out if
  // write latency matters.
  bool write_replicas(const std::string& key, const std::vector<std::string>& cmd) {
    int acks = 0;
    resp::Reply r;
    for (size_t n : preference_list(key)) acks += call(n, cmd, r) && r.type != '-';
    return acks > 0;
  }

  bool call(size_t i, const std::vector<std::string>& cmd, resp::Reply& r) {
    for (const auto& a : cmd)
      if (a.size() > resp::kMaxBulk) throw std::invalid_argument("key or value larger than 16 MB");
    Node& n = nodes_[i];
    if (std::chrono::steady_clock::now() < n.retry_at) return false;  // failed recently: skip it
    // A pooled connection can be stale (e.g. the node restarted), so a failure
    // on one gets a single retry over a fresh connection.
    for (int attempt = 0; attempt < 2; attempt++) {
      bool fresh = !n.conn.connected();
      if (fresh && !n.conn.connect(n.host, n.port, timeout_ms_)) break;
      if (n.conn.call(cmd, r)) return true;
      if (fresh) break;
    }
    // ponytail: fixed 1 s backoff, counted from the failure, not from the start
    // of an attempt that may itself have spent two timeouts.
    n.retry_at = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    return false;
  }

  std::vector<Node> nodes_;
  std::vector<std::pair<uint64_t, size_t>> ring_;  // (vnode hash, node index), sorted
  size_t replicas_;
  int timeout_ms_;
};

}  // namespace shardkv
