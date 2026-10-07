// kvserver: one cache node. Speaks a RESP2 subset, so redis-cli can talk to it:
//   PING | GET k | SET k v [PX ms | PXAT unix-ms] | DEL k [k ...] | DBSIZE | INFO
// One thread per connection; storage is the sharded LRU in store.h with an
// optional append-only log for durability.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>

#include <csignal>
#include <cstdio>
#include <memory>
#include <thread>

#include "store.h"

static volatile sig_atomic_t g_stop = 0;
constexpr size_t kMaxRequest = 64 << 20;  // caps bytes buffered for one unfinished command

static std::string upper(std::string s) {
  for (char& c : s) c = toupper(static_cast<unsigned char>(c));
  return s;
}

// Strict positive integer: digits only, no sign, no overflow.
static bool parse_ms(const std::string& s, int64_t& out) {
  if (s.empty() || s.size() > 15 || s.find_first_not_of("0123456789") != std::string::npos)
    return false;
  out = std::stoll(s);
  return out > 0;
}

static void execute(Store& store, std::vector<std::string>& a, std::string& out) {
  const std::string cmd = upper(a[0]);
  const size_t n = a.size();
  if (cmd == "PING" && n == 1) {
    out += "+PONG\r\n";
  } else if (cmd == "GET" && n == 2) {
    std::string v;
    if (store.get(a[1], &v)) resp::append_bulk(out, v);
    else out += "$-1\r\n";
  } else if (cmd == "SET" && (n == 3 || n == 5)) {
    int64_t at = 0, ms = 0;
    if (n == 5) {
      const std::string opt = upper(a[3]);
      if ((opt != "PX" && opt != "PXAT") || !parse_ms(a[4], ms)) {
        out += "-ERR syntax error or invalid expire time\r\n";
        return;
      }
      at = opt == "PX" ? now_ms() + ms : ms;
    }
    store.set(a[1], std::move(a[2]), at);
    out += "+OK\r\n";
  } else if (cmd == "DEL" && n >= 2) {
    int64_t removed = 0;
    for (size_t i = 1; i < n; i++) removed += store.del(a[i]);
    out += ":" + std::to_string(removed) + "\r\n";
  } else if (cmd == "DBSIZE" && n == 1) {
    out += ":" + std::to_string(store.stats().keys) + "\r\n";
  } else if (cmd == "INFO" || cmd == "STATS") {
    Store::Stats s = store.stats();
    char buf[256];
    snprintf(buf, sizeof buf,
             "keys:%llu\r\nused_bytes:%llu\r\nhits:%llu\r\nmisses:%llu\r\n"
             "evictions:%llu\r\nexpired:%llu\r\n",
             (unsigned long long)s.keys, (unsigned long long)s.bytes, (unsigned long long)s.hits,
             (unsigned long long)s.misses, (unsigned long long)s.evictions,
             (unsigned long long)s.expired);
    resp::append_bulk(out, buf);
  } else {
    out += "-ERR unknown command or wrong number of arguments\r\n";
  }
}

static void serve(int fd, Store& store) {
  std::string in, out;
  std::vector<std::string> args;
  char buf[16 << 10];
  for (;;) {
    ssize_t r = ::read(fd, buf, sizeof buf);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) break;
    in.append(buf, r);
    // Run every complete command in the buffer (pipelining); a partial command
    // stays buffered until the rest of it arrives.
    size_t pos = 0, used = 0;
    resp::Status st;
    while ((st = resp::parse_request(in.data() + pos, in.size() - pos, args, used)) == resp::kOk) {
      execute(store, args, out);
      pos += used;
    }
    in.erase(0, pos);
    const bool bad = st == resp::kError || in.size() > kMaxRequest;
    if (bad) out += "-ERR Protocol error\r\n";
    if (!out.empty() && !resp::write_all(fd, out.data(), out.size())) break;
    out.clear();
    if (bad) break;  // framing is lost; like Redis, reply and hang up
  }
  ::close(fd);
}

int main(int argc, char** argv) {
  int port = 6380, fsync_ms = 1000, shards = 16, max_mb = 256;
  std::string bind_ip = "127.0.0.1", aof_path;
  bool ok = argc % 2 == 1;
  for (int i = 1; ok && i + 1 < argc; i += 2) {
    const std::string f = argv[i];
    const char* v = argv[i + 1];
    if (f == "--port") port = atoi(v);
    else if (f == "--bind") bind_ip = v;
    else if (f == "--aof") aof_path = v;
    else if (f == "--fsync-ms") fsync_ms = atoi(v);
    else if (f == "--max-mb") max_mb = atoi(v);
    else if (f == "--shards") shards = atoi(v);
    else ok = false;
  }
  if (!ok || port <= 0 || port > 65535 || fsync_ms <= 0 || max_mb <= 0 || shards <= 0) {
    fprintf(stderr,
            "usage: kvserver [--port 6380] [--bind 127.0.0.1] [--aof FILE] [--fsync-ms 1000]\n"
            "                [--max-mb 256] [--shards 16]\n");
    return 2;
  }

  Store store(shards, size_t(max_mb) << 20);
  std::unique_ptr<Aof> aof;
  if (!aof_path.empty()) {
    try {
      aof = std::make_unique<Aof>(aof_path, fsync_ms);
      size_t n = aof->replay([&](std::vector<std::string>& cmd) { store.apply(cmd); });
      fprintf(stderr, "kvserver: replayed %zu AOF records (%llu keys)\n", n,
              (unsigned long long)store.stats().keys);
    } catch (const std::exception& e) {
      fprintf(stderr, "kvserver: %s\n", e.what());
      return 1;
    }
    store.attach_aof(aof.get());
  }

  int one = 1, lfd = socket(AF_INET, SOCK_STREAM, 0);
  setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (inet_pton(AF_INET, bind_ip.c_str(), &addr.sin_addr) != 1 ||
      bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0 || listen(lfd, 512) < 0) {
    perror("kvserver: bind/listen");
    return 1;
  }
  signal(SIGPIPE, SIG_IGN);
  signal(SIGINT, [](int) { g_stop = 1; });
  signal(SIGTERM, [](int) { g_stop = 1; });
  fprintf(stderr, "kvserver: listening on %s:%d\n", bind_ip.c_str(), port);

  std::thread([&store] {  // active expiry; reads also expire keys lazily
    for (;;) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      store.sweep();
    }
  }).detach();

  while (!g_stop) {
    pollfd p{lfd, POLLIN, 0};
    if (poll(&p, 1, 200) <= 0) continue;  // timeout lets us notice g_stop
    int fd = accept(lfd, nullptr, nullptr);
    if (fd < 0) continue;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    // ponytail: thread per connection; switch to an epoll/kqueue loop past ~1k clients
    std::thread(serve, fd, std::ref(store)).detach();
  }
  // Make the log durable, then exit without unwinding: detached connection
  // threads may still be using `store`.
  if (aof) aof->sync();
  fprintf(stderr, "kvserver: shut down\n");
  std::_Exit(0);
}
