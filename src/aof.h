#pragma once
// Append-only file. Each mutation is one record:
//   [u32 payload length][u32 crc32(payload)][payload = command as a RESP array]
// (integers in native byte order). Records are write()n immediately, so they
// survive a process crash; a background thread fsyncs every fsync_ms, so a
// power loss loses at most that window. This is group commit: one fsync makes
// every append since the previous fsync durable.
#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "resp.h"

inline uint32_t crc32(const char* p, size_t n) {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int k = 0; k < 8; k++) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
      t[i] = c;
    }
    return t;
  }();
  uint32_t c = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; i++) c = table[(c ^ uint8_t(p[i])) & 0xFF] ^ (c >> 8);
  return c ^ 0xFFFFFFFFu;
}

class Aof {
 public:
  Aof(const std::string& path, int fsync_ms) : fsync_ms_(fsync_ms) {
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::runtime_error("open " + path + ": " + strerror(errno));
    flusher_ = std::thread([this] { flush_loop(); });
  }
  ~Aof() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stop_ = true;
    }
    cv_.notify_one();
    flusher_.join();
    ::fsync(fd_);
    ::close(fd_);
  }
  Aof(const Aof&) = delete;
  Aof& operator=(const Aof&) = delete;

  // Feeds every intact record to apply(). Stops at the first torn or corrupt
  // record (a crash mid-write) and truncates the file there, so new appends
  // land right after valid data. Returns the number of records applied.
  size_t replay(const std::function<void(std::vector<std::string>&)>& apply) {
    std::string data;  // ponytail: whole file in memory; stream it if AOFs outgrow RAM
    char buf[1 << 16];
    for (ssize_t n; (n = ::pread(fd_, buf, sizeof buf, data.size())) > 0;) data.append(buf, n);

    size_t pos = 0, count = 0, used = 0;
    std::vector<std::string> args;
    while (data.size() - pos >= 8) {
      uint32_t len, crc;
      memcpy(&len, &data[pos], 4);
      memcpy(&crc, &data[pos + 4], 4);
      const char* payload = data.data() + pos + 8;
      if (len > data.size() - pos - 8 || crc32(payload, len) != crc) break;
      if (resp::parse_request(payload, len, args, used) != resp::kOk || used != len) break;
      apply(args);
      count++;
      pos += 8 + len;
    }
    if (pos < data.size()) {
      fprintf(stderr, "aof: dropping %zu bytes of torn/corrupt tail\n", data.size() - pos);
      if (::ftruncate(fd_, pos) != 0) throw std::runtime_error("aof truncate failed");
    }
    return count;
  }

  void append(const std::vector<std::string>& cmd) {
    std::string payload = resp::encode(cmd);
    uint32_t hdr[2] = {uint32_t(payload.size()), crc32(payload.data(), payload.size())};
    std::string rec(reinterpret_cast<const char*>(hdr), sizeof hdr);
    rec += payload;
    std::lock_guard<std::mutex> lk(mu_);
    if (!resp::write_all(fd_, rec.data(), rec.size())) {
      perror("aof write");  // fail-stop: never keep acking writes we cannot log
      std::abort();
    }
    dirty_ = true;
  }

  void sync() { ::fsync(fd_); }

 private:
  void flush_loop() {
    std::unique_lock<std::mutex> lk(mu_);
    while (!stop_) {
      cv_.wait_for(lk, std::chrono::milliseconds(fsync_ms_));
      if (!dirty_) continue;
      dirty_ = false;
      lk.unlock();  // appends keep going while we fsync
      ::fsync(fd_);
      lk.lock();
    }
  }

  int fd_;
  int fsync_ms_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool dirty_ = false, stop_ = false;
  std::thread flusher_;
};
