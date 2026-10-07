#pragma once
// Append-only file. Each mutation is one record:
//   [u32 payload length][u32 crc32(payload)][payload = command as a RESP array]
// (integers in native byte order). append() returns once its records are
// write()n, so they survive a process crash; appends that arrive together share
// one write(). A background thread fsyncs every fsync_ms, so a power loss loses
// at most that window.
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
    sync();
    ::close(fd_);
  }
  Aof(const Aof&) = delete;
  Aof& operator=(const Aof&) = delete;

  // Feeds every intact record to apply(). A bad record with nothing intact
  // after it is what a crash mid-write leaves behind: the file is truncated
  // there so new appends follow valid data. A bad record with intact records
  // after it is corruption, and replay throws rather than drop them.
  // Returns the number of records applied.
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
      if (intact_record_after(data, pos + 1))
        throw std::runtime_error("aof: corrupt record at byte " + std::to_string(pos) +
                                 " with intact records after it; refusing to truncate");
      fprintf(stderr, "aof: dropping %zu bytes of torn tail\n", data.size() - pos);
      if (::ftruncate(fd_, pos) != 0) throw std::runtime_error("aof truncate failed");
      sync();
    }
    return count;
  }

  // One command encoded as a record, ready for append().
  static std::string record(const std::vector<std::string>& cmd) {
    std::string payload = resp::encode(cmd);
    uint32_t hdr[2] = {uint32_t(payload.size()), crc32(payload.data(), payload.size())};
    return std::string(reinterpret_cast<const char*>(hdr), sizeof hdr) + payload;
  }

  // Appends encoded records and returns once they are write()n. Records that
  // arrive while a write is in flight queue up, and the next caller to run
  // writes the whole queue with one write(), so concurrent writers share
  // syscalls instead of taking turns on them.
  void append(const std::string& records) {
    std::unique_lock<std::mutex> lk(mu_);
    queue_ += records;
    const uint64_t mine = ++queued_;
    while (written_ < mine) {
      if (writing_) {
        written_cv_.wait(lk);
        continue;
      }
      writing_ = true;
      std::string batch;
      batch.swap(queue_);
      const uint64_t upto = queued_;
      lk.unlock();
      if (!resp::write_all(fd_, batch.data(), batch.size())) {
        perror("aof write");  // fail-stop: never keep acking writes we cannot log
        std::abort();
      }
      lk.lock();
      writing_ = false;
      written_ = upto;
      dirty_ = true;
      written_cv_.notify_all();
    }
  }

  // Plain fsync() on macOS leaves data in the drive's cache; F_FULLFSYNC
  // flushes it. A failed sync may have lost acked writes, so fail-stop.
  void sync() {
#ifdef __APPLE__
    if (::fcntl(fd_, F_FULLFSYNC) == 0) return;  // not every filesystem supports it
#endif
    if (::fsync(fd_) != 0) {
      perror("aof fsync");
      std::abort();
    }
  }

 private:
  // Whether a whole record with a matching CRC starts anywhere in data[from..].
  static bool intact_record_after(const std::string& data, size_t from) {
    for (size_t q = from; q + 8 <= data.size(); q++) {
      uint32_t len, crc;
      memcpy(&len, &data[q], 4);
      memcpy(&crc, &data[q + 4], 4);
      if (len > 0 && len <= data.size() - q - 8 && crc32(&data[q + 8], len) == crc) return true;
    }
    return false;
  }

  void flush_loop() {
    std::unique_lock<std::mutex> lk(mu_);
    while (!stop_) {
      cv_.wait_for(lk, std::chrono::milliseconds(fsync_ms_));
      if (!dirty_) continue;
      dirty_ = false;
      lk.unlock();  // appends keep going while we fsync
      sync();
      lk.lock();
    }
  }

  int fd_;
  int fsync_ms_;
  std::mutex mu_;
  std::condition_variable cv_, written_cv_;
  std::string queue_;               // records waiting for the next write()
  uint64_t queued_ = 0, written_ = 0;  // append() calls queued / written so far
  bool writing_ = false, dirty_ = false, stop_ = false;
  std::thread flusher_;
};
