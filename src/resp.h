#pragma once
// Minimal RESP2 codec (the Redis wire protocol). Requests are arrays of bulk
// strings; replies are simple strings, errors, integers or bulk strings.
// Parsers never consume partial input: on a short buffer they return
// kIncomplete and the caller retries once more bytes have arrived.
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace resp {

enum Status { kOk, kIncomplete, kError };
constexpr size_t kMaxArgs = 1024;
constexpr size_t kMaxBulk = 16 << 20;  // largest key/value accepted
constexpr size_t kMaxLine = 1024;      // longest header or simple-string line

struct Reply {
  char type = 0;  // '+', '-', ':' or '$'
  std::string str;
  long long num = 0;
  bool nil = false;
};

struct Cursor {
  const char* p;
  size_t n, i;

  Status expect(char c) {
    if (i >= n) return kIncomplete;
    return p[i++] == c ? kOk : kError;
  }
  Status line(std::string& out) {
    const char* cr = static_cast<const char*>(memchr(p + i, '\r', n - i));
    if (!cr) return n - i > kMaxLine ? kError : kIncomplete;
    size_t e = cr - p;
    if (e + 1 >= n) return kIncomplete;
    if (p[e + 1] != '\n') return kError;
    out.assign(p + i, e - i);
    i = e + 2;
    return kOk;
  }
  Status integer(long long& v) {
    std::string s;
    if (Status st = line(s); st != kOk) return st;
    // digits with an optional '-': strtoll alone would also take "+1", " 1" or "1\0junk"
    if (s.empty() || s.size() > 18 || s.find_first_not_of("0123456789", s[0] == '-') != s.npos)
      return kError;
    char* end = nullptr;
    v = strtoll(s.c_str(), &end, 10);
    return *end ? kError : kOk;
  }
  Status bulk(std::string& out, bool& nil) {
    long long len;
    if (Status st = integer(len); st != kOk) return st;
    nil = len == -1;
    if (nil) return kOk;
    if (len < 0 || size_t(len) > kMaxBulk) return kError;
    if (n - i < size_t(len) + 2) return kIncomplete;
    if (p[i + len] != '\r' || p[i + len + 1] != '\n') return kError;
    out.assign(p + i, len);
    i += len + 2;
    return kOk;
  }
};

// Parses one command. On kOk, `args` holds it and `used` is its size in bytes.
inline Status parse_request(const char* buf, size_t len, std::vector<std::string>& args,
                            size_t& used) {
  Cursor c{buf, len, 0};
  long long n = 0;
  Status st = c.expect('*');
  if (st == kOk) st = c.integer(n);
  if (st != kOk) return st;
  if (n < 1 || size_t(n) > kMaxArgs) return kError;
  args.resize(n);
  for (auto& a : args) {
    bool nil = false;
    if ((st = c.expect('$')) != kOk || (st = c.bulk(a, nil)) != kOk) return st;
    if (nil) return kError;
  }
  used = c.i;
  return kOk;
}

inline Status parse_reply(const char* buf, size_t len, Reply& r, size_t& used) {
  if (len == 0) return kIncomplete;
  Cursor c{buf, len, 1};
  r = Reply{};
  r.type = buf[0];
  Status st = kError;
  if (r.type == '+' || r.type == '-') st = c.line(r.str);
  else if (r.type == ':') st = c.integer(r.num);
  else if (r.type == '$') st = c.bulk(r.str, r.nil);
  if (st == kOk) used = c.i;
  return st;
}

inline void append_bulk(std::string& out, const std::string& s) {
  out += '$';
  out += std::to_string(s.size());
  out += "\r\n";
  out += s;
  out += "\r\n";
}

inline std::string encode(const std::vector<std::string>& args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& a : args) append_bulk(out, a);
  return out;
}

// Writes all bytes, retrying short writes and EINTR. False on any error.
inline bool write_all(int fd, const char* buf, size_t len) {
  while (len > 0) {
    ssize_t n = ::write(fd, buf, len);
    if (n < 0 && errno == EINTR) continue;
    if (n <= 0) return false;
    buf += n;
    len -= n;
  }
  return true;
}

}  // namespace resp
