// kvcli: tiny client for a ShardKV cluster.
//   kvcli [--nodes h:p,h:p,...] [--replicas R] get KEY | set KEY VALUE [TTL_MS] | del KEY
// With no command it reads one command per line from stdin.
#include <cctype>
#include <iostream>
#include <sstream>

#include "client.h"

static int run(shardkv::Cluster& c, const std::vector<std::string>& a) {
  std::string op = a[0];
  for (char& ch : op) ch = tolower(static_cast<unsigned char>(ch));
  try {
    if (op == "get" && a.size() == 2) {
      auto v = c.get(a[1]);
      std::cout << (v ? *v : "(nil)") << std::endl;
      return 0;
    }
    bool ok;
    char* end = nullptr;
    const long long ttl = a.size() == 4 ? strtoll(a[3].c_str(), &end, 10) : 0;
    if (op == "set" && (a.size() == 3 || (a.size() == 4 && ttl > 0 && !*end)))
      ok = c.set(a[1], a[2], ttl);
    else if (op == "del" && a.size() == 2)
      ok = c.del(a[1]);
    else {
      std::cout << "ERR usage: get KEY | set KEY VALUE [TTL_MS] | del KEY" << std::endl;
      return 1;
    }
    std::cout << (ok ? "OK" : "ERR no replica acknowledged") << std::endl;
    return ok ? 0 : 1;
  } catch (const std::exception& e) {
    std::cout << "ERR " << e.what() << std::endl;
    return 1;
  }
}

int main(int argc, char** argv) {
  std::string nodes = "127.0.0.1:6380";
  int replicas = 2;
  std::vector<std::string> cmd;
  for (int i = 1; i < argc; i++) {
    const std::string a = argv[i];
    if (a == "--nodes" && i + 1 < argc) nodes = argv[++i];
    else if (a == "--replicas" && i + 1 < argc) replicas = atoi(argv[++i]);
    else cmd.push_back(a);
  }
  try {
    shardkv::Cluster c(shardkv::split(nodes, ','), replicas);
    if (!cmd.empty()) return run(c, cmd);
    int rc = 0;
    for (std::string line; std::getline(std::cin, line);) {
      std::istringstream ss(line);
      std::vector<std::string> words;
      for (std::string w; ss >> w;) words.push_back(w);
      if (!words.empty()) rc |= run(c, words);
    }
    return rc;
  } catch (const std::exception& e) {
    std::cerr << "kvcli: " << e.what() << std::endl;
    return 2;
  }
}
