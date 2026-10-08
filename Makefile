# `make` builds into build/, `make test` runs the unit and integration tests,
# `make tsan` rebuilds everything with ThreadSanitizer into build-tsan/ and
# runs the same tests there, `make asan` does the same with AddressSanitizer +
# UndefinedBehaviorSanitizer into build-asan/. `make bench` runs tests/bench.py.
CXX      := clang++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
OUT      := build
HDRS     := $(wildcard src/*.h)
BINS     := $(addprefix $(OUT)/,kvserver kvcli kvbench unit_test)

.PHONY: all test tsan asan bench clean

all: $(BINS)

$(OUT)/%: src/%.cpp $(HDRS)
	@mkdir -p $(OUT)
	$(CXX) $(CXXFLAGS) -o $@ $<

$(OUT)/unit_test: tests/unit_test.cpp $(HDRS)
	@mkdir -p $(OUT)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $<

test: all
	$(OUT)/unit_test
	python3 tests/integration_test.py $(OUT)

tsan:
	TSAN_OPTIONS=halt_on_error=1 $(MAKE) test OUT=build-tsan \
		CXXFLAGS="-std=c++17 -O1 -g -Wall -Wextra -pthread -fsanitize=thread"

asan:
	$(MAKE) test OUT=build-asan \
		CXXFLAGS="-std=c++17 -O1 -g -fno-omit-frame-pointer -Wall -Wextra -pthread -fsanitize=address,undefined -fno-sanitize-recover=all"

bench: all
	python3 tests/bench.py $(OUT)

clean:
	rm -rf build build-tsan build-asan
