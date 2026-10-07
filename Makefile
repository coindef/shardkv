# `make` builds into build/, `make test` runs the unit and integration tests,
# `make tsan` rebuilds everything with ThreadSanitizer into build-tsan/ and
# runs the same tests there.
CXX      := clang++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
OUT      := build
HDRS     := $(wildcard src/*.h)
BINS     := $(addprefix $(OUT)/,kvserver kvcli kvbench unit_test)

.PHONY: all test tsan clean

all: $(BINS)

$(OUT)/%: src/%.cpp $(HDRS)
	@mkdir -p $(OUT)
	$(CXX) $(CXXFLAGS) -o $@ $<

$(OUT)/unit_test: tests/unit_test.cpp $(HDRS)
	@mkdir -p $(OUT)
	$(CXX) $(CXXFLAGS) -Isrc -o $@ $<

test: all
	./$(OUT)/unit_test
	python3 tests/integration_test.py $(OUT)

tsan:
	TSAN_OPTIONS=halt_on_error=1 $(MAKE) test OUT=build-tsan \
		CXXFLAGS="-std=c++17 -O1 -g -Wall -Wextra -pthread -fsanitize=thread"

clean:
	rm -rf build build-tsan
