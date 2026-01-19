# ──────────────────────────────
#  Simple Makefile
#  Usage
#    make            → build in Debug mode (-g)
#    make release    → build in Release mode (-O3)
#    make run        → run the program (all the test cases)
#    make test 6     → run a specific test (e.g., test 6)
#    make clean      → delete build artifacts
#    make lldb
# ──────────────────────────────

# ---- 1. Configurable variables ----
CXX      ?= g++
NAME     ?= buzzdb_lab1            # base executable name (no extension)
SRC      := buzzdb_lab1.cpp        # add more .cpp files here if needed
BUILD    ?= debug                  # default build type: debug | release

# ---- 2. Flags per build type ----
DEBUG_FLAGS   := -std=c++17 -Wall -Wextra -g
RELEASE_FLAGS := -std=c++17 -Wall -Wextra -O3

ifeq ($(BUILD),release)
  CXXFLAGS := $(RELEASE_FLAGS)
  OUTDIR   := build/release
else
  CXXFLAGS := $(DEBUG_FLAGS)
  OUTDIR   := build/debug
endif

TARGET := $(OUTDIR)/$(NAME)
OBJS   := $(patsubst %.cpp,$(OUTDIR)/%.o,$(SRC))

# ---- 3. Default goal ----
.DEFAULT_GOAL := all
all: $(TARGET)   # “make” builds the executable

# ---- 4. Build rules ----
$(TARGET): $(OBJS) | $(OUTDIR)
	$(CXX) $(OBJS) -o $@

# pattern rule: any .cpp → .o inside the build dir
$(OUTDIR)/%.o: %.cpp | $(OUTDIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(OUTDIR):
	@mkdir -p $@

# ---- 5. Convenience shortcuts ----
# Grab every extra word after 'test' and treat it as a program argument.
TESTARGS := $(filter-out test,$(MAKECMDGOALS))

.PHONY: test
test: $(TARGET)
	$(TARGET) $(TESTARGS)

# Dummy pattern so extra args (e.g., 5) don't trigger errors
%::                      # double-colon = independent rule
	@true

# ─── 6. Debug with LLDB ---
.PHONY: lldb
lldb: $(TARGET)
	lldb $(TARGET) -- $(ARGS)

# Example:

.PHONY: run release clean
run:     $(TARGET) ; $<
release: ; $(MAKE) BUILD=release
clean:   ; rm -rf build