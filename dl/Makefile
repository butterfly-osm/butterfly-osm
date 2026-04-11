# Makefile for butterfly-dl C library builds
#
# Run from tools/butterfly-dl/ (this file's location).
# Cargo discovers the workspace root automatically, so target/ paths
# resolve to ../../target/ relative to this directory.

CARGO := cargo
PREFIX ?= /usr/local
LIBDIR := $(PREFIX)/lib
INCLUDEDIR := $(PREFIX)/include
PKGCONFIGDIR := $(LIBDIR)/pkgconfig

.PHONY: all rust-lib c-lib static dynamic install install-headers install-pkgconfig clean help

all: rust-lib c-lib

rust-lib:
	$(CARGO) build --release -p butterfly-dl

c-lib: static dynamic

static:
	$(CARGO) build --release -p butterfly-dl --features c-bindings
	@echo "Static library: target/release/libbutterfly_dl.a"

dynamic:
	$(CARGO) build --release -p butterfly-dl --features c-bindings
	@echo "Dynamic library: target/release/libbutterfly_dl.so"

# NOTE: Installing to /usr/local requires root. Run: sudo make install
# Or install to a user directory: make install PREFIX=$HOME/.local
install: install-libs install-headers install-pkgconfig

install-libs: c-lib
	@mkdir -p $(LIBDIR) 2>/dev/null || { echo "Cannot create $(LIBDIR). Try: sudo make install"; exit 1; }
	@cp target/release/libbutterfly_dl.a $(LIBDIR)/
	@cp target/release/libbutterfly_dl.so $(LIBDIR)/

install-headers:
	@mkdir -p $(INCLUDEDIR)
	@cp include/butterfly.h $(INCLUDEDIR)/

install-pkgconfig:
	@mkdir -p $(PKGCONFIGDIR)
	@sed -e 's|@PREFIX@|$(PREFIX)|g' \
	     -e "s|@VERSION@|$$(cat VERSION)|g" \
	     butterfly-dl.pc.in > $(PKGCONFIGDIR)/butterfly-dl.pc

test:
	$(CARGO) test -p butterfly-dl
	$(CARGO) test -p butterfly-dl --features c-bindings

clean:
	$(CARGO) clean
	@rm -f $(PKGCONFIGDIR)/butterfly-dl.pc

help:
	@echo "Butterfly-dl Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build all libraries (Rust + C)"
	@echo "  rust-lib     - Build Rust library (.rlib)"
	@echo "  c-lib        - Build C libraries (static + dynamic)"
	@echo "  static       - Build static C library (.a)"
	@echo "  dynamic      - Build dynamic C library (.so)"
	@echo "  install      - Install libraries and headers system-wide"
	@echo "  test         - Run all tests"
	@echo "  clean        - Clean build artifacts"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX       - Installation prefix (default: /usr/local)"
