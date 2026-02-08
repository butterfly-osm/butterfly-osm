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

# Build targets
.PHONY: all rust-lib c-lib static dynamic install install-headers install-pkgconfig clean help

all: rust-lib c-lib

# Build Rust library (rlib) for Rust projects
rust-lib:
	@echo "🦀 Building Rust library..."
	$(CARGO) build --release -p butterfly-dl

# Build C-compatible libraries (static and dynamic)
c-lib: static dynamic

# Build static library (.a) for C/C++
static:
	@echo "📚 Building static library for C/C++..."
	$(CARGO) build --release -p butterfly-dl --features c-bindings
	@echo "✅ Static library: target/release/libbutterfly_dl.a"

# Build dynamic library (.so/.dylib/.dll) for C/C++
dynamic:
	@echo "🔗 Building dynamic library for C/C++..."
	$(CARGO) build --release -p butterfly-dl --features c-bindings
	@if [ -f target/release/libbutterfly_dl.so ]; then \
		echo "✅ Dynamic library: target/release/libbutterfly_dl.so"; \
	elif [ -f target/release/libbutterfly_dl.dylib ]; then \
		echo "✅ Dynamic library: target/release/libbutterfly_dl.dylib"; \
	elif [ -f target/release/butterfly_dl.dll ]; then \
		echo "✅ Dynamic library: target/release/butterfly_dl.dll"; \
	fi

# Install libraries and headers system-wide
# NOTE: Installing to system directories (e.g., /usr/local) typically requires
# root privileges. Run with: sudo make install
# Or install to a user directory: make install PREFIX=$HOME/.local
install: install-libs install-headers install-pkgconfig
	@echo "✅ Installation complete!"
	@echo "📍 Libraries installed to: $(LIBDIR)"
	@echo "📍 Headers installed to: $(INCLUDEDIR)"
	@echo "📍 pkg-config file: $(PKGCONFIGDIR)/butterfly-dl.pc"

install-libs: c-lib
	@echo "📦 Installing libraries to $(LIBDIR)..."
	@mkdir -p $(LIBDIR) 2>/dev/null || { echo "❌ Cannot create $(LIBDIR). Try: sudo make install"; exit 1; }
	@cp target/release/libbutterfly_dl.a $(LIBDIR)/ 2>/dev/null || true
	@cp target/release/libbutterfly_dl.so $(LIBDIR)/ 2>/dev/null || true
	@cp target/release/libbutterfly_dl.dylib $(LIBDIR)/ 2>/dev/null || true
	@cp target/release/butterfly_dl.dll $(LIBDIR)/ 2>/dev/null || true

install-headers:
	@echo "📄 Installing headers to $(INCLUDEDIR)..."
	@mkdir -p $(INCLUDEDIR)
	@cp include/butterfly.h $(INCLUDEDIR)/

install-pkgconfig:
	@echo "⚙️ Installing pkg-config file..."
	@mkdir -p $(PKGCONFIGDIR)
	@sed -e 's|@PREFIX@|$(PREFIX)|g' \
	     -e "s|@VERSION@|$$(cat VERSION)|g" \
	     butterfly-dl.pc.in > $(PKGCONFIGDIR)/butterfly-dl.pc

# Build examples
examples: c-lib
	@echo "🧪 Building C examples..."
	@if [ -d examples ]; then \
		$(MAKE) -C examples; \
	else \
		echo "No examples directory found"; \
	fi

# Run tests
test:
	@echo "🧪 Running Rust tests..."
	$(CARGO) test -p butterfly-dl
	@echo "🧪 Running C FFI tests..."
	$(CARGO) test -p butterfly-dl --features c-bindings

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	$(CARGO) clean
	@rm -f $(PKGCONFIGDIR)/butterfly-dl.pc

# Development helpers
dev-build:
	@echo "🔧 Development build with all features..."
	$(CARGO) build --all-features -p butterfly-dl

check:
	@echo "🔍 Checking code..."
	$(CARGO) check --all-features -p butterfly-dl
	$(CARGO) clippy --all-features -p butterfly-dl

fmt:
	@echo "📐 Formatting code..."
	$(CARGO) fmt -p butterfly-dl

# Show build information
info:
	@echo "📊 Build Information:"
	@echo "   Cargo: $(shell $(CARGO) --version)"
	@echo "   Target: $(shell $(CARGO) --version --verbose | grep host | cut -d' ' -f2)"
	@echo "   Features available:"
	@echo "     - s3: S3 support for planet downloads"
	@echo "     - c-bindings: C-compatible FFI interface"
	@echo "   Output files:"
	@echo "     - target/release/libbutterfly_dl.rlib (Rust library)"
	@echo "     - target/release/libbutterfly_dl.a (C static library)"
	@echo "     - target/release/libbutterfly_dl.so (C dynamic library, Linux)"
	@echo "     - target/release/libbutterfly_dl.dylib (C dynamic library, macOS)"
	@echo "     - target/release/butterfly_dl.dll (C dynamic library, Windows)"
	@echo "     - target/release/butterfly-dl (CLI binary)"

help:
	@echo "🦋 Butterfly-dl Build System"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Build all libraries (Rust + C)"
	@echo "  rust-lib     - Build Rust library (.rlib)"
	@echo "  c-lib        - Build C libraries (static + dynamic)"
	@echo "  static       - Build static C library (.a)"
	@echo "  dynamic      - Build dynamic C library (.so/.dylib/.dll)"
	@echo "  install      - Install libraries and headers system-wide"
	@echo "  examples     - Build C examples (if available)"
	@echo "  test         - Run all tests"
	@echo "  clean        - Clean build artifacts"
	@echo "  info         - Show build information"
	@echo "  help         - Show this help"
	@echo ""
	@echo "Development:"
	@echo "  dev-build    - Development build with all features"
	@echo "  check        - Check code and run clippy"
	@echo "  fmt          - Format code"
	@echo ""
	@echo "Variables:"
	@echo "  PREFIX       - Installation prefix (default: /usr/local)"
	@echo "  CARGO        - Cargo command (default: cargo)"
	@echo ""
	@echo "Examples:"
	@echo "  make all                    # Build everything"
	@echo "  make static                 # Build static library only"
	@echo "  sudo make install           # Install system-wide"
	@echo "  make install PREFIX=/opt    # Install to /opt"