# ################################################################################
# # Configuration and Variables
# ################################################################################
ZIG    ?= $(shell which zig || echo ~/.local/share/zig/0.15.2/zig)
BUILD_TYPE    ?= Debug
JOBS          ?= $(shell nproc || echo 2)
SRC_DIR       := src
BUILD_DIR     := zig-out
CACHE_DIR     := .zig-cache
RELEASE_MODE := ReleaseFast
TEST_FLAGS := --summary all #--verbose
JUNK_FILES := *.o *.obj *.dSYM *.dll *.so *.dylib *.a *.lib *.pdb temp/

# Extension configuration
EXTENSION_NAME ?= vizier
EXTENSION_API_VERSION ?= v1.2.0
EXTENSION_VERSION ?= v0.1.0
PLATFORM ?= linux_amd64

SHELL         := /usr/bin/env bash
.SHELLFLAGS   := -eu -o pipefail -c

################################################################################
# Targets
################################################################################

.PHONY: all help build build-all rebuild test test-unit test-property test-integration release clean \
 lint format docs serve-docs install-deps duckdb-translate duckdb

.DEFAULT_GOAL := help

help: ## Show the help messages for all targets
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*## .*$$' Makefile | \
	awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Configuration Variables:"
	@echo "  EXTENSION_API_VERSION    DuckDB extension API version to target (default: $(EXTENSION_API_VERSION))"
	@echo "  EXTENSION_VERSION Extension version (default: $(EXTENSION_VERSION))"
	@echo "  PLATFORM          Target platform (default: $(PLATFORM))"
	@echo ""
	@echo "Examples:"
	@echo "  make build-all EXTENSION_API_VERSION=v1.3.0"
	@echo "  make build-all EXTENSION_API_VERSION=v1.2.0 EXTENSION_VERSION=v1.0.0"

all: build test  ## Build and test (use 'make build-all' for extension with metadata)

build: ## Build extension library
	@echo "Building the DuckDB extension with $(JOBS) concurrent jobs..."
	@$(ZIG) build \
		-Dextension-name=$(EXTENSION_NAME) \
		-j$(JOBS)

build-all: ## Build extension with DuckDB metadata (ready to load)
	@echo "Building the DuckDB extension with API $(EXTENSION_API_VERSION)..."
	@$(ZIG) build build-all \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=$(PLATFORM) \
		-j$(JOBS)

rebuild: clean build-all  ## Clean and build with metadata

test: ## Run all tests (unit, property, integration)
	@echo "Running all tests..."
	@$(ZIG) build test \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=$(PLATFORM) \
		-j$(JOBS) $(TEST_FLAGS)

test-unit: ## Run unit and regression tests
	@echo "Running unit tests..."
	@$(ZIG) build test-unit -j$(JOBS) $(TEST_FLAGS)

test-property: ## Run property-based tests (Minish)
	@echo "Running property-based tests..."
	@$(ZIG) build test-property -j$(JOBS) $(TEST_FLAGS)

test-integration: build-all  ## Run integration tests (requires DuckDB)
	@echo "Running integration tests..."
	@$(ZIG) build test-integration \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=$(PLATFORM) \
		$(TEST_FLAGS)

test-sql: build-all  ## Run standalone SQL tests (needs DuckDB)
	@echo "Running SQL tests..."
	@fail=0; \
	for f in tests/sql/*.sql; do \
		name=$$(basename "$$f"); \
		if duckdb -unsigned -c ".read $$f" > /dev/null 2>&1; then \
			printf "  %-40s PASS\n" "$$name"; \
		else \
			printf "  %-40s FAIL\n" "$$name"; \
			fail=1; \
		fi; \
	done; \
	[ $$fail -eq 0 ] && echo "All SQL tests passed." || (echo "Some SQL tests failed." && exit 1)

bench: build-all  ## Run benchmarks (needs DuckDB)
	@echo "Running benchmarks..."
	@./benches/run.sh


release: ## Build in ReleaseFast mode with metadata
	@echo "Building the extension in Release mode with API $(EXTENSION_API_VERSION)..."
	@$(ZIG) build build-all \
		-Doptimize=ReleaseFast \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=$(PLATFORM) \
		-j$(JOBS)

clean: ## Remove build artifacts, cache, and generated docs
	@echo "Removing build artifacts, cache, and junk files..."
	@$(ZIG) build clean
	@rm -rf $(JUNK_FILES) docs/api public

lint: ## Check code style and formatting of Zig files
	@echo "Running code style checks..."
	@$(ZIG) fmt --check $(SRC_DIR)

format: ## Format Zig and C files
	@echo "Formatting Zig files..."
	@$(ZIG) fmt $(SRC_DIR)
	@echo "Formatting C files..."
	@if command -v clang-format &> /dev/null; then \
		find $(SRC_DIR) -name "*.c" -o -name "*.h" | xargs clang-format -i; \
	else \
		echo "clang-format not found, skipping C formatting"; \
	fi

docs: ## Generate API documentation
	@echo "Generating API documentation..."
	@$(ZIG) build docs

serve-docs: docs  ## Serve the generated documentation on a local server
	@echo "Serving API documentation at http://localhost:8000"
	@cd docs/api && python3 -m http.server 8000

duckdb-translate: ## Regenerate Zig bindings from DuckDB C API headers
	@echo "Generating DuckDB Zig bindings..."
	@$(ZIG) build duckdb-translate

duckdb: build-all  ## Start interactive DuckDB with the extension loaded
	@echo "Starting DuckDB with extension pre-loaded..."
	@$(ZIG) build duckdb \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=$(PLATFORM)

install-deps: ## Install system dependencies (for Debian-based systems)
	@echo "Installing system dependencies..."
	@sudo apt-get update
	@sudo apt-get install -y build-essential python3 python3-pip clang-format
	@echo "Note: Install zig separately or use the version in ~/.local/share/zig/0.15.1/"

.PHONY: build-multi-version
build-multi-version: ## Build extension (works with DuckDB v1.2.0 and later)
	@echo "Note: With Extension API v1.2.0, one build works across multiple DuckDB versions!"
	@echo "Building single extension with API $(EXTENSION_API_VERSION)..."
	@$(MAKE) build-all
	@echo "Done! This extension works with DuckDB v1.2.0 or newer"

# Cross-compilation targets
.PHONY: build-linux-amd64 build-linux-arm64 build-linux-amd64-musl build-linux-arm64-musl build-macos-amd64 build-macos-arm64 build-windows-amd64 build-windows-arm64
build-linux-amd64: ## Build for Linux x86_64
	@echo "Building for Linux AMD64..."
	@$(ZIG) build build-all \
		-Dtarget=x86_64-linux-gnu \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=linux_amd64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/linux_amd64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/linux_amd64/extension.duckdb_extension

build-linux-arm64: ## Build for Linux ARM64
	@echo "Building for Linux ARM64..."
	@$(ZIG) build build-all \
		-Dtarget=aarch64-linux-gnu \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=linux_arm64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/linux_arm64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/linux_arm64/extension.duckdb_extension

build-linux-amd64-musl: ## Build for Linux x86_64 with musl libc (for Alpine Linux)
	@echo "Building for Linux AMD64 (musl)..."
	@$(ZIG) build build-all \
		-Dtarget=x86_64-linux-musl \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=linux_amd64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/linux_amd64_musl
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/linux_amd64_musl/extension.duckdb_extension

build-linux-arm64-musl: ## Build for Linux ARM64 with musl libc (for Alpine Linux)
	@echo "Building for Linux ARM64 (musl)..."
	@$(ZIG) build build-all \
		-Dtarget=aarch64-linux-musl \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=linux_arm64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/linux_arm64_musl
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/linux_arm64_musl/extension.duckdb_extension

build-macos-amd64: ## Build for macOS x86_64 (Intel)
	@echo "Building for macOS AMD64..."
	@$(ZIG) build build-all \
		-Dtarget=x86_64-macos \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=osx_amd64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/osx_amd64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/osx_amd64/extension.duckdb_extension

build-macos-arm64: ## Build for macOS ARM64 (Apple Silicon)
	@echo "Building for macOS ARM64..."
	@$(ZIG) build build-all \
		-Dtarget=aarch64-macos \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=osx_arm64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/osx_arm64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/osx_arm64/extension.duckdb_extension

build-windows-amd64: ## Build for Windows x86_64
	@echo "Building for Windows AMD64..."
	@$(ZIG) build build-all \
		-Dtarget=x86_64-windows-gnu \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=windows_amd64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/windows_amd64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/windows_amd64/extension.duckdb_extension

build-windows-arm64: ## Build for Windows ARM64
	@echo "Building for Windows ARM64..."
	@$(ZIG) build build-all \
		-Dtarget=aarch64-windows-gnu \
		-Dextension-name=$(EXTENSION_NAME) \
		-Dapi-version=$(EXTENSION_API_VERSION) \
		-Dextension-version=$(EXTENSION_VERSION) \
		-Dplatform=windows_arm64 \
		-j$(JOBS)
	@mkdir -p $(BUILD_DIR)/lib/windows_arm64
	@cp $(BUILD_DIR)/lib/${EXTENSION_NAME}.duckdb_extension $(BUILD_DIR)/lib/windows_arm64/extension.duckdb_extension

# Build for all major platforms
.PHONY: build-all-platforms
build-all-platforms: ## Build for all major platforms (Linux glibc, Linux musl, macOS, and Windows)
	@echo "Building the extension for all major platforms..."
	@$(MAKE) build-linux-amd64
	@$(MAKE) build-linux-arm64
	@$(MAKE) build-linux-amd64-musl
	@$(MAKE) build-linux-arm64-musl
	@$(MAKE) build-macos-amd64
	@$(MAKE) build-macos-arm64
	@$(MAKE) build-windows-amd64
	@$(MAKE) build-windows-arm64
	@echo ""
	@echo "Done! Built for all platforms:"
	@find $(BUILD_DIR)/lib -name "${EXTENSION_NAME}.duckdb_extension" -type f -exec echo "  {}" \; -exec ls -lh {} \;

.PHONY: setup-hooks
setup-hooks: ## Install Git hooks (pre-commit and pre-push)
	@echo "Setting up Git hooks..."
	@if ! command -v pre-commit &> /dev/null; then \
	   echo "pre-commit not found. Please install it using 'pip install pre-commit'"; \
	   exit 1; \
	fi
	@pre-commit install --hook-type pre-commit
	@pre-commit install --hook-type pre-push
	@pre-commit install-hooks

.PHONY: test-hooks
test-hooks: ## Test Git hooks on all files
	@echo "Testing Git hooks..."
	@pre-commit run --all-files --show-diff-on-failure
