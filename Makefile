# Petit Makefile
# Common targets for running the orchestrator and TUI

# Configuration
JOBS_DIR ?= examples/jobs
DB_FILE ?= petit.db

.PHONY: help build build-tui run run-sqlite tui clean test

help:
	@echo "Petit - Task Orchestrator"
	@echo ""
	@echo "Usage:"
	@echo "  make build        Build the petit binary (default features)"
	@echo "  make build-tui    Build with TUI support (includes sqlite)"
	@echo "  make run          Run scheduler with in-memory storage"
	@echo "  make run-sqlite   Run scheduler with SQLite storage (DB_FILE=$(DB_FILE))"
	@echo "  make tui          Run the TUI dashboard (requires run-sqlite in another terminal)"
	@echo "  make test         Run all tests"
	@echo "  make clean        Remove build artifacts and database"
	@echo ""
	@echo "Environment Variables:"
	@echo "  JOBS_DIR          Directory containing job YAML files (default: $(JOBS_DIR))"
	@echo "  DB_FILE           SQLite database file path (default: $(DB_FILE))"
	@echo ""
	@echo "Examples:"
	@echo "  make run-sqlite                    # Run with default settings"
	@echo "  make run-sqlite DB_FILE=./my.db    # Use custom database file"
	@echo "  make tui DB_FILE=./my.db           # Monitor custom database"

# Build targets
build:
	cargo build --release

build-tui:
	cargo build --release --features tui

# Run targets
run:
	cargo run -- run $(JOBS_DIR)

run-sqlite: build-tui
	cargo run --features sqlite -- run $(JOBS_DIR) --db $(DB_FILE)

tui: build-tui
	cargo run --features tui --bin petit-tui -- --db $(DB_FILE)

# Development targets
test:
	cargo test --features tui

clean:
	cargo clean
	rm -f $(DB_FILE)
