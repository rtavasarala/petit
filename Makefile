.PHONY: all check fmt fmt-check lint test build clean

# Run all checks (formatting, linting, tests)
all: fmt-check lint test

# Quick compile check without producing binaries
check:
	cargo check --all-features

# Format code
fmt:
	cargo fmt

# Check formatting without modifying files
fmt-check:
	cargo fmt -- --check

# Run clippy linter with warnings as errors
lint:
	cargo clippy --all-features --all-targets -- -D warnings

# Run tests
test:
	cargo test --all-features

# Build release binary
build:
	cargo build --release

# Clean build artifacts
clean:
	cargo clean

# Run CI checks (same as GitHub Actions)
ci: fmt-check lint test
