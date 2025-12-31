.PHONY: fmt lint build test ci

fmt:
	cargo fmt

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings

build:
	cargo build --release

test:
	cargo test --all-targets --all-features

ci: lint test build
