#!/bin/sh
# Full pre-commit gate: check Rust comments, format, lint, test, and audit.
# Needs cargo-deny: `brew install cargo-deny`.
set -eu
cd "$(dirname "$0")/.."
cargo run --quiet --locked -p nt-dex-rust-comments
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo deny --locked check
