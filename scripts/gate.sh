#!/bin/sh
# Full pre-commit gate: format, lint, test, and audit the whole workspace.
# Needs cargo-deny: `brew install cargo-deny`.
set -eu
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo deny --locked check
