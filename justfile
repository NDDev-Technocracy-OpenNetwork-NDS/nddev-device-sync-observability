set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

setup:
    cargo fetch --locked
    bun install --frozen-lockfile --ignore-scripts

check: browser-check
    just --fmt --check
    cargo fmt --all -- --check
    cargo deny --locked --all-features check licenses sources
    cargo shear --locked --deny-warnings

# Execution host or runner only, including generated JavaScript equivalence.
test: build audit protocol-check sdk-check sdk-wasm-check
    cargo test --locked --workspace --doc --jobs 4
    cargo clippy --workspace --all-targets --all-features --locked --jobs 4 -- -D warnings
    cargo test --workspace --all-features --locked --jobs 4
    cargo test --workspace --all-features --locked --doc --jobs 4
    bun test --reporter=junit --reporter-outfile=.nddev-reports/bun-test.xml
    bun run check:generated

integration:
    python3 scripts/check-compose.py

sdk-check:
    python3 scripts/check-sdk.py

sdk-wasm-check:
    cargo clippy --locked -p nddev-device-sync-telemetry --features client-traces,browser-bindings --target wasm32-unknown-unknown -- -D warnings

protocol-check:
    python3 scripts/check-protocol.py

browser-check:
    bun run check

# Compile the locked release workspace.
build:
    cargo build --locked --workspace --release --jobs 4

# Check current RustSec advisories.
audit:
    cargo audit --deny warnings

# Run source checks, native tests and the advisory audit.
verify: check test audit build
