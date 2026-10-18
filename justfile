set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

check: browser-check
    cargo fmt --all -- --check
    cargo test --workspace --all-features --locked --jobs 4
    cargo clippy --workspace --all-targets --all-features --locked --jobs 4 -- -D warnings

integration:
    python3 scripts/check-compose.py

sdk-check:
    python3 scripts/check-sdk.py

sdk-wasm-check:
    cargo clippy --locked -p nddev-device-sync-telemetry --features client-traces,browser-bindings --target wasm32-unknown-unknown -- -D warnings

protocol-check:
    python3 scripts/check-protocol.py

browser-check:
    bun install --frozen-lockfile --ignore-scripts
    bun run check
    bun run check:generated
