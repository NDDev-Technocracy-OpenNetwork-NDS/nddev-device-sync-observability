# Bootstrap image has the complete architecture index. The repository toolchain
# selects the current compiler independently of Docker tag publication timing.
FROM rust:1.99.1-trixie@sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07 AS build
WORKDIR /build
ARG TARGETARCH
COPY rust-toolchain.toml ./
RUN test "$(dpkg --print-architecture)" = "$TARGETARCH" && rustup show active-toolchain && rustc --version
ARG NDS_SOURCE_COMMIT=unknown
ENV NDS_SOURCE_COMMIT=${NDS_SOURCE_COMMIT}
COPY Cargo.toml Cargo.lock build.rs contracts.lock ./
COPY crates ./crates
COPY src ./src
COPY contracts ./contracts
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry --mount=type=cache,target=/build/target \
    cargo build --locked --release -j4 && cp target/release/nddev-device-sync-observability /build/nds-observability
FROM debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/nds-observability /usr/local/bin/nds-observability
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/nds-observability"]
