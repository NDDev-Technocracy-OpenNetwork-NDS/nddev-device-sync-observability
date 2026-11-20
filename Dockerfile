FROM rust:1.100.0-trixie@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS build
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
