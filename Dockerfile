# syntax=docker/dockerfile:1

# ubuntu:22.04 = glibc 2.35 floor, so the binary runs on 22.04 and 24.04 EC2 hosts.
FROM ubuntu:22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH
# Exact toolchain, components and target from rust-toolchain.toml, so the rustup proxy
# finds everything installed and downloads nothing at build time.
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal \
        --default-toolchain 1.92.0 --component rustfmt --component clippy \
        --component rust-analyzer --target wasm32-unknown-unknown

WORKDIR /build
COPY . .

ARG BIN
# Binary must leave the cache mount within this RUN; cache mounts are not image layers.
RUN --mount=type=cache,id=polysim-cargo-registry,target=/opt/cargo/registry \
    --mount=type=cache,id=polysim-target,target=/build/target \
    test -n "$BIN" || { echo "BIN build-arg required" >&2; exit 1; } \
    && cargo build --locked --profile deploy --bin "$BIN" \
    && mkdir -p /out \
    && cp "target/deploy/$BIN" /out/

FROM scratch AS export
COPY --from=builder /out/ /
