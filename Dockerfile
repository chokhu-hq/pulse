FROM --platform=$BUILDPLATFORM rust:1-trixie AS build

ARG BUILDARCH
ARG TARGETARCH
WORKDIR /src

# cross compile on the build platform instead of emulating the target
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu; gnu=x86-64-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu ;; \
      *) echo "unsupported architecture: $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      apt-get update; \
      apt-get install -y --no-install-recommends "gcc-$gnu" "libc6-dev-$TARGETARCH-cross"; \
      rm -rf /var/lib/apt/lists/*; \
    fi; \
    rustup target add "$triple"; \
    echo "$triple" > /triple; \
    echo "$gnu" > /gnu

COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN set -eu; \
    triple=$(cat /triple); \
    if [ "$TARGETARCH" != "$BUILDARCH" ]; then \
      export "CARGO_TARGET_$(echo "$triple" | tr 'a-z-' 'A-Z_')_LINKER=$(cat /gnu)-gcc"; \
    fi; \
    cargo build --release --locked -p pulse --target "$triple"; \
    cp "target/$triple/release/pulse" /pulse

FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=build /pulse /usr/local/bin/pulse

ENV PULSE_CONFIG=/etc/pulse/config.toml
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/pulse"]
