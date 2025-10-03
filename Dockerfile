# Build the ft-relay binary
FROM rust:1.86 as build
WORKDIR /app

# System deps needed for near-api bindings
RUN apt-get update \
    && apt-get install -y --no-install-recommends libudev-dev \
    && rm -rf /var/lib/apt/lists/*

# Leverage caching by compiling dependencies first
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY resources ./resources
RUN cargo build --release --locked

# Minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY --from=build /app/target/release/ft-relay /usr/local/bin/ft-relay

ENV RUST_LOG=info
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/ft-relay"]
# Provide the FT contract at runtime, e.g.:
#   docker run --env-file .env ghcr.io/your-org/ft-relay:latest --token your-ft.testnet
CMD ["--help"]
