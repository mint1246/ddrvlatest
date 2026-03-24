FROM rust:1.89-bookworm AS build

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY internal/http/web/static ./internal/http/web/static

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=build /app/target/release/ddrv /app/ddrv

EXPOSE 2525
EXPOSE 2526

ENTRYPOINT ["/app/ddrv"]
