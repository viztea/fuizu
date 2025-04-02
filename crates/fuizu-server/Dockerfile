FROM rustlang/rust:nightly-bullseye-slim AS builder

WORKDIR /app/fuizu
COPY . . 

RUN cargo install --path crates/fuizu-server

FROM debian:bullseye-slim

WORKDIR /app/fuizu
COPY --from=builder /usr/local/cargo/bin/fuizu-server /app/fuizu/fuizu-server

ENTRYPOINT [ "/app/fuizu/fuizu-server" ]