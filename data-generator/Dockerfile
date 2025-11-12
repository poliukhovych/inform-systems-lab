FROM rust:latest AS builder
WORKDIR /app/data-generator

COPY ./data-generator/Cargo.toml .
COPY ./Cargo.lock .
COPY ./data-generator/src ./src

RUN rustc --version && cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/data-generator/target/release/data-generator .

CMD ["./data-generator"]
