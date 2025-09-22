FROM rust:latest as builder
WORKDIR /app/auth-service

COPY ./auth-service/Cargo.toml .
COPY ./Cargo.lock .
COPY ./auth-service/src ./src

RUN rustc --version && cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/auth-service/target/release/auth-service .

CMD ["./auth-service"]
