# Static binary in an empty image.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/rowkit /rowkit

FROM scratch
COPY --from=build /rowkit /rowkit
USER 1000:1000
CMD ["/rowkit"]
