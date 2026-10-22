# --- Build -------------------------------------------------------------------
FROM rust:1-slim-trixie AS build
WORKDIR /src

# Pré-compilation des dépendances (couche mise en cache tant que Cargo.* ne change pas)
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

# --- Runtime : Debian 13 (trixie) slim ----------------------------------------
# Le binaire ne dépend que de la glibc (TLS via rustls + racines webpki embarquées) :
# aucun paquet supplémentaire n'est installé.
FROM debian:13-slim
RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin saisonnier
WORKDIR /app
COPY --from=build /src/target/release/saisonnier /usr/local/bin/saisonnier
COPY schemas /app/schemas

ENV LISTEN_ADDR=0.0.0.0:7000 \
    SCHEMA_DIR=/app/schemas \
    RUST_LOG=info

EXPOSE 7000
USER saisonnier
ENTRYPOINT ["/usr/local/bin/saisonnier"]
