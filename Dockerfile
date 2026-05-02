# =============================================================================
#  STAGE 1 — Rust build environment (Bullseye)
#
#  Compiles all ETL worker binaries in a dedicated builder image.
#  The final image only receives the compiled binaries, keeping it lean.
# =============================================================================
FROM rust:1-slim-bullseye AS builder

WORKDIR /usr/src/app

# System dependencies required to compile Rust crates:
#   pkg-config  → required for OpenSSL detection by cargo
#   libssl-dev  → OpenSSL headers for the mysql crate (isra_etl)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY ./rust_app .

# Single `cargo build --release` compiles all 4 ETL binaries:
#   isra_etl    — ISRA machine data ingestion (MySQL source)
#   fsp_etl     — FSP inspection line ingestion (CSV via rsync/SSH)
#   eagle_etl   — Eagle Vision SL2/LRA1 ingestion (CSV via SMB)
#   backup_etl  — Quarterly cold archival to Parquet (Snappy compressed)
#
# Release profile applies: opt-level=3, LTO, single codegen-unit.
# See Cargo.toml [profile.release] for full settings.
RUN cargo build --release

# =============================================================================
#  STAGE 2 — Production image: Apache Airflow + Rust ETL binaries
#
#  Base: official Apache Airflow image (Python 3.11).
#  Adds: runtime libraries + network tools for OT machine connectivity.
#  Result: a single self-contained image that orchestrates AND processes
#  inspection data from glass manufacturing lines.
# =============================================================================
FROM apache/airflow:2.7.2-python3.11

USER root

# Runtime dependencies:
#   libssl1.1       → OpenSSL runtime compatible with Debian Bullseye
#   ca-certificates → TLS certificate validation for MySQL and PostgreSQL connections
#   smbclient       → SMB file transfer from Eagle Vision machines (SL2 / LRA1 lines)
#   sshpass         → Non-interactive SSH authentication for FSP rsync transfers
#   rsync           → Incremental, checksum-verified file sync from FSP host
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       libssl1.1 \
       ca-certificates \
       smbclient \
       sshpass \
       rsync \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled Rust binaries from the builder stage.
# Binaries are placed in /opt/airflow/bin so Airflow DAGs can invoke them
# without PATH manipulation.
RUN mkdir -p /opt/airflow/bin

COPY --from=builder /usr/src/app/target/release/isra_etl   /opt/airflow/bin/isra_etl
COPY --from=builder /usr/src/app/target/release/fsp_etl    /opt/airflow/bin/fsp_etl
COPY --from=builder /usr/src/app/target/release/eagle_etl  /opt/airflow/bin/eagle_etl
COPY --from=builder /usr/src/app/target/release/backup_etl /opt/airflow/bin/backup_etl

RUN chmod +x \
    /opt/airflow/bin/isra_etl \
    /opt/airflow/bin/fsp_etl \
    /opt/airflow/bin/eagle_etl \
    /opt/airflow/bin/backup_etl

USER airflow

# Python dependency required by the ISRA discovery DAG
# (direct MySQL query for machine metadata — separate from the Rust ingestion path)
RUN pip install --no-cache-dir mysql-connector-python
