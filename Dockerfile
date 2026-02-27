FROM python:3.13-alpine

# Logs show up immediately
ENV PYTHONUNBUFFERED=1

# Install dependencies
RUN apk --no-cache add \
    curl \
    build-base \
    ca-certificates \
    openssl \
    libffi-dev \
    openssl-dev \
    postgresql-dev \
    && update-ca-certificates

RUN pip install --upgrade pip

# Install dagster and dependencies
ARG DAGSTER_VERSION=1.12.0
ARG DAGSTER_EXT_VERSION=0.28.1
ARG OTEL_VERSION=1.38.0

RUN pip install \
    dagster~=${DAGSTER_VERSION} \
    dagster-postgres~=${DAGSTER_EXT_VERSION} \
    dagster-dbt~=${DAGSTER_EXT_VERSION} \
    dagster-dlt~=${DAGSTER_EXT_VERSION} \
    opentelemetry-api~=${OTEL_VERSION} \
    opentelemetry-sdk~=${OTEL_VERSION} \
    opentelemetry-exporter-otlp-proto-grpc~=${OTEL_VERSION}

# Install data pipeline packages (PostgreSQL only)
RUN pip install \
    dbt-core~=1.10.3 \
    dbt-postgres~=1.10.3 \
    dlt[postgres]~=1.18.1 \
    pandas \
    requests \
    sqlalchemy \
    psycopg2-binary

# Add project code
RUN mkdir -p /lana-dw-pg
COPY src/ /lana-dw-pg/src/

# Pre-generate dbt manifest at build time
RUN DST_PG_HOST=placeholder \
    DST_PG_DATABASE=placeholder \
    DST_RAW_SCHEMA=raw \
    DST_DBT_SCHEMA=dbt \
    dbt parse --project-dir /lana-dw-pg/src/dbt_project --profiles-dir /lana-dw-pg/src/dbt_project

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "/lana-dw-pg/src/definitions.py", "-d", "/lana-dw-pg"]
