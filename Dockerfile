FROM python:3.13-alpine

# Logs show up immediately
ENV PYTHONUNBUFFERED=1
ENV DAGSTER_HOME=/opt/dagster/dagster_home

# Install dependencies
RUN apk --no-cache add \
    curl \
    build-base \
    ca-certificates \
    openssl \
    libffi-dev \
    openssl-dev \
    postgresql-dev \
    postgresql-client \
    && update-ca-certificates

RUN pip install --upgrade pip

# Install dagster and dependencies
# Note: dagster extensions (postgres, dbt, dlt) use 0.x versioning
# dagster and dagster-webserver use 1.x versioning
ARG DAGSTER_VERSION=1.12.0
ARG DAGSTER_EXT_VERSION=0.28.0
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
# Note: dbt-postgres 1.10.0 is the latest, dbt-core must match
ARG DBT_VERSION=1.10.0
ARG DLT_VERSION=1.18.1

RUN pip install \
    dbt-core~=${DBT_VERSION} \
    dbt-postgres~=${DBT_VERSION} \
    dlt[postgres]~=${DLT_VERSION} \
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

# Setup DAGSTER_HOME for CLI commands
RUN mkdir -p ${DAGSTER_HOME}
COPY dagster.yaml workspace.yaml ${DAGSTER_HOME}/

EXPOSE 4000

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-f", "/lana-dw-pg/src/definitions.py", "-d", "/lana-dw-pg"]
