# lana-dw-pg

Standalone Dagster data pipeline for lana-bank, targeting PostgreSQL only.

## Architecture

```
┌──────────────────────────┐     ┌─────────────────────────────────────────┐
│   SOURCE PG (PG_CON)     │     │        DESTINATION PG                   │
│    public schema         │────▶│  ┌────────────┐      ┌────────────┐    │
│  - rollup tables         │ dlt │  │ raw schema │─────▶│ dbt schema │    │
│  - cala_* tables         │     │  │  (EL data) │ dbt  │ (models)   │    │
│  - core_* tables         │     │  └────────────┘      └────────────┘    │
└──────────────────────────┘     └─────────────────────────────────────────┘
```

## Environment Variables

### Source Database (lana-bank)
| Variable | Description | Example |
|----------|-------------|---------|
| `PG_CON` | Source lana-bank postgres connection | `postgres://user:pass@core-pg:5432/lana` |

### Destination Database (Data Warehouse)
| Variable | Default | Description |
|----------|---------|-------------|
| `DST_PG_HOST` | `localhost` | Destination PG host |
| `DST_PG_PORT` | `5432` | Destination PG port |
| `DST_PG_DATABASE` | `lana_dw` | Destination database name |
| `DST_PG_USER` | `postgres` | Destination user |
| `DST_PG_PASSWORD` | `` | Destination password |
| `DST_RAW_SCHEMA` | `raw` | Schema for raw EL data |
| `DST_DBT_SCHEMA` | `dbt` | Schema for dbt model outputs |

### Dagster Metadata Database
| Variable | Description |
|----------|-------------|
| `DAGSTER_POSTGRES_HOST` | Dagster metadata PG host |
| `DAGSTER_POSTGRES_PORT` | Dagster metadata PG port |
| `DAGSTER_POSTGRES_USER` | Dagster metadata PG user |
| `DAGSTER_POSTGRES_PASSWORD` | Dagster metadata PG password |
| `DAGSTER_POSTGRES_DB` | Dagster metadata database |

### External APIs (optional)
| Variable | Description |
|----------|-------------|
| `SUMSUB_KEY` | Sumsub API key |
| `SUMSUB_SECRET` | Sumsub API secret |

### Other
| Variable | Default | Description |
|----------|---------|-------------|
| `DAGSTER_AUTOMATIONS_ACTIVE` | `false` | Enable automations (schedules/sensors) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `` | OpenTelemetry collector endpoint |

## Quick Start

### Using Docker Compose

```bash
# Default mode: with built-in postgres
make up
# or: docker compose up -d

# External mode: use your own postgres (no lana-dw-postgres container)
export PG_CON="postgres://user:pass@your-source:5432/lana"
export DST_PG_HOST="your-dest-host"
export DST_PG_PORT="5432"
export DST_PG_DATABASE="lana_dw"
export DST_PG_USER="postgres"
export DST_PG_PASSWORD="secret"

# Create schemas on external PG first
make init-schemas

# Start without built-in postgres
make up-external
# or: docker compose --profile external up -d
```

### Local Development

```bash
# Set environment variables
export PG_CON="postgres://user:pass@localhost:5432/lana"
export DST_PG_HOST=localhost
export DST_PG_PORT=5433
export DST_PG_DATABASE=lana_dw
export DST_PG_USER=postgres
export DST_PG_PASSWORD=postgres
export DST_RAW_SCHEMA=raw
export DST_DBT_SCHEMA=dbt

# Install dependencies
pip install -e .

# Run Dagster dev server
dagster dev
```

## Data Flow

1. **EL (Extract-Load)**: dlt extracts tables from source PG `public` schema → destination PG `raw` schema
2. **Transform**: dbt transforms data from `raw` schema → `dbt` schema

### Tables Extracted (22)

| Category | Tables |
|----------|--------|
| Rollups | `core_chart_events_rollup`, `core_credit_facility_events_rollup`, `core_customer_events_rollup`, etc. |
| CALA | `cala_accounts`, `cala_balance_history`, `cala_account_sets`, etc. |
| Core | `core_public_ids`, `core_chart_events`, `core_chart_node_events`, `inbox_events` |

### External Sources

- **Bitfinex**: Ticker, trades, order book snapshots
- **Sumsub**: KYC applicant data (triggered by inbox_events)

## All-in-One Mode

You can run everything in a single PostgreSQL instance:
- `public` schema: Source tables (lana-bank data)
- `raw` schema: EL staging tables
- `dbt` schema: Transformed models

Set `PG_CON` to point to the same database as `DST_*` variables.
