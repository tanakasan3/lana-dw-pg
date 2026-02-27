.PHONY: help up up-external down restart logs build clean \
        materialize-el materialize-seeds materialize-dbt materialize-all \
        psql psql-schemas psql-tables psql-raw psql-dbt init-schemas

# Default target
help:
	@echo "lana-dw-pg - Standalone Dagster pipeline for lana-bank"
	@echo ""
	@echo "Docker:"
	@echo "  make up                        Start with built-in postgres (default)"
	@echo "  make up-external               Start with external postgres"
	@echo "  make up-external-container     Auto-detect container IP (CONTAINER=name)"
	@echo "  make get-container-ip          Print container IP (CONTAINER=name)"
	@echo "  make down            Stop all services"
	@echo "  make restart         Restart all services"
	@echo "  make logs            Tail logs from all services"
	@echo "  make logs-dagster    Tail logs from dagster code location"
	@echo "  make build           Build docker images"
	@echo "  make clean           Stop and remove volumes"
	@echo ""
	@echo "Dagster Materialization:"
	@echo "  make materialize-el      Run EL (lana source -> raw schema)"
	@echo "  make materialize-seeds   Run dbt seeds"
	@echo "  make materialize-dbt     Run dbt models"
	@echo "  make materialize-all     Run all (EL + seeds + dbt)"
	@echo "  make materialize-bitfinex  Run Bitfinex EL jobs"
	@echo ""
	@echo "Database:"
	@echo "  make psql            Connect to destination PG"
	@echo "  make psql-schemas    List schemas"
	@echo "  make psql-tables     List all tables (raw + dbt)"
	@echo "  make psql-raw        List tables in raw schema"
	@echo "  make psql-dbt        List tables in dbt schema"
	@echo "  make psql-source     Connect to source PG (if separate)"
	@echo "  make init-schemas    Create raw/dbt schemas on external PG"
	@echo ""
	@echo "External PG mode requires these env vars:"
	@echo "  PG_CON, DST_PG_HOST, DST_PG_PORT, DST_PG_DATABASE, DST_PG_USER, DST_PG_PASSWORD"
	@echo ""

# =============================================================================
# Docker
# =============================================================================

# Start with built-in postgres (default)
up:
	docker compose up -d --build

# Start with external postgres (no lana-dw-postgres container)
up-external:
	docker compose --profile external up -d --build

# Start with external postgres, auto-detecting container IP
# Usage: make up-external-container CONTAINER=core-pg
up-external-container:
ifndef CONTAINER
	$(error CONTAINER is required. Usage: make up-external-container CONTAINER=core-pg)
endif
	$(eval DST_IP := $(shell docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(CONTAINER)))
	@echo "Detected $(CONTAINER) IP: $(DST_IP)"
	DST_PG_HOST=$(DST_IP) docker compose --profile external up -d --build

# Get a container's IP address
# Usage: make get-container-ip CONTAINER=core-pg
get-container-ip:
ifndef CONTAINER
	$(error CONTAINER is required. Usage: make get-container-ip CONTAINER=core-pg)
endif
	@docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(CONTAINER)

down:
	docker compose --profile external down
	docker compose down

restart: down up

logs:
	docker compose logs -f

logs-dagster:
	docker compose logs -f dagster-code-location

build:
	docker compose build

clean:
	docker compose --profile external down -v
	docker compose down -v

# =============================================================================
# Dagster Materialization (via dagster CLI in container)
# =============================================================================

# Run all Lana EL jobs (source PG -> raw schema)
materialize-el:
	docker compose exec dagster-code-location dagster asset materialize \
		--select 'lana/*' \
		-d /lana-dw-pg

# Run dbt seeds
materialize-seeds:
	docker compose exec dagster-code-location dagster asset materialize \
		--select 'dbt_lana_dw/seeds/*' \
		-d /lana-dw-pg

# Run dbt models (raw -> dbt schema)
materialize-dbt:
	docker compose exec dagster-code-location dagster asset materialize \
		--select 'dbt_lana_dw/*' \
		-d /lana-dw-pg

# Run everything: EL + seeds + dbt
materialize-all: materialize-el materialize-seeds materialize-dbt

# Run Bitfinex EL jobs
materialize-bitfinex:
	docker compose exec dagster-code-location dagster asset materialize \
		--select 'bitfinex/*' \
		-d /lana-dw-pg

# =============================================================================
# Database - Destination PG
# =============================================================================

# Default connection params (override with env vars)
DST_PG_HOST ?= localhost
DST_PG_PORT ?= 5433
DST_PG_DATABASE ?= lana_dw
DST_PG_USER ?= postgres
DST_PG_PASSWORD ?= postgres
DST_RAW_SCHEMA ?= raw
DST_DBT_SCHEMA ?= dbt

PSQL_CMD = PGPASSWORD=$(DST_PG_PASSWORD) psql -h $(DST_PG_HOST) -p $(DST_PG_PORT) -U $(DST_PG_USER) -d $(DST_PG_DATABASE)

# Connect to destination PG interactively
psql:
	@$(PSQL_CMD)

# List all schemas
psql-schemas:
	@$(PSQL_CMD) -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') ORDER BY schema_name;"

# List all tables in raw + dbt schemas
psql-tables:
	@$(PSQL_CMD) -c "\
		SELECT table_schema, table_name, pg_size_pretty(pg_total_relation_size(quote_ident(table_schema) || '.' || quote_ident(table_name))) as size \
		FROM information_schema.tables \
		WHERE table_schema IN ('$(DST_RAW_SCHEMA)', '$(DST_DBT_SCHEMA)') \
		ORDER BY table_schema, table_name;"

# List tables in raw schema
psql-raw:
	@$(PSQL_CMD) -c "\
		SELECT table_name, pg_size_pretty(pg_total_relation_size('$(DST_RAW_SCHEMA).' || quote_ident(table_name))) as size \
		FROM information_schema.tables \
		WHERE table_schema = '$(DST_RAW_SCHEMA)' \
		ORDER BY table_name;"

# List tables in dbt schema
psql-dbt:
	@$(PSQL_CMD) -c "\
		SELECT table_name, pg_size_pretty(pg_total_relation_size('$(DST_DBT_SCHEMA).' || quote_ident(table_name))) as size \
		FROM information_schema.tables \
		WHERE table_schema = '$(DST_DBT_SCHEMA)' \
		ORDER BY table_name;"

# Count rows in raw tables
psql-raw-counts:
	@$(PSQL_CMD) -c "\
		SELECT table_name, \
			(xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count \
		FROM ( \
			SELECT table_name, \
				query_to_xml('SELECT count(*) as cnt FROM $(DST_RAW_SCHEMA).' || quote_ident(table_name), false, true, '') as xml_count \
			FROM information_schema.tables \
			WHERE table_schema = '$(DST_RAW_SCHEMA)' \
		) t \
		ORDER BY table_name;"

# Initialize schemas on external PG (for external mode)
init-schemas:
	@$(PSQL_CMD) -c "CREATE SCHEMA IF NOT EXISTS $(DST_RAW_SCHEMA);"
	@$(PSQL_CMD) -c "CREATE SCHEMA IF NOT EXISTS $(DST_DBT_SCHEMA);"
	@echo "Schemas created: $(DST_RAW_SCHEMA), $(DST_DBT_SCHEMA)"

# =============================================================================
# Database - Source PG (if using external source)
# =============================================================================

# Parse PG_CON if set, otherwise use defaults
SOURCE_PG_HOST ?= localhost
SOURCE_PG_PORT ?= 5432
SOURCE_PG_DATABASE ?= lana
SOURCE_PG_USER ?= postgres
SOURCE_PG_PASSWORD ?= postgres

PSQL_SOURCE_CMD = PGPASSWORD=$(SOURCE_PG_PASSWORD) psql -h $(SOURCE_PG_HOST) -p $(SOURCE_PG_PORT) -U $(SOURCE_PG_USER) -d $(SOURCE_PG_DATABASE)

# Connect to source PG
psql-source:
	@$(PSQL_SOURCE_CMD)

# List source tables
psql-source-tables:
	@$(PSQL_SOURCE_CMD) -c "\
		SELECT table_name \
		FROM information_schema.tables \
		WHERE table_schema = 'public' \
		ORDER BY table_name;"
