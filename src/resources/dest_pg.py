"""Destination PostgreSQL resource (data warehouse)."""

import os
from typing import Dict, Any

import dagster as dg

RESOURCE_KEY_DEST_PG = "dest_pg"


class DestPgResource(dg.ConfigurableResource):
    """Dagster resource for destination PostgreSQL configuration."""

    def get_raw_schema(self) -> str:
        """Get the schema where raw data is loaded by dlt."""
        return os.getenv("DST_RAW_SCHEMA", "raw")

    def get_dbt_schema(self) -> str:
        """Get the schema where dbt writes models."""
        return os.getenv("DST_DBT_SCHEMA", "dbt")

    def get_connection_params(self) -> Dict[str, Any]:
        """Get destination Postgres connection parameters."""
        return {
            "host": os.getenv("DST_PG_HOST", "localhost"),
            "port": int(os.getenv("DST_PG_PORT", "5432")),
            "database": os.getenv("DST_PG_DATABASE", "lana_dw"),
            "user": os.getenv("DST_PG_USER", "postgres"),
            "password": os.getenv("DST_PG_PASSWORD", ""),
        }

    def get_connection_string(self) -> str:
        """Get destination Postgres connection string."""
        params = self.get_connection_params()
        return (
            f"postgresql://{params['user']}:{params['password']}"
            f"@{params['host']}:{params['port']}/{params['database']}"
        )

    def get_credentials(self) -> Dict[str, Any]:
        """Get credentials dict for dlt destination."""
        params = self.get_connection_params()
        return {
            "host": params["host"],
            "port": params["port"],
            "database": params["database"],
            "username": params["user"],
            "password": params["password"],
        }
