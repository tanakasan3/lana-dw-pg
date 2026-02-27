"""DLT destination factory for PostgreSQL data warehouse."""

import os
from typing import Any, Dict

import dlt


def get_raw_schema() -> str:
    """Get the raw data schema name."""
    return os.getenv("DST_RAW_SCHEMA", "raw")


def create_dw_destination(credentials: Dict[str, Any] | None = None):
    """Create a dlt Postgres destination.
    
    Args:
        credentials: Optional credentials dict with host, port, database, 
                     username, password. If None, reads from DST_* env vars.
    
    Returns:
        A dlt postgres destination.
    """
    if credentials is None:
        credentials = {
            "host": os.getenv("DST_PG_HOST", "localhost"),
            "port": int(os.getenv("DST_PG_PORT", "5432")),
            "database": os.getenv("DST_PG_DATABASE", "lana_dw"),
            "username": os.getenv("DST_PG_USER", "postgres"),
            "password": os.getenv("DST_PG_PASSWORD", ""),
        }
    
    return dlt.destinations.postgres(credentials=credentials)


__all__ = [
    "get_raw_schema",
    "create_dw_destination",
]
