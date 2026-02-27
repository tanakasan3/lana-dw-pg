"""Dagster resources module for lana-dw-pg."""

import os
from pathlib import Path

from dagster_dbt import DbtCliResource

from src.resources.source_pg import RESOURCE_KEY_SOURCE_PG, SourcePgResource
from src.resources.dest_pg import RESOURCE_KEY_DEST_PG, DestPgResource
from src.resources.sumsub import RESOURCE_KEY_SUMSUB, SumsubResource

# dbt resource configuration
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"
RESOURCE_KEY_DBT = "dbt"

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
)

__all__ = [
    # Source database (lana-bank)
    "RESOURCE_KEY_SOURCE_PG",
    "SourcePgResource",
    # Destination database (data warehouse)
    "RESOURCE_KEY_DEST_PG",
    "DestPgResource",
    # dbt
    "RESOURCE_KEY_DBT",
    "dbt_resource",
    "DBT_MANIFEST_PATH",
    # External APIs
    "RESOURCE_KEY_SUMSUB",
    "SumsubResource",
    # Factory
    "get_project_resources",
]


def get_project_resources():
    """Get all project resources as a dictionary."""
    return {
        RESOURCE_KEY_SOURCE_PG: SourcePgResource(),
        RESOURCE_KEY_DEST_PG: DestPgResource(),
        RESOURCE_KEY_DBT: dbt_resource,
        RESOURCE_KEY_SUMSUB: SumsubResource(),
    }
