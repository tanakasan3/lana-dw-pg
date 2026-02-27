"""Source PostgreSQL resource (lana-bank core database)."""

import dagster as dg

RESOURCE_KEY_SOURCE_PG = "source_pg"


class SourcePgResource(dg.ConfigurableResource):
    """Dagster resource for source PostgreSQL connection (lana-bank)."""

    def get_connection_string(self) -> str:
        """Get the source database connection string from PG_CON env var."""
        return dg.EnvVar("PG_CON").get_value()
