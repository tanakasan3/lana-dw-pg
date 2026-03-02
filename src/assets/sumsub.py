"""Sumsub applicants EL asset - KYC data to destination PG raw schema."""

import os

import dlt

import dagster as dg
from src.assets.lana import LANA_SYSTEM_NAME
from src.core import COLD_START_CONDITION, Protoasset
from src.dlt_destinations import create_dw_destination
from src.dlt_resources.sumsub import SUMSUB_APPLICANTS_DLT_TABLE
from src.dlt_resources.sumsub import applicants as dlt_sumsub_applicants
from src.resources import (
    RESOURCE_KEY_DEST_PG,
    RESOURCE_KEY_SUMSUB,
    DestPgResource,
    SumsubResource,
)

SUMSUB_SYSTEM_NAME = "sumsub"


class SumsubConfig(dg.Config):
    """Configuration for the Sumsub applicants asset."""
    
    use_test_ids: bool = dg.Field(
        default=False,
        description=(
            "If True, substitute local customer IDs with known good Sumsub "
            "external user IDs from sumsub_approved_applicants.csv. "
            "Useful for testing with real Sumsub data when local IDs don't exist."
        ),
    )
    test_ids_csv_path: str = dg.Field(
        default="",
        description="Optional custom path to the test ID mappings CSV file.",
    )


def sumsub_applicants(
    context: dg.AssetExecutionContext,
    config: SumsubConfig,
    dest_pg: DestPgResource,
    sumsub: SumsubResource,
) -> None:
    """Runs the Sumsub applicants DLT pipeline into the data warehouse.
    
    Set use_test_ids=True to substitute customer IDs with known good Sumsub 
    external user IDs from the sumsub_approved_applicants.csv file.
    """
    sumsub_key, sumsub_secret = sumsub.get_auth()

    dest = create_dw_destination(dest_pg.get_credentials())
    raw_schema = dest_pg.get_raw_schema()

    pipe = dlt.pipeline(
        pipeline_name="sumsub_applicants",
        destination=dest,
        dataset_name=raw_schema,
    )

    # Check for env var override as well
    use_test_ids = config.use_test_ids or os.getenv("SUMSUB_USE_TEST_IDS", "").lower() in ("true", "1", "yes")
    test_ids_csv_path = config.test_ids_csv_path or os.getenv("SUMSUB_TEST_IDS_CSV_PATH") or None
    
    if use_test_ids:
        context.log.info("Sumsub test ID mode ENABLED")

    dlt_resource = dlt_sumsub_applicants(
        dest_connection_string=dest_pg.get_connection_string(),
        raw_schema=raw_schema,
        sumsub_key=sumsub_key,
        sumsub_secret=sumsub_secret,
        logger=context.log,
        use_test_ids=use_test_ids,
        test_ids_csv_path=test_ids_csv_path if test_ids_csv_path else None,
    )

    load_info = pipe.run(dlt_resource)
    context.log.info(str(load_info))


def sumsub_protoasset() -> Protoasset:
    """Return the Sumsub applicants protoasset."""
    return Protoasset(
        key=dg.AssetKey([SUMSUB_SYSTEM_NAME, SUMSUB_APPLICANTS_DLT_TABLE]),
        callable=sumsub_applicants,
        required_resource_keys={
            RESOURCE_KEY_DEST_PG,
            RESOURCE_KEY_SUMSUB,
        },
        deps=[dg.AssetKey([LANA_SYSTEM_NAME, "inbox_events"])],
        tags={"system": SUMSUB_SYSTEM_NAME, "asset_type": "el_target_asset"},
        automation_condition=COLD_START_CONDITION,
    )
