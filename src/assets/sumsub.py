"""Sumsub applicants EL asset - KYC data to destination PG raw schema."""

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


def sumsub_applicants(
    context: dg.AssetExecutionContext,
    dest_pg: DestPgResource,
    sumsub: SumsubResource,
) -> None:
    """Runs the Sumsub applicants DLT pipeline into the data warehouse."""
    sumsub_key, sumsub_secret = sumsub.get_auth()

    dest = create_dw_destination(dest_pg.get_credentials())
    raw_schema = dest_pg.get_raw_schema()

    pipe = dlt.pipeline(
        pipeline_name="sumsub_applicants",
        destination=dest,
        dataset_name=raw_schema,
    )

    dlt_resource = dlt_sumsub_applicants(
        dest_connection_string=dest_pg.get_connection_string(),
        raw_schema=raw_schema,
        sumsub_key=sumsub_key,
        sumsub_secret=sumsub_secret,
        logger=context.log,
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
