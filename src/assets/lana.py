"""Lana EL (Extract-Load) assets - source PG to destination PG raw schema."""

from typing import List

import dlt

import dagster as dg
from src.core import COLD_START_CONDITION_SKIP_DEPS, Protoasset
from src.dlt_destinations import create_dw_destination
from src.dlt_resources.postgres import create_dlt_postgres_resource
from src.resources import (
    RESOURCE_KEY_DEST_PG,
    RESOURCE_KEY_SOURCE_PG,
    DestPgResource,
    SourcePgResource,
)

LANA_EL_TABLE_NAMES = (
    "core_chart_events_rollup",
    "core_credit_facility_events_rollup",
    "core_credit_facility_proposal_events_rollup",
    "core_customer_events_rollup",
    "core_deposit_account_events_rollup",
    "core_deposit_events_rollup",
    "core_disbursal_events_rollup",
    "core_interest_accrual_cycle_events_rollup",
    "core_obligation_events_rollup",
    "core_payment_allocation_events_rollup",
    "core_payment_events_rollup",
    "core_pending_credit_facility_events_rollup",
    "core_withdrawal_events_rollup",
    "core_public_ids",
    "core_chart_events",
    "core_chart_node_events",
    "cala_account_set_member_account_sets",
    "cala_account_set_member_accounts",
    "cala_account_sets",
    "cala_accounts",
    "cala_balance_history",
    "inbox_events",
)

EL_SOURCE_ASSET_DESCRIPTION = "el_source_asset"
EL_TARGET_ASSET_DESCRIPTION = "el_target_asset"
LANA_SYSTEM_NAME = "lana"


def get_el_source_asset_name(system_name: str, table_name: str) -> str:
    return f"{EL_SOURCE_ASSET_DESCRIPTION}__{system_name}__{table_name}"


def lana_source_protoassets() -> List[Protoasset]:
    """Create external source assets representing lana-bank source tables."""
    lana_source_protoassets = []
    for table_name in LANA_EL_TABLE_NAMES:
        lana_source_protoassets.append(
            Protoasset(
                key=dg.AssetKey(
                    get_el_source_asset_name(
                        system_name=LANA_SYSTEM_NAME, table_name=table_name
                    )
                ),
                tags={
                    "asset_type": EL_SOURCE_ASSET_DESCRIPTION,
                    "system": LANA_SYSTEM_NAME,
                },
            )
        )
    return lana_source_protoassets


def lana_to_dw_el_protoassets() -> List[Protoasset]:
    """Create EL assets that copy data from source PG to destination PG raw schema."""
    lana_el_protoassets = []
    for table_name in LANA_EL_TABLE_NAMES:
        lana_el_protoassets.append(
            build_lana_to_dw_el_protoasset(table_name=table_name)
        )
    return lana_el_protoassets


def build_lana_to_dw_el_protoasset(table_name) -> Protoasset:
    """Build a single EL protoasset for a table."""

    def lana_to_dw_el_asset(
        context: dg.AssetExecutionContext,
        source_pg: SourcePgResource,
        dest_pg: DestPgResource,
    ):
        context.log.info(
            f"Running lana_to_dw_el_asset pipeline for table {table_name}."
        )

        runnable_pipeline = prepare_lana_el_pipeline(
            source_pg=source_pg, dest_pg=dest_pg, table_name=table_name
        )
        load_info = runnable_pipeline()

        context.log.info("Pipeline completed.")
        context.log.info(load_info)

        return load_info

    lana_to_dw_protoasset = Protoasset(
        key=dg.AssetKey([LANA_SYSTEM_NAME, table_name]),
        deps=[
            dg.AssetKey(
                get_el_source_asset_name(
                    system_name=LANA_SYSTEM_NAME, table_name=table_name
                )
            )
        ],
        tags={"asset_type": EL_TARGET_ASSET_DESCRIPTION, "system": LANA_SYSTEM_NAME},
        callable=lana_to_dw_el_asset,
        required_resource_keys={RESOURCE_KEY_SOURCE_PG, RESOURCE_KEY_DEST_PG},
        automation_condition=COLD_START_CONDITION_SKIP_DEPS,
    )

    return lana_to_dw_protoasset


def prepare_lana_el_pipeline(
    source_pg: SourcePgResource,
    dest_pg: DestPgResource,
    table_name: str,
):
    """Prepare a dlt pipeline for loading data from source PG to destination PG."""
    dlt_postgres_resource = create_dlt_postgres_resource(
        connection_string=source_pg.get_connection_string(), table_name=table_name
    )
    
    dlt_destination = create_dw_destination(dest_pg.get_credentials())
    raw_schema = dest_pg.get_raw_schema()

    pipeline = dlt.pipeline(
        pipeline_name=table_name,
        destination=dlt_destination,
        dataset_name=raw_schema,
    )

    def wrapped_pipeline():
        load_info = pipeline.run(
            dlt_postgres_resource,
            write_disposition="replace",
            table_name=table_name,
        )
        return load_info

    return wrapped_pipeline
