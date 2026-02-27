"""Dagster sensors for lana-dw-pg."""

import dagster as dg
from src.assets.dbt import TAG_KEY_ASSET_TYPE, TAG_VALUE_DBT_MODEL, TAG_VALUE_DBT_SEED


def build_dbt_automation_sensor(
    dagster_automations_active: bool,
) -> dg.AutomationConditionSensorDefinition:
    """Build sensor for dbt model automation."""
    return dg.AutomationConditionSensorDefinition(
        name="dbt_automation_condition_sensor",
        target=dg.AssetSelection.tag(TAG_KEY_ASSET_TYPE, TAG_VALUE_DBT_MODEL),
        default_status=(
            dg.DefaultSensorStatus.RUNNING
            if dagster_automations_active
            else dg.DefaultSensorStatus.STOPPED
        ),
    )


def build_cold_start_automation_sensor(
    dagster_automations_active: bool,
) -> dg.AutomationConditionSensorDefinition:
    """Build sensor for cold-start automation (seeds + EL assets)."""
    return dg.AutomationConditionSensorDefinition(
        name="cold_start_automation_condition_sensor",
        target=(
            dg.AssetSelection.tag(TAG_KEY_ASSET_TYPE, TAG_VALUE_DBT_SEED)
            | dg.AssetSelection.tag(TAG_KEY_ASSET_TYPE, "el_target_asset")
        ),
        default_status=(
            dg.DefaultSensorStatus.RUNNING
            if dagster_automations_active
            else dg.DefaultSensorStatus.STOPPED
        ),
    )


def build_sumsub_sensor(
    sumsub_applicants_job: dg.JobDefinition,
    dagster_automations_active: bool,
) -> dg.SensorDefinition:
    """Build sensor to trigger Sumsub sync on inbox_events materialization."""
    def _trigger_sumsub_on_inbox_events(
        _context: dg.SensorEvaluationContext, asset_event
    ):
        dagster_event = getattr(asset_event, "dagster_event", None)
        event_id = getattr(dagster_event, "event_log_entry_id", None) or getattr(
            asset_event, "run_id", None
        )

        yield dg.RunRequest(run_key=f"sumsub_applicants_from_inbox_events_{event_id}")

    return dg.AssetSensorDefinition(
        name="sumsub_applicant_inbox_events_sensor",
        asset_key=dg.AssetKey(["lana", "inbox_events"]),
        job_name=sumsub_applicants_job.name,
        asset_materialization_fn=_trigger_sumsub_on_inbox_events,
        default_status=(
            dg.DefaultSensorStatus.RUNNING
            if dagster_automations_active
            else dg.DefaultSensorStatus.STOPPED
        ),
    )
