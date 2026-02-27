{{
    config(
        unique_key=["interest_accrual_cycle_id", "version"],
    )
}}


with
    source as (
        select s.* from {{ ref("stg_core_interest_accrual_cycle_events_rollup") }} as s
    ),

    transformed as (
        select
            interest_accrual_cycle_id,
            credit_facility_id,
            cast(facility_maturity_date as timestamp) as facility_maturity_date,

            idx,
            ((period::jsonb)->>'start')::timestamp as period_start_at,
            ((period::jsonb)->>'end')::timestamp as period_end_at,
            (period::jsonb)->'interval'->>'type' as period_interval_type,

            cast((terms::jsonb)->>'annual_rate' as numeric) as annual_rate,
            cast((terms::jsonb)->>'one_time_fee_rate' as numeric) as one_time_fee_rate,
            cast((terms::jsonb)->>'initial_cvl' as numeric) as initial_cvl,
            cast((terms::jsonb)->>'liquidation_cvl' as numeric) as liquidation_cvl,
            cast((terms::jsonb)->>'margin_call_cvl' as numeric) as margin_call_cvl,
            cast((terms::jsonb)->'duration'->>'value' as integer) as duration_value,
            (terms::jsonb)->'duration'->>'type' as duration_type,
            (terms::jsonb)->'accrual_interval'->>'type' as accrual_interval,
            (terms::jsonb)->'accrual_cycle_interval'->>'type' as accrual_cycle_interval,
            cast((terms::jsonb)->'interest_due_duration_from_accrual'->>'value' as integer) as interest_due_duration_from_accrual_value,
            (terms::jsonb)->'interest_due_duration_from_accrual'->>'type' as interest_due_duration_from_accrual_type,
            cast((terms::jsonb)->'obligation_overdue_duration_from_due'->>'value' as integer) as obligation_overdue_duration_from_due_value,
            (terms::jsonb)->'obligation_overdue_duration_from_due'->>'type' as obligation_overdue_duration_from_due_type,
            cast((terms::jsonb)->'obligation_liquidation_duration_from_due'->>'value' as integer) as obligation_liquidation_duration_from_due_value,
            (terms::jsonb)->'obligation_liquidation_duration_from_due'->>'type' as obligation_liquidation_duration_from_due_type,

            cast(accrued_at as timestamp) as last_accrued_at,
            cast(amount as numeric)
            / {{ var("cents_per_usd") }} as last_accrued_interest_amount_usd,

            cast(effective as timestamp) as posted_effective,
            cast(total as numeric)
            / {{ var("cents_per_usd") }} as posted_total_interest_usd,
            obligation_id as posted_obligation_id,
            is_interest_accruals_posted,

            created_at as interest_accrual_cycle_created_at,
            modified_at as interest_accrual_cycle_modified_at,
            version,
            account_ids,
            tx_ref,
            ledger_tx_ids,
            loaded_to_dw_at
        from source
    )

select *
from transformed
