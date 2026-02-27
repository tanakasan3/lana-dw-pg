{{
    config(
        unique_key=["disbursal_id", "version"],
    )
}}


with
    source as (select s.* from {{ ref("stg_core_disbursal_events_rollup") }} as s),

    transformed as (
        select
            disbursal_id,
            credit_facility_id,
            cast(effective as timestamp) as effective,
            cast(amount as numeric) / {{ var("cents_per_usd") }} as amount_usd,
            approved,
            is_approval_process_concluded,
            is_settled,
            is_cancelled,
            cast(due_date as timestamp) as due_date,
            overdue_date,
            liquidation_date,
            created_at as disbursal_created_at,
            modified_at as disbursal_modified_at,
            version,
            account_ids,
            approval_process_id,
            disbursal_credit_account_id,
            obligation_id,
            public_id,
            ledger_tx_ids,
            loaded_to_dw_at
        from source
    )

select *
from transformed
