{{
    config(
        unique_key=["payment_allocation_id", "version"],
    )
}}


with
    source as (
        select s.* from {{ ref("stg_core_payment_allocation_events_rollup") }} as s
    ),

    transformed as (
        select
            payment_allocation_id,
            payment_id,
            credit_facility_id,
            cast(amount as numeric) / {{ var("cents_per_usd") }} as amount_usd,
            cast(effective as timestamp) as effective,
            obligation_type,
            payment_allocation_idx,
            payment_holding_account_id,
            receivable_account_id,
            obligation_id,
            created_at as payment_allocation_created_at,
            modified_at as payment_allocation_modified_at,
            version,
            ledger_tx_id,
            loaded_to_dw_at
        from source
    )

select *
from transformed
