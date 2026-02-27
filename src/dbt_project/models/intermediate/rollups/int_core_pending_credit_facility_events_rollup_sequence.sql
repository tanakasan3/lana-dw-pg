{{
    config(
        unique_key=["pending_credit_facility_id", "version"],
    )
}}

with
    source as (
        select s.* from {{ ref("stg_core_pending_credit_facility_events_rollup") }} as s
    ),

    transformed as (
        select
            created_at,
            modified_at,
            account_ids,
            amount,
            approval_process_id,
            collateral,
            collateral_id,
            collateralization_ratio,
            collateralization_state,
            credit_facility_proposal_id,
            customer_id,
            customer_type,
            disbursal_credit_account_id,
            price,
            terms,
            ledger_tx_ids,
            is_collateralization_ratio_changed,
            is_collateralization_state_changed,
            is_completed,
            loaded_to_dw_at,
            pending_credit_facility_id,
            version
        from source
    ),

    final as (select * from transformed)

select *
from final
