{{
    config(
        unique_key=["credit_facility_proposal_id", "version"],
    )
}}

with
    source as (
        select s.*
        from {{ ref("stg_core_credit_facility_proposal_events_rollup") }} as s
    ),

    transformed as (
        select
            created_at,
            modified_at,
            amount,
            approval_process_id,
            custodian_id,
            customer_id,
            customer_type,
            disbursal_credit_account_id,
            status,
            terms,
            is_approval_process_concluded,
            loaded_to_dw_at,
            credit_facility_proposal_id,
            version
        from source
    ),

    final as (select * from transformed)

select *
from final
