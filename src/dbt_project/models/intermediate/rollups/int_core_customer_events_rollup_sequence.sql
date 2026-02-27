{{
    config(
        unique_key=["customer_id", "version"],
    )
}}


with
    source as (select s.* from {{ ref("stg_core_customer_events_rollup") }} as s),

    transformed as (
        select
            version,
            activity,
            applicant_id,
            customer_type,
            email,
            kyc_verification,
            level,
            public_id,
            telegram_handle,
            is_kyc_approved,
            loaded_to_dw_at,
            customer_id,
            created_at as customer_created_at,
            modified_at as customer_modified_at
        from source
    )

select *
from transformed
