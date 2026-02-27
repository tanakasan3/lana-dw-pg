{{
    config(
        unique_key=["id", "version"],
    )
}}

with
    raw_stg_core_customer_events_rollup as (
        select
            id,
            version,
            created_at,
            modified_at,
            activity,
            applicant_id,
            customer_type,
            email,
            kyc_verification,
            level,
            public_id,
            telegram_handle,
            is_kyc_approved,
            _dlt_load_id,
            _dlt_id
        from {{ source("lana", "core_customer_events_rollup") }}
    )
select
    id as customer_id,
    version,
    created_at,
    modified_at,
    activity,
    applicant_id,
    customer_type,
    email,
    kyc_verification,
    level,
    public_id,
    telegram_handle,
    is_kyc_approved,
    to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at
from raw_stg_core_customer_events_rollup
