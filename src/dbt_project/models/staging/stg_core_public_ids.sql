{{
    config(
        unique_key=["target_id"],
    )
}}

with
    raw_stg_core_public_ids as (
        select id, target_id, created_at, _dlt_load_id, _dlt_id
        from {{ source("lana", "core_public_ids") }}
    )
select
    id,
    target_id,
    created_at,
    to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at
from raw_stg_core_public_ids
