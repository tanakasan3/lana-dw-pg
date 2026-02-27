{{
    config(
        unique_key=["id"],
    )
}}

with
    raw_stg_cala_account_sets as (
        select * from {{ source("lana", "cala_account_sets") }}
    ),

    ordered as (

        select
            id,
            journal_id,
            name as set_name,
            created_at,
            to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at,
            row_number() over (
                partition by id order by _dlt_load_id desc
            ) as order_received_desc

        from raw_stg_cala_account_sets
    )

select 
    id,
    journal_id,
    set_name,
    created_at,
    loaded_to_dw_at

from ordered

where order_received_desc = 1
