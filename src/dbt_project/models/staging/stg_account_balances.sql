{{
    config(
        unique_key=["journal_id", "account_id", "currency", "version"],
    )
}}

with
    raw_stg_cala_balance_history as (
        select * from {{ source("lana", "cala_balance_history") }}
    ),

    ordered as (

        select journal_id, account_id, currency, version, recorded_at,
        values
            ,
            to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at,
            row_number() over (
                partition by account_id order by _dlt_load_id desc
            ) as order_received_desc

        from raw_stg_cala_balance_history

    )

select 
    journal_id, 
    account_id, 
    currency, 
    version, 
    recorded_at,
    values,
    loaded_to_dw_at

from ordered

where order_received_desc = 1
