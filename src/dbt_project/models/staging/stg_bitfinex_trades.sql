{{
    config(
        unique_key="ID",
    )
}}

with
    raw_bitfinex_trades as (
        select * from {{ source("bitfinex", "bitfinex_trades_dlt") }}
    )
select
    *,
    to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at
from raw_bitfinex_trades
