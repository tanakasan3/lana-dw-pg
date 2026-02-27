{{
    config(
        unique_key="requested_at",
    )
}}

with
    raw_bitfinex_ticker as (
        select * from {{ source("bitfinex", "bitfinex_ticker_dlt") }}
    )
select
    bid,
    bid_size,
    ask,
    ask_size,
    daily_change,
    daily_change_relative,
    volume,
    high,
    low,
    symbol,
    requested_at,
    _dlt_load_id,
    _dlt_id,
    last_price as last_price_usd,
    to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at
from raw_bitfinex_ticker
