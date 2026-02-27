{{
    config(
        unique_key="requested_at",
    )
}}

with
    raw_bitfinex_order_book as (
        select * from {{ source("bitfinex", "bitfinex_order_book_dlt") }}
    )
select
    *,
    to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at
from raw_bitfinex_order_book
