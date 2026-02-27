with
    raw_stg_sumsub_applicants as (
        select * from {{ source("sumsub", "sumsub_applicants_dlt") }}
    ),

    ordered as (
        select
            customer_id,
            recorded_at,
            content,
            row_number() over (
                partition by customer_id order by recorded_at desc
            ) as order_recorded_at_desc,
            to_timestamp(_dlt_load_id::decimal) as loaded_to_dw_at

        from raw_stg_sumsub_applicants
    )

select 
    customer_id,
    recorded_at,
    content,
    loaded_to_dw_at,
    content::jsonb as parsed_content

from ordered

where order_recorded_at_desc = 1
