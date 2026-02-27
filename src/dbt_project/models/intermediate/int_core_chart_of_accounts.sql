with
    nodes as (
        select
            (event::jsonb)->>'chart_id' as chart_id,
            (
                select coalesce(array_agg(elem::text), ARRAY[]::text[])
                from jsonb_array_elements(
                    (event::jsonb)->'spec'->'code'->'sections'->'code'
                ) as elem
            ) as code_array,
            (event::jsonb)->'spec'->'name'->>'name' as node_name,
            (event::jsonb)->>'ledger_account_set_id' as account_set_id
        from {{ ref("stg_core_chart_node_events") }}
        where
            loaded_to_dw_at >= (
                select coalesce(max(loaded_to_dw_at), '1900-01-01'::timestamp)
                from {{ ref("stg_core_chart_node_events") }}
                where event_type = 'initialized'
            )
            and event_type = 'initialized'
    )

select
    chart_id,
    array_to_string(code_array, '') as code,
    array_to_string(code_array, '.') as dotted_code,
    array_to_string(code_array, ' ') as spaced_code,
    node_name,
    account_set_id
from nodes
