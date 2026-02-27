with latest_balances as (
    select 
        values,
        (values::jsonb)->'account_id' as account_id,
        (values::jsonb)->>'currency' as currency,
        row_number() over (
            partition by (values::jsonb)->>'account_id', (values::jsonb)->>'currency'
            order by recorded_at desc
        ) as rn
    from {{ ref("stg_account_balances") }}
    where
        loaded_to_dw_at >= (
            select coalesce(max(loaded_to_dw_at), '1900-01-01'::timestamp)
            from {{ ref("stg_core_chart_node_events") }}
            where event_type = 'initialized'
        )
)

select
    cast(((values::jsonb)->'settled'->>'cr_balance') as numeric) as settled_cr,
    cast(((values::jsonb)->'settled'->>'dr_balance') as numeric) as settled_dr,
    account_id::text as account_id,
    currency

from latest_balances
where rn = 1
