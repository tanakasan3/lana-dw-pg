with

    config as (
        select
            cfg.order_by,
            ac.account_code,
            cfg.title,
            cfg.eng_title,
            cfg.account_name,
            cfg.eng_account_name,
            coalesce(lc.coefficient, 1) as coefficient
        from {{ ref("static_nrp_28_01_account_config") }} cfg
        cross join lateral unnest(cfg.sum_account_codes) as ac(account_code)
        left join {{ ref("static_nrp_28_01_liquidity_coefficients") }} lc 
            on lc.account_code = ac.account_code

        union all

        select
            cfg.order_by,
            ac.account_code,
            cfg.title,
            cfg.eng_title,
            cfg.account_name,
            cfg.eng_account_name,
            -1 * coalesce(lc.coefficient, 1) as coefficient
        from {{ ref("static_nrp_28_01_account_config") }} cfg
        cross join lateral unnest(cfg.diff_account_codes) as ac(account_code)
        left join {{ ref("static_nrp_28_01_liquidity_coefficients") }} lc 
            on lc.account_code = ac.account_code
    ),

    chart as (select * from {{ ref("int_core_chart_of_account_with_balances") }}),

    final as (
        select 
            config.*,
            chart.code,
            chart.dotted_code,
            chart.spaced_code,
            chart.node_name,
            chart.account_set_id,
            coalesce(chart.balance, 0) as balance
        from config
        left join chart on chart.code = config.account_code
    )

select *
from final
