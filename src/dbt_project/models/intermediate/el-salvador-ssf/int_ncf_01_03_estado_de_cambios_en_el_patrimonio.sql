with

    config as (select * from {{ ref("static_ncf_01_03_account_config") }}),

    chart as (select * from {{ ref("int_core_chart_of_account_with_balances") }}),

    joined as (
        select order_by, title, column_title, sum(coalesce(balance, 0)) as balance
        from config
        left join chart on code = any(source_account_codes)
        group by order_by, title, column_title
    ),

    -- PostgreSQL pivot using conditional aggregation
    final as (
        select 
            order_by,
            title,
            sum(case when column_title = 'Capital Social' then balance end) as "Capital Social",
            sum(case when column_title = 'Reservas de Capital' then balance end) as "Reservas de Capital",
            sum(case when column_title = 'Otras Reservas' then balance end) as "Otras Reservas",
            sum(case when column_title = 'Resultados por Aplicar' then balance end) as "Resultados por Aplicar",
            sum(case when column_title = 'Utilidades no Distribuibles' then balance end) as "Utilidades no Distribuibles",
            sum(case when column_title = 'Donaciones' then balance end) as "Donaciones",
            sum(case when column_title = 'Otro Resultado Integral Ejercicios Anteriores' then balance end) as "Otro Resultado Integral Ejercicios Anteriores",
            sum(case when column_title = 'Otro Resultado Integral del Ejercicio' then balance end) as "Otro Resultado Integral del Ejercicio",
            sum(case when column_title = 'Participaciones accionistas no controladores' then balance end) as "Participaciones accionistas no controladores",
            sum(case when column_title = 'Patrimonio Total' then balance end) as "Patrimonio Total"
        from joined
        group by order_by, title
    )

select 
    title,
    "Capital Social",
    "Reservas de Capital",
    "Otras Reservas",
    "Resultados por Aplicar",
    "Utilidades no Distribuibles",
    "Donaciones",
    "Otro Resultado Integral Ejercicios Anteriores",
    "Otro Resultado Integral del Ejercicio",
    "Participaciones accionistas no controladores",
    "Patrimonio Total"
from final
order by order_by
