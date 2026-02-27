with
    loans as (
        select
            *,
            disbursal_start_date as start_date,
            disbursal_end_date as end_date,
            'actual/360' as day_count_convention,
            annual_rate / 100.0 as annual_interest_rate

        from {{ ref("int_approved_credit_facility_loans") }}
    ),

    projections as (
        select
            *,
            annual_interest_rate / nullif(
                case
                    when day_count_convention like '%/360'
                    then 360.0
                    when day_count_convention like '%/365'
                    then 365.0
                    else
                        extract(day from (
                            (date_trunc('year', start_date::date) + interval '1 year' - interval '1 day')::timestamp
                            - date_trunc('year', start_date::date)::timestamp
                        ))
                end, 0
            ) as daily_interest_rate,
            case
                when day_count_convention like '%/360'
                then 360.0
                when day_count_convention like '%/365'
                then 365.0
                else
                    extract(day from (
                        (date_trunc('year', start_date::date) + interval '1 year' - interval '1 day')::timestamp
                        - date_trunc('year', start_date::date)::timestamp
                    ))
            end as days_per_year
        from loans
    ),

    -- Generate date series for interest schedule
    projected_interest_cash_flow_data as (
        select
            p.credit_facility_id,
            p.disbursal_id,
            p.customer_id,
            p.facility_initialized_recorded_at,
            p.disbursal_initialized_recorded_at,
            p.disbursal_settled_recorded_at,
            p.start_date,
            p.end_date,
            p.duration_value,
            p.duration_type,
            p.annual_rate,
            p.accrual_interval,
            p.accrual_cycle_interval,
            p.facility_amount_usd,
            p.total_disbursed_usd,
            p.matured,
            p.day_count_convention,
            p.annual_interest_rate,
            p.daily_interest_rate,
            p.days_per_year,
            case
                when date_trunc('month', projected_month)::timestamp < p.start_date
                then p.start_date::timestamp
                else date_trunc('month', projected_month)::timestamp
            end as period_start_date,
            case
                when (date_trunc('month', projected_month) + interval '1 month' - interval '1 day')::date > p.end_date::date
                then p.end_date::timestamp
                else (date_trunc('month', projected_month) + interval '1 month' - interval '1 day')::timestamp
            end as period_end_date,
            'projected_interest_cash_flow' as cash_flow_type
        from projections as p
        cross join lateral generate_series(
            p.start_date::date,
            (date_trunc('month', p.end_date::date) + interval '1 month' - interval '1 day')::date,
            case 
                when p.accrual_cycle_interval = 'end_of_day' then interval '1 day'
                else interval '1 month'
            end
        ) as projected_month
    ),

    projected_principal_cash_flow_data as (
        select
            credit_facility_id,
            disbursal_id,
            customer_id,
            facility_initialized_recorded_at,
            disbursal_initialized_recorded_at,
            disbursal_settled_recorded_at,
            start_date,
            end_date,
            duration_value,
            duration_type,
            annual_rate,
            accrual_interval,
            accrual_cycle_interval,
            facility_amount_usd,
            total_disbursed_usd,
            matured,
            day_count_convention,
            annual_interest_rate,
            daily_interest_rate,
            days_per_year,
            start_date::timestamp as period_start_date,
            end_date::timestamp as period_end_date,
            'projected_principal_cash_flow' as cash_flow_type
        from projections
    ),

    projected_cash_flow_data as (
        select *
        from projected_interest_cash_flow_data
        union all
        select *
        from projected_principal_cash_flow_data
    ),

    projected_time_data as (
        select
            *,
            extract(day from (period_end_date::date - period_start_date::date))::integer + 1 as days_in_period
        from projected_cash_flow_data
    ),

    projected_cash_flows as (
        select
            *,
            case
                when cash_flow_type = 'projected_interest_cash_flow'
                then total_disbursed_usd * daily_interest_rate * days_in_period
                when cash_flow_type = 'projected_principal_cash_flow'
                then total_disbursed_usd
                else 0
            end as cash_flow_amount
        from projected_time_data
    ),

    final as (

        select
            credit_facility_id,
            disbursal_id,
            customer_id,
            facility_initialized_recorded_at,
            disbursal_initialized_recorded_at,
            disbursal_settled_recorded_at,
            start_date,
            end_date,
            duration_value,
            duration_type,
            annual_rate,
            accrual_interval,
            accrual_cycle_interval,
            facility_amount_usd,
            total_disbursed_usd,
            matured,
            day_count_convention,
            annual_interest_rate,
            daily_interest_rate,
            days_per_year,
            period_start_date,
            period_end_date,
            days_in_period,
            cash_flow_type,
            cash_flow_amount
        from projected_cash_flows
    )

select *
from final
