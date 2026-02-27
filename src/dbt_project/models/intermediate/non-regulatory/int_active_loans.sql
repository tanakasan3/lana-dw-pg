with

    active_cf as (
        select
            credit_facility_id,
            version,
            proposal_version,
            customer_id,
            facility_amount_usd,
            annual_rate,
            one_time_fee_rate,
            initial_cvl,
            liquidation_cvl,
            margin_call_cvl,
            duration_value,
            duration_type,
            accrual_interval,
            accrual_cycle_interval,
            collateral_amount_sats,
            collateral_amount_btc,
            price_usd_per_btc,
            collateral_amount_usd,
            collateralization_state,
            approved,
            is_approval_process_concluded,
            is_activated,
            credit_facility_activated_at,
            is_completed,
            interest_period_start_at,
            interest_period_end_at,
            interest_period_interval_type,
            outstanding_interest_usd,
            outstanding_disbursed_usd,
            interest_due_duration_from_accrual_value,
            interest_due_duration_from_accrual_type,
            obligation_overdue_duration_from_due_value,
            obligation_overdue_duration_from_due_type,
            obligation_liquidation_duration_from_due_value,
            obligation_liquidation_duration_from_due_type,
            credit_facility_created_at,
            credit_facility_modified_at,
            facility_account_id,
            collateral_account_id,
            fee_income_account_id,
            interest_income_account_id,
            interest_defaulted_account_id,
            disbursed_defaulted_account_id,
            interest_receivable_due_account_id,
            disbursed_receivable_due_account_id,
            interest_receivable_overdue_account_id,
            disbursed_receivable_overdue_account_id,
            interest_receivable_not_yet_due_account_id,
            disbursed_receivable_not_yet_due_account_id,
            event_type,
            collateralization_ratio,
            customer_type,
            maturity_date,
            public_id,
            structuring_fee_tx_id,
            is_matured,
            loaded_to_dw_at,
            current_facility_cvl,
            credit_facility_maturity_at
        from {{ ref("int_core_credit_facility_events_rollup") }}
        where not is_matured
    ),

    disbursals as (
        select
            credit_facility_id,
            sum(amount_usd) as total_disbursed_usd,
            count(*) as number_disbursals
        from {{ ref("int_core_disbursal_events_rollup") }}
        group by credit_facility_id
    ),

    final as (
        select
            active_cf.*,
            total_disbursed_usd,
            number_disbursals
        from active_cf
        left join disbursals using (credit_facility_id)

    )

select *
from final
