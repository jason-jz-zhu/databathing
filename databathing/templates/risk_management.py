"""
Risk Management Templates  
Pre-built SQL patterns for credit risk, market risk, and operational risk calculations
"""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class RiskConfig:
    """Configuration for risk management calculations"""
    customer_table: str = "customers"
    account_table: str = "accounts"
    transaction_table: str = "transactions"
    credit_bureau_table: str = "credit_bureau_data"
    loan_table: str = "loans"
    market_data_table: str = "market_data"
    portfolio_table: str = "portfolio_positions"
    enable_stress_testing: bool = True


class RiskManagementTemplates:
    """Templates for risk management calculations and monitoring"""
    
    @staticmethod
    def credit_risk_scoring(config: RiskConfig) -> str:
        """Generate comprehensive credit risk scores"""
        return f"""
WITH customer_credit_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.date_of_birth,
        c.customer_since_date,
        datediff(current_date(), c.customer_since_date) as relationship_tenure_days,
        -- Credit Bureau Data
        cb.credit_score,
        cb.payment_history_score,
        cb.credit_utilization_ratio,
        cb.length_of_credit_history,
        cb.credit_mix_score,
        cb.new_credit_score,
        cb.total_debt,
        cb.total_available_credit,
        -- Banking Relationship Metrics
        count(distinct a.account_id) as total_accounts,
        sum(a.current_balance) as total_balance,
        avg(a.average_monthly_balance) as avg_monthly_balance,
        -- Transaction Behavior
        count(case when t.transaction_type = 'OVERDRAFT' then 1 end) as overdraft_count_12m,
        count(case when t.transaction_type = 'NSF' then 1 end) as nsf_count_12m,
        avg(t.daily_balance) as avg_daily_balance,
        stddev(t.daily_balance) as balance_volatility,
        -- Loan Performance
        count(distinct l.loan_id) as active_loans,
        sum(l.outstanding_balance) as total_loan_balance,
        sum(case when l.days_past_due > 30 then 1 else 0 end) as loans_past_due_30,
        sum(case when l.days_past_due > 90 then 1 else 0 end) as loans_past_due_90,
        avg(l.payment_history_12m) as avg_payment_performance
    FROM {config.customer_table} c
    LEFT JOIN {config.credit_bureau_table} cb ON c.customer_id = cb.customer_id
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.transaction_table} t ON a.account_id = t.account_id
    LEFT JOIN {config.loan_table} l ON c.customer_id = l.customer_id
    WHERE t.transaction_date >= date_sub(current_date(), 365) -- Last 12 months
      AND cb.report_date = (SELECT max(report_date) FROM {config.credit_bureau_table} cb2 WHERE cb2.customer_id = cb.customer_id)
    GROUP BY c.customer_id, c.first_name, c.last_name, c.date_of_birth, c.customer_since_date,
             cb.credit_score, cb.payment_history_score, cb.credit_utilization_ratio,
             cb.length_of_credit_history, cb.credit_mix_score, cb.new_credit_score,
             cb.total_debt, cb.total_available_credit
),
risk_scoring AS (
    SELECT 
        *,
        -- Credit Score Component (40% weight)
        case 
            when credit_score >= 750 then 40
            when credit_score >= 700 then 35
            when credit_score >= 650 then 25
            when credit_score >= 600 then 15
            when credit_score >= 550 then 8
            else 0
        end as credit_score_points,
        
        -- Banking Relationship Component (25% weight)
        least(25, greatest(0,
            (case when relationship_tenure_days > 1825 then 8 -- 5+ years
                  when relationship_tenure_days > 1095 then 6 -- 3+ years  
                  when relationship_tenure_days > 365 then 4  -- 1+ years
                  else 2 end) +
            (case when total_accounts > 3 then 5 
                  when total_accounts > 1 then 3 else 1 end) +
            (case when avg_monthly_balance > 10000 then 8
                  when avg_monthly_balance > 5000 then 6
                  when avg_monthly_balance > 1000 then 4 
                  when avg_monthly_balance > 0 then 2 else 0 end) +
            (case when balance_volatility < 1000 then 4 
                  when balance_volatility < 5000 then 2 else 0 end)
        )) as banking_relationship_points,
        
        -- Payment Behavior Component (20% weight) 
        least(20, greatest(0,
            20 - (overdraft_count_12m * 2) - (nsf_count_12m * 3) - 
            (loans_past_due_30 * 5) - (loans_past_due_90 * 10) +
            (case when avg_payment_performance >= 0.95 then 5
                  when avg_payment_performance >= 0.90 then 3
                  when avg_payment_performance >= 0.80 then 1 else 0 end)
        )) as payment_behavior_points,
        
        -- Debt-to-Income Proxy Component (15% weight)
        case 
            when total_debt / nullif(avg_monthly_balance * 12, 0) <= 0.20 then 15
            when total_debt / nullif(avg_monthly_balance * 12, 0) <= 0.35 then 12
            when total_debt / nullif(avg_monthly_balance * 12, 0) <= 0.50 then 8
            when total_debt / nullif(avg_monthly_balance * 12, 0) <= 0.75 then 4
            else 0
        end as debt_income_points
    FROM customer_credit_metrics
)
SELECT 
    *,
    -- Total Risk Score (0-100)
    credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points as total_risk_score,
    
    -- Risk Rating
    case 
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 85 then 'EXCELLENT'
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 70 then 'GOOD'
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 55 then 'FAIR'
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 40 then 'POOR'
        else 'HIGH_RISK'
    end as risk_rating,
    
    -- Probability of Default (simplified model)
    case 
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 85 then 0.01
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 70 then 0.03
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 55 then 0.08
        when credit_score_points + banking_relationship_points + payment_behavior_points + debt_income_points >= 40 then 0.15
        else 0.30
    end as probability_of_default,
    
    current_timestamp() as risk_assessment_date
FROM risk_scoring
ORDER BY total_risk_score DESC
"""

    @staticmethod
    def portfolio_risk_analysis(config: RiskConfig) -> str:
        """Analyze portfolio-level risk concentrations and exposures"""
        return f"""
WITH portfolio_exposures AS (
    SELECT 
        p.portfolio_id,
        p.portfolio_name,
        p.portfolio_type,
        -- Concentration Metrics
        count(distinct c.customer_id) as total_customers,
        sum(l.outstanding_balance) as total_exposure,
        avg(l.outstanding_balance) as avg_loan_size,
        max(l.outstanding_balance) as max_single_exposure,
        -- Geographic Concentration
        count(distinct c.state) as states_represented,
        count(distinct c.zip_code) as zip_codes_represented,
        -- Industry Concentration (if applicable)
        count(distinct c.industry_code) as industries_represented,
        -- Risk Distribution
        sum(case when l.risk_rating = 'HIGH_RISK' then l.outstanding_balance else 0 end) as high_risk_exposure,
        sum(case when l.risk_rating = 'MEDIUM_RISK' then l.outstanding_balance else 0 end) as medium_risk_exposure,
        sum(case when l.risk_rating = 'LOW_RISK' then l.outstanding_balance else 0 end) as low_risk_exposure,
        -- Performance Metrics
        sum(case when l.days_past_due > 30 then l.outstanding_balance else 0 end) as past_due_30_exposure,
        sum(case when l.days_past_due > 90 then l.outstanding_balance else 0 end) as past_due_90_exposure,
        sum(case when l.loan_status = 'CHARGED_OFF' then l.original_balance else 0 end) as charged_off_amount,
        -- Vintage Analysis
        sum(case when l.origination_date >= date_sub(current_date(), 365) then l.outstanding_balance else 0 end) as new_originations_12m,
        avg(datediff(current_date(), l.origination_date)) as avg_loan_age_days
    FROM {config.portfolio_table} p
    LEFT JOIN {config.loan_table} l ON p.portfolio_id = l.portfolio_id
    LEFT JOIN {config.customer_table} c ON l.customer_id = c.customer_id
    WHERE l.loan_status in ('ACTIVE', 'PAST_DUE', 'CHARGED_OFF')
    GROUP BY p.portfolio_id, p.portfolio_name, p.portfolio_type
),
risk_metrics AS (
    SELECT 
        *,
        -- Concentration Risk Metrics
        case 
            when max_single_exposure / nullif(total_exposure, 0) > 0.10 then 'HIGH'
            when max_single_exposure / nullif(total_exposure, 0) > 0.05 then 'MEDIUM'
            else 'LOW'
        end as single_name_concentration_risk,
        
        -- Geographic Diversification Score
        case 
            when states_represented >= 10 and zip_codes_represented >= 100 then 'WELL_DIVERSIFIED'
            when states_represented >= 5 and zip_codes_represented >= 50 then 'MODERATELY_DIVERSIFIED'
            else 'CONCENTRATED'
        end as geographic_diversification,
        
        -- Risk Rating Distribution
        high_risk_exposure / nullif(total_exposure, 0) as high_risk_percentage,
        medium_risk_exposure / nullif(total_exposure, 0) as medium_risk_percentage, 
        low_risk_exposure / nullif(total_exposure, 0) as low_risk_percentage,
        
        -- Delinquency Rates
        past_due_30_exposure / nullif(total_exposure, 0) as delinquency_rate_30,
        past_due_90_exposure / nullif(total_exposure, 0) as delinquency_rate_90,
        charged_off_amount / nullif(total_exposure + charged_off_amount, 0) as charge_off_rate,
        
        -- Portfolio Health Score (0-100)
        greatest(0, least(100,
            100 - 
            (high_risk_exposure / nullif(total_exposure, 0) * 30) -
            (past_due_30_exposure / nullif(total_exposure, 0) * 25) -
            (past_due_90_exposure / nullif(total_exposure, 0) * 35) -
            (case when max_single_exposure / nullif(total_exposure, 0) > 0.10 then 10 else 0 end)
        )) as portfolio_health_score
    FROM portfolio_exposures
)
SELECT 
    *,
    -- Overall Risk Rating
    case 
        when portfolio_health_score >= 85 then 'LOW_RISK'
        when portfolio_health_score >= 70 then 'MEDIUM_RISK'  
        when portfolio_health_score >= 55 then 'HIGH_RISK'
        else 'CRITICAL_RISK'
    end as overall_portfolio_risk,
    
    current_timestamp() as risk_analysis_date
FROM risk_metrics
ORDER BY portfolio_health_score ASC
"""

    @staticmethod
    def stress_testing_scenarios(config: RiskConfig) -> str:
        """Run stress testing scenarios on loan portfolios"""
        if not config.enable_stress_testing:
            return "-- Stress testing disabled in configuration"
            
        return f"""
WITH base_portfolio AS (
    SELECT 
        l.loan_id,
        l.customer_id,
        l.outstanding_balance,
        l.interest_rate,
        l.loan_term_months,
        l.monthly_payment,
        l.ltv_ratio,
        l.risk_rating,
        c.credit_score,
        c.debt_to_income_ratio,
        c.employment_status,
        c.industry_code,
        c.state as customer_state
    FROM {config.loan_table} l
    JOIN {config.customer_table} c ON l.customer_id = c.customer_id
    WHERE l.loan_status = 'ACTIVE'
),
stress_scenarios AS (
    SELECT 
        'BASELINE' as scenario_name,
        loan_id, customer_id, outstanding_balance, interest_rate, 
        risk_rating, credit_score,
        -- Baseline PD rates by risk rating
        case risk_rating
            when 'LOW_RISK' then 0.02
            when 'MEDIUM_RISK' then 0.05  
            when 'HIGH_RISK' then 0.12
            else 0.20
        end as probability_of_default,
        -- Baseline LGD rates
        case 
            when ltv_ratio <= 0.80 then 0.25
            when ltv_ratio <= 0.90 then 0.35
            else 0.45
        end as loss_given_default
    FROM base_portfolio
    
    UNION ALL
    
    SELECT 
        'MILD_RECESSION' as scenario_name,
        loan_id, customer_id, outstanding_balance, interest_rate,
        risk_rating, credit_score,
        -- Increased PD rates for mild recession (+50% stress)
        case risk_rating
            when 'LOW_RISK' then 0.02 * 1.5
            when 'MEDIUM_RISK' then 0.05 * 1.5
            when 'HIGH_RISK' then 0.12 * 1.5  
            else 0.20 * 1.5
        end as probability_of_default,
        -- Slightly higher LGD due to asset price decline
        case 
            when ltv_ratio <= 0.80 then 0.30
            when ltv_ratio <= 0.90 then 0.40
            else 0.50
        end as loss_given_default
    FROM base_portfolio
    
    UNION ALL
    
    SELECT 
        'SEVERE_RECESSION' as scenario_name,
        loan_id, customer_id, outstanding_balance, interest_rate,
        risk_rating, credit_score,
        -- Severely increased PD rates (+200% stress)  
        case risk_rating
            when 'LOW_RISK' then 0.02 * 3.0
            when 'MEDIUM_RISK' then 0.05 * 3.0
            when 'HIGH_RISK' then 0.12 * 3.0
            else 0.20 * 3.0
        end as probability_of_default,
        -- Much higher LGD due to severe asset price decline
        case 
            when ltv_ratio <= 0.80 then 0.40  
            when ltv_ratio <= 0.90 then 0.55
            else 0.70
        end as loss_given_default
    FROM base_portfolio
),
expected_losses AS (
    SELECT 
        scenario_name,
        count(*) as total_loans,
        sum(outstanding_balance) as total_exposure,
        sum(outstanding_balance * probability_of_default) as total_expected_defaults,
        sum(outstanding_balance * probability_of_default * loss_given_default) as total_expected_loss,
        avg(probability_of_default) as avg_probability_of_default,
        avg(loss_given_default) as avg_loss_given_default,
        -- Risk Rating Breakdown
        sum(case when risk_rating = 'LOW_RISK' then outstanding_balance else 0 end) as low_risk_exposure,
        sum(case when risk_rating = 'MEDIUM_RISK' then outstanding_balance else 0 end) as medium_risk_exposure,
        sum(case when risk_rating = 'HIGH_RISK' then outstanding_balance else 0 end) as high_risk_exposure,
        sum(case when risk_rating = 'LOW_RISK' then outstanding_balance * probability_of_default * loss_given_default else 0 end) as low_risk_expected_loss,
        sum(case when risk_rating = 'MEDIUM_RISK' then outstanding_balance * probability_of_default * loss_given_default else 0 end) as medium_risk_expected_loss,
        sum(case when risk_rating = 'HIGH_RISK' then outstanding_balance * probability_of_default * loss_given_default else 0 end) as high_risk_expected_loss
    FROM stress_scenarios
    GROUP BY scenario_name
)
SELECT 
    scenario_name,
    total_loans,
    total_exposure,
    total_expected_defaults,
    total_expected_loss,
    total_expected_loss / nullif(total_exposure, 0) as expected_loss_rate,
    avg_probability_of_default,
    avg_loss_given_default,
    -- Risk Rating Analysis
    low_risk_exposure,
    medium_risk_exposure, 
    high_risk_exposure,
    low_risk_expected_loss,
    medium_risk_expected_loss,
    high_risk_expected_loss,
    low_risk_expected_loss / nullif(low_risk_exposure, 0) as low_risk_loss_rate,
    medium_risk_expected_loss / nullif(medium_risk_exposure, 0) as medium_risk_loss_rate,
    high_risk_expected_loss / nullif(high_risk_exposure, 0) as high_risk_loss_rate,
    current_timestamp() as stress_test_date
FROM expected_losses
ORDER BY 
    case scenario_name
        when 'BASELINE' then 1
        when 'MILD_RECESSION' then 2  
        when 'SEVERE_RECESSION' then 3
        else 4
    end
"""

    @staticmethod
    def early_warning_system(config: RiskConfig) -> str:
        """Early warning system for identifying deteriorating credit quality"""
        return f"""
WITH customer_risk_indicators AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        l.loan_id,
        l.outstanding_balance,
        l.risk_rating as current_risk_rating,
        -- Payment Behavior Deterioration
        count(case when t.days_past_due between 1 and 29 then 1 end) as late_payments_30d,
        count(case when t.days_past_due >= 30 then 1 end) as late_payments_30plus,
        avg(case when t.transaction_date >= date_sub(current_date(), 90) 
                 then t.payment_amount end) as recent_avg_payment,
        avg(case when t.transaction_date between date_sub(current_date(), 180) and date_sub(current_date(), 90)
                 then t.payment_amount end) as previous_avg_payment,
        -- Balance Trends
        avg(case when a.balance_date >= date_sub(current_date(), 30) 
                 then a.daily_balance end) as recent_avg_balance,
        avg(case when a.balance_date between date_sub(current_date(), 90) and date_sub(current_date(), 60)
                 then a.daily_balance end) as previous_avg_balance,
        count(case when a.daily_balance < 0 then 1 end) as negative_balance_days,
        -- Credit Utilization Changes
        cb_current.credit_utilization_ratio as current_utilization,
        cb_previous.credit_utilization_ratio as previous_utilization,
        cb_current.credit_score as current_credit_score,
        cb_previous.credit_score as previous_credit_score,
        -- Employment/Income Indicators
        c.employment_status,
        c.last_income_verification_date,
        datediff(current_date(), c.last_income_verification_date) as days_since_income_verification
    FROM {config.customer_table} c
    LEFT JOIN {config.loan_table} l ON c.customer_id = l.customer_id
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.transaction_table} t ON l.loan_id = t.loan_id
    LEFT JOIN {config.credit_bureau_table} cb_current ON c.customer_id = cb_current.customer_id 
        AND cb_current.report_date = (SELECT max(report_date) FROM {config.credit_bureau_table} cb_max WHERE cb_max.customer_id = c.customer_id)
    LEFT JOIN {config.credit_bureau_table} cb_previous ON c.customer_id = cb_previous.customer_id
        AND cb_previous.report_date = (SELECT max(report_date) FROM {config.credit_bureau_table} cb_prev 
                                     WHERE cb_prev.customer_id = c.customer_id 
                                     AND cb_prev.report_date < cb_current.report_date)
    WHERE l.loan_status = 'ACTIVE'
      AND t.transaction_date >= date_sub(current_date(), 180)
    GROUP BY c.customer_id, c.first_name, c.last_name, l.loan_id, l.outstanding_balance, 
             l.risk_rating, c.employment_status, c.last_income_verification_date,
             cb_current.credit_utilization_ratio, cb_previous.credit_utilization_ratio,
             cb_current.credit_score, cb_previous.credit_score
),
risk_flags AS (
    SELECT 
        *,
        -- Payment Behavior Flags
        case when late_payments_30d > 1 then 1 else 0 end as frequent_late_payments_flag,
        case when late_payments_30plus > 0 then 1 else 0 end as past_due_30_flag,
        case when recent_avg_payment < previous_avg_payment * 0.8 then 1 else 0 end as payment_reduction_flag,
        -- Balance Behavior Flags  
        case when recent_avg_balance < previous_avg_balance * 0.7 then 1 else 0 end as balance_decline_flag,
        case when negative_balance_days > 5 then 1 else 0 end as frequent_overdrafts_flag,
        -- Credit Profile Flags
        case when current_utilization > previous_utilization + 0.2 then 1 else 0 end as utilization_spike_flag,
        case when current_credit_score < previous_credit_score - 30 then 1 else 0 end as credit_score_drop_flag,
        -- Employment/Income Flags
        case when employment_status in ('UNEMPLOYED', 'UNKNOWN') then 1 else 0 end as employment_risk_flag,
        case when days_since_income_verification > 365 then 1 else 0 end as stale_income_flag
    FROM customer_risk_indicators
)
SELECT 
    customer_id,
    first_name,
    last_name, 
    loan_id,
    outstanding_balance,
    current_risk_rating,
    -- Risk Flags Summary
    frequent_late_payments_flag + past_due_30_flag + payment_reduction_flag + 
    balance_decline_flag + frequent_overdrafts_flag + utilization_spike_flag +
    credit_score_drop_flag + employment_risk_flag + stale_income_flag as total_risk_flags,
    -- Individual Flags for Analysis
    frequent_late_payments_flag,
    past_due_30_flag, 
    payment_reduction_flag,
    balance_decline_flag,
    frequent_overdrafts_flag,
    utilization_spike_flag,
    credit_score_drop_flag,
    employment_risk_flag,
    stale_income_flag,
    -- Warning Level
    case 
        when frequent_late_payments_flag + past_due_30_flag + payment_reduction_flag + 
             balance_decline_flag + frequent_overdrafts_flag + utilization_spike_flag +
             credit_score_drop_flag + employment_risk_flag + stale_income_flag >= 4 then 'HIGH'
        when frequent_late_payments_flag + past_due_30_flag + payment_reduction_flag + 
             balance_decline_flag + frequent_overdrafts_flag + utilization_spike_flag +
             credit_score_drop_flag + employment_risk_flag + stale_income_flag >= 2 then 'MEDIUM'
        when frequent_late_payments_flag + past_due_30_flag + payment_reduction_flag + 
             balance_decline_flag + frequent_overdrafts_flag + utilization_spike_flag +
             credit_score_drop_flag + employment_risk_flag + stale_income_flag >= 1 then 'LOW'
        else 'NONE'
    end as warning_level,
    current_timestamp() as early_warning_date
FROM risk_flags
WHERE frequent_late_payments_flag + past_due_30_flag + payment_reduction_flag + 
      balance_decline_flag + frequent_overdrafts_flag + utilization_spike_flag +
      credit_score_drop_flag + employment_risk_flag + stale_income_flag > 0
ORDER BY total_risk_flags DESC, outstanding_balance DESC
"""

    @staticmethod
    def get_all_templates() -> Dict[str, str]:
        """Get all risk management templates"""
        config = RiskConfig()
        
        return {
            "credit_risk_scoring": RiskManagementTemplates.credit_risk_scoring(config),
            "portfolio_risk_analysis": RiskManagementTemplates.portfolio_risk_analysis(config), 
            "stress_testing_scenarios": RiskManagementTemplates.stress_testing_scenarios(config),
            "early_warning_system": RiskManagementTemplates.early_warning_system(config)
        }