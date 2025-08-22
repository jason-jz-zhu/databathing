"""
Customer 360 View Templates
Pre-built SQL patterns for comprehensive customer data consolidation
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class Customer360Config:
    """Configuration for Customer 360 view generation"""
    customer_table: str = "customers"
    account_table: str = "accounts" 
    transaction_table: str = "transactions"
    product_table: str = "products"
    interaction_table: str = "customer_interactions"
    demographics_table: str = "demographics"
    enable_masking: bool = True
    include_audit_trail: bool = True


class Customer360Templates:
    """Templates for generating Customer 360 views and analytics"""
    
    @staticmethod
    def basic_customer_profile(config: Customer360Config) -> str:
        """Generate basic customer profile aggregation"""
        masking_clause = ""
        if config.enable_masking:
            masking_clause = """
    concat('XXX-XX-', substring(c.ssn, -4, 4)) as masked_ssn,
    concat('****', substring(c.account_primary, -4, 4)) as masked_primary_account,
    concat(substring(c.email, 1, 3), '***@', substring_index(c.email, '@', -1)) as masked_email,"""
        
        audit_clause = ""
        if config.include_audit_trail:
            audit_clause = """
    current_timestamp() as profile_generated_at,
    current_user() as generated_by,
    'customer_360_basic' as view_type,"""
        
        return f"""
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.date_of_birth,{masking_clause}
    c.customer_since_date,
    c.customer_status,
    c.risk_rating,
    c.preferred_contact_method,{audit_clause}
    -- Demographics
    d.income_range,
    d.occupation,
    d.education_level,
    d.marital_status,
    -- Account Summary
    count(distinct a.account_id) as total_accounts,
    sum(case when a.account_status = 'ACTIVE' then 1 else 0 end) as active_accounts,
    sum(a.current_balance) as total_balance,
    max(a.account_open_date) as most_recent_account_date,
    -- Product Holdings
    string_agg(distinct p.product_type, ', ') as product_portfolio
FROM {config.customer_table} c
LEFT JOIN {config.demographics_table} d ON c.customer_id = d.customer_id  
LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
LEFT JOIN {config.product_table} p ON a.product_id = p.product_id
WHERE c.customer_status != 'INACTIVE'
GROUP BY c.customer_id, c.first_name, c.last_name, c.date_of_birth, 
         c.customer_since_date, c.customer_status, c.risk_rating, 
         c.preferred_contact_method, d.income_range, d.occupation, 
         d.education_level, d.marital_status
"""

    @staticmethod
    def customer_transaction_summary(config: Customer360Config, days: int = 90) -> str:
        """Generate customer transaction behavior summary"""
        return f"""
WITH transaction_metrics AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        -- Transaction Volume Metrics
        count(t.transaction_id) as total_transactions,
        count(distinct date(t.transaction_date)) as active_days,
        sum(t.transaction_amount) as total_transaction_volume,
        avg(t.transaction_amount) as avg_transaction_amount,
        max(t.transaction_amount) as max_transaction_amount,
        min(t.transaction_amount) as min_transaction_amount,
        -- Transaction Type Analysis
        sum(case when t.transaction_type = 'DEBIT' then t.transaction_amount else 0 end) as total_debits,
        sum(case when t.transaction_type = 'CREDIT' then t.transaction_amount else 0 end) as total_credits,
        count(case when t.transaction_type = 'DEBIT' then 1 end) as debit_count,
        count(case when t.transaction_type = 'CREDIT' then 1 end) as credit_count,
        -- Channel Analysis
        count(distinct t.channel) as channels_used,
        string_agg(distinct t.channel, ', ') as channel_list,
        -- Risk Indicators
        sum(case when t.transaction_amount > 10000 then 1 else 0 end) as large_transactions,
        count(case when t.merchant_category = 'HIGH_RISK' then 1 end) as high_risk_merchants
    FROM {config.customer_table} c
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.transaction_table} t ON a.account_id = t.account_id
    WHERE t.transaction_date >= date_sub(current_date(), {days})
      AND t.transaction_status = 'COMPLETED'
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    *,
    -- Derived Metrics
    case 
        when active_days > 0 then total_transactions / active_days 
        else 0 
    end as avg_transactions_per_day,
    case 
        when total_credits > 0 then total_debits / total_credits 
        else total_debits 
    end as debit_credit_ratio,
    -- Risk Scoring
    case 
        when large_transactions > 5 or high_risk_merchants > 3 then 'HIGH'
        when large_transactions > 2 or high_risk_merchants > 1 then 'MEDIUM' 
        else 'LOW'
    end as transaction_risk_level,
    current_timestamp() as analysis_generated_at
FROM transaction_metrics
"""

    @staticmethod 
    def customer_product_affinity(config: Customer360Config) -> str:
        """Generate customer product affinity and cross-sell opportunities"""
        return f"""
WITH customer_products AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.customer_since_date,
        c.risk_rating,
        d.income_range,
        -- Current Products
        collect_set(p.product_type) as current_products,
        collect_set(p.product_category) as current_categories,
        count(distinct p.product_type) as product_count,
        sum(a.current_balance) as total_relationship_value,
        -- Product Usage Metrics
        avg(case when p.product_type = 'CHECKING' then a.monthly_avg_balance end) as avg_checking_balance,
        avg(case when p.product_type = 'SAVINGS' then a.monthly_avg_balance end) as avg_savings_balance,
        max(case when p.product_category = 'CREDIT' then a.credit_limit end) as max_credit_limit,
        sum(case when p.product_category = 'LOAN' then a.current_balance end) as total_loan_balance
    FROM {config.customer_table} c
    LEFT JOIN {config.demographics_table} d ON c.customer_id = d.customer_id
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.product_table} p ON a.product_id = p.product_id
    WHERE a.account_status = 'ACTIVE'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_since_date, 
             c.risk_rating, d.income_range
),
cross_sell_opportunities AS (
    SELECT 
        cp.*,
        -- Cross-sell Opportunities
        case 
            when not array_contains(current_products, 'CREDIT_CARD') 
                 and total_relationship_value > 5000 
                 and risk_rating in ('LOW', 'MEDIUM')
            then 'CREDIT_CARD'
        end as credit_card_opportunity,
        case 
            when not array_contains(current_products, 'INVESTMENT') 
                 and avg_checking_balance > 10000
                 and income_range in ('HIGH', 'VERY_HIGH')
            then 'INVESTMENT_ACCOUNT'
        end as investment_opportunity,
        case 
            when not array_contains(current_products, 'MORTGAGE') 
                 and total_relationship_value > 25000
                 and datediff(current_date(), customer_since_date) > 365
            then 'MORTGAGE'
        end as mortgage_opportunity,
        case
            when product_count = 1 
                 and total_relationship_value > 1000
            then 'BUNDLE_OPPORTUNITY'
        end as bundle_opportunity
    FROM customer_products cp
)
SELECT 
    *,
    -- Priority Scoring
    case 
        when credit_card_opportunity is not null then 3
        when investment_opportunity is not null then 4  
        when mortgage_opportunity is not null then 5
        when bundle_opportunity is not null then 2
        else 1
    end as cross_sell_priority,
    current_timestamp() as opportunity_generated_at
FROM cross_sell_opportunities
ORDER BY total_relationship_value DESC, cross_sell_priority DESC
"""

    @staticmethod
    def customer_lifecycle_analysis(config: Customer360Config) -> str:
        """Analyze customer lifecycle stages and engagement patterns"""
        return f"""
WITH customer_lifecycle AS (
    SELECT 
        c.customer_id,
        c.first_name, 
        c.last_name,
        c.customer_since_date,
        datediff(current_date(), c.customer_since_date) as days_as_customer,
        -- Account Activity
        count(distinct a.account_id) as total_accounts,
        max(a.last_transaction_date) as last_activity_date,
        datediff(current_date(), max(a.last_transaction_date)) as days_since_last_activity,
        sum(a.current_balance) as total_balance,
        avg(a.monthly_avg_balance) as avg_monthly_balance,
        -- Interaction History
        count(distinct i.interaction_id) as total_interactions,
        max(i.interaction_date) as last_interaction_date,
        count(case when i.interaction_type = 'COMPLAINT' then 1 end) as complaint_count,
        count(case when i.interaction_type = 'SUPPORT' then 1 end) as support_count,
        -- Transaction Patterns
        count(case when t.transaction_date >= date_sub(current_date(), 30) then 1 end) as transactions_last_30d,
        count(case when t.transaction_date >= date_sub(current_date(), 90) then 1 end) as transactions_last_90d,
        avg(case when t.transaction_date >= date_sub(current_date(), 90) 
                 then t.transaction_amount end) as avg_recent_transaction_amount
    FROM {config.customer_table} c
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.interaction_table} i ON c.customer_id = i.customer_id
    LEFT JOIN {config.transaction_table} t ON a.account_id = t.account_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_since_date
)
SELECT 
    *,
    -- Lifecycle Stage Classification
    case 
        when days_as_customer <= 90 then 'NEW'
        when days_as_customer <= 365 then 'GROWING'
        when days_as_customer <= 1825 and transactions_last_90d >= 10 then 'MATURE_ACTIVE'
        when days_as_customer > 1825 and transactions_last_90d >= 5 then 'LOYAL'
        when days_since_last_activity <= 30 then 'ACTIVE'
        when days_since_last_activity <= 90 then 'DECLINING'
        when days_since_last_activity <= 180 then 'DORMANT'
        else 'AT_RISK'
    end as lifecycle_stage,
    -- Engagement Score (0-100)
    least(100, greatest(0, 
        (case when transactions_last_30d > 0 then 25 else 0 end) +
        (case when days_since_last_activity <= 7 then 25 
              when days_since_last_activity <= 30 then 15
              when days_since_last_activity <= 60 then 5 else 0 end) +
        (case when total_accounts > 2 then 20 
              when total_accounts > 1 then 10 else 0 end) +
        (case when total_balance > 10000 then 20
              when total_balance > 5000 then 15 
              when total_balance > 1000 then 10 else 0 end) +
        (case when complaint_count = 0 then 10 else 0 end)
    )) as engagement_score,
    -- Risk Indicators
    case 
        when days_since_last_activity > 180 then 'CHURN_RISK'
        when complaint_count > 2 then 'SATISFACTION_RISK'
        when total_balance < 100 and transactions_last_90d = 0 then 'DORMANCY_RISK'
        else 'LOW_RISK'
    end as retention_risk,
    current_timestamp() as analysis_timestamp
FROM customer_lifecycle
ORDER BY engagement_score DESC, total_balance DESC
"""

    @staticmethod
    def customer_profitability_analysis(config: Customer360Config) -> str:
        """Calculate customer profitability metrics"""
        return f"""
WITH customer_revenue AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        -- Fee Revenue
        sum(case when t.fee_type = 'MONTHLY_MAINTENANCE' then t.fee_amount else 0 end) as maintenance_fees,
        sum(case when t.fee_type = 'OVERDRAFT' then t.fee_amount else 0 end) as overdraft_fees,
        sum(case when t.fee_type = 'TRANSACTION' then t.fee_amount else 0 end) as transaction_fees,
        sum(case when t.fee_type = 'ATM' then t.fee_amount else 0 end) as atm_fees,
        sum(t.fee_amount) as total_fee_revenue,
        -- Interest Revenue
        sum(case when p.product_category = 'LOAN' then a.interest_earned else 0 end) as loan_interest_revenue,
        sum(case when p.product_category = 'CREDIT' then a.interest_earned else 0 end) as credit_interest_revenue,
        -- Relationship Metrics
        avg(a.average_balance) as avg_relationship_balance,
        sum(a.current_balance) as total_relationship_balance,
        count(distinct p.product_type) as product_count
    FROM {config.customer_table} c
    LEFT JOIN {config.account_table} a ON c.customer_id = a.customer_id
    LEFT JOIN {config.product_table} p ON a.product_id = p.product_id
    LEFT JOIN {config.transaction_table} t ON a.account_id = t.account_id
    WHERE t.transaction_date >= date_sub(current_date(), 365) -- Last 12 months
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    *,
    -- Total Revenue
    total_fee_revenue + loan_interest_revenue + credit_interest_revenue as total_annual_revenue,
    -- Profitability Metrics  
    case 
        when product_count > 3 and total_fee_revenue + loan_interest_revenue + credit_interest_revenue > 1000 then 'HIGH_VALUE'
        when product_count > 2 and total_fee_revenue + loan_interest_revenue + credit_interest_revenue > 500 then 'MEDIUM_VALUE'
        when total_fee_revenue + loan_interest_revenue + credit_interest_revenue > 100 then 'STANDARD_VALUE'
        else 'LOW_VALUE'
    end as customer_value_segment,
    -- Revenue per Product
    case 
        when product_count > 0 then (total_fee_revenue + loan_interest_revenue + credit_interest_revenue) / product_count
        else 0 
    end as revenue_per_product,
    current_timestamp() as profitability_analysis_date
FROM customer_revenue
ORDER BY total_annual_revenue DESC
"""

    @staticmethod
    def get_all_templates() -> Dict[str, str]:
        """Get all Customer 360 templates"""
        config = Customer360Config()  # Default configuration
        
        return {
            "basic_customer_profile": Customer360Templates.basic_customer_profile(config),
            "customer_transaction_summary": Customer360Templates.customer_transaction_summary(config),
            "customer_product_affinity": Customer360Templates.customer_product_affinity(config),
            "customer_lifecycle_analysis": Customer360Templates.customer_lifecycle_analysis(config),
            "customer_profitability_analysis": Customer360Templates.customer_profitability_analysis(config)
        }