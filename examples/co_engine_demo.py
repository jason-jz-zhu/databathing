"""
Capital One Banking Engine Demo
Demonstrates the COEngine capabilities for banking compliance and security
"""

from databathing import Pipeline
from databathing.templates.customer_360 import Customer360Templates, Customer360Config
from databathing.templates.risk_management import RiskManagementTemplates, RiskConfig
from databathing.validation.co_compliance_rules import validate_banking_compliance


def demo_basic_co_engine():
    """Demonstrate basic COEngine functionality with security features"""
    print("=" * 60)
    print("Capital One Banking Engine - Basic Demo")
    print("=" * 60)
    
    # Basic customer query with sensitive data
    query = """
    SELECT 
        customer_id,
        first_name,
        last_name,
        ssn,
        email,
        account_number,
        current_balance
    FROM customers c
    JOIN accounts a ON c.customer_id = a.customer_id
    WHERE a.account_status = 'ACTIVE'
      AND c.risk_rating != 'HIGH_RISK'
    ORDER BY current_balance DESC
    LIMIT 100
    """
    
    # Security configuration
    security_config = {
        'enable_masking': True,
        'enable_audit': True
    }
    
    # Generate banking-compliant PySpark code
    pipeline = Pipeline(query, engine="co", security_config=security_config, validate=False)
    result = pipeline.parse()
    
    print("Original SQL Query:")
    print(query)
    print("\nGenerated Banking-Compliant PySpark Code:")
    print(result)
    
    # Get compliance report
    from mo_sql_parsing import parse_bigquery as parse
    import json
    from databathing.engines.co_engine import COEngine
    
    parsed_query = parse(query)
    parsed_json = json.loads(json.dumps(parsed_query, indent=4))
    engine = COEngine(parsed_json, security_config)
    engine.parse()  # Process the query
    
    compliance_report = engine.get_compliance_report()
    print("\nCompliance Report:")
    print(f"Sensitive Columns Detected: {compliance_report['sensitive_columns_detected']}")
    print(f"Audit Trail Required: {compliance_report['audit_trail_required']}")
    print(f"Masking Enabled: {compliance_report['masking_enabled']}")
    print(f"Compliance Score: {compliance_report['compliance_score']}/100")


def demo_customer_360_template():
    """Demonstrate Customer 360 template usage"""
    print("\n" + "=" * 60)
    print("Customer 360 Template Demo")
    print("=" * 60)
    
    config = Customer360Config(
        customer_table="customers",
        account_table="accounts", 
        transaction_table="transactions",
        enable_masking=True,
        include_audit_trail=True
    )
    
    # Generate Customer 360 profile query
    customer_profile_sql = Customer360Templates.basic_customer_profile(config)
    
    print("Customer 360 Basic Profile SQL Template:")
    print(customer_profile_sql[:500] + "...\n")
    
    # Convert to PySpark using COEngine
    pipeline = Pipeline(customer_profile_sql, engine="co", validate=False)
    result = pipeline.parse()
    
    print("Generated PySpark Code (first 300 chars):")
    print(result[:300] + "...")


def demo_risk_management_template():
    """Demonstrate Risk Management template usage"""
    print("\n" + "=" * 60) 
    print("Risk Management Template Demo")
    print("=" * 60)
    
    config = RiskConfig(
        customer_table="customers",
        loan_table="loans",
        credit_bureau_table="credit_bureau_data",
        enable_stress_testing=True
    )
    
    # Generate credit risk scoring query
    risk_scoring_sql = RiskManagementTemplates.credit_risk_scoring(config)
    
    print("Credit Risk Scoring SQL Template (first 400 chars):")
    print(risk_scoring_sql[:400] + "...\n")
    
    # Note: This would be a very complex query to parse, so just show the template
    print("This complex risk management query includes:")
    print("- Credit bureau data integration")
    print("- Banking relationship metrics") 
    print("- Payment behavior analysis")
    print("- Comprehensive risk scoring (0-100)")
    print("- Probability of default calculation")


def demo_compliance_validation():
    """Demonstrate banking compliance validation"""
    print("\n" + "=" * 60)
    print("Banking Compliance Validation Demo") 
    print("=" * 60)
    
    # Example of problematic code with compliance issues
    problematic_code = """
    customers.selectExpr("customer_id", "ssn", "credit_card_number", "cvv")
    .filter("ssn = '123-45-6789'")
    .filter("credit_card_number = '4111-1111-1111-1111'")
    """
    
    print("Problematic Code:")
    print(problematic_code)
    
    # Run compliance validation
    compliance_result = validate_banking_compliance(problematic_code, "co")
    
    print("\nCompliance Analysis Results:")
    print(f"Overall Compliance Score: {compliance_result['overall_compliance_score']:.1f}/100")
    print(f"Compliance Grade: {compliance_result['compliance_grade']}")
    print(f"Critical Issues: {compliance_result['critical_issues']}")
    print(f"Warning Issues: {compliance_result['warning_issues']}")
    
    print("\nDetected Issues:")
    for issue in compliance_result['issues'][:3]:  # Show first 3 issues
        print(f"- [{issue.severity.value}] {issue.message}")
        if issue.suggestion:
            print(f"  Suggestion: {issue.suggestion}")
    
    # Show rule-specific scores
    print(f"\nRule Scores:")
    for rule_name, score in compliance_result['rule_scores'].items():
        print(f"- {rule_name}: {score}/100")


def demo_complex_banking_query():
    """Demonstrate complex banking query with multiple features"""
    print("\n" + "=" * 60)
    print("Complex Banking Query Demo")
    print("=" * 60)
    
    complex_query = """
    WITH customer_risk_profile AS (
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            cb.credit_score,
            sum(a.current_balance) as total_balance,
            count(distinct a.account_id) as account_count,
            avg(t.transaction_amount) as avg_transaction_amount
        FROM customers c
        JOIN credit_bureau cb ON c.customer_id = cb.customer_id
        JOIN accounts a ON c.customer_id = a.customer_id  
        JOIN transactions t ON a.account_id = t.account_id
        WHERE t.transaction_date >= date_sub(current_date(), 90)
          AND a.account_status = 'ACTIVE'
        GROUP BY c.customer_id, c.first_name, c.last_name, cb.credit_score
    ),
    risk_classification AS (
        SELECT 
            *,
            case 
                when credit_score >= 750 and total_balance >= 50000 then 'PRIME'
                when credit_score >= 700 and total_balance >= 25000 then 'NEAR_PRIME'
                when credit_score >= 650 then 'SUBPRIME' 
                else 'DEEP_SUBPRIME'
            end as risk_tier,
            case
                when avg_transaction_amount > 5000 then 'HIGH_ACTIVITY'
                when avg_transaction_amount > 1000 then 'MEDIUM_ACTIVITY'
                else 'LOW_ACTIVITY'  
            end as activity_level
        FROM customer_risk_profile
    )
    SELECT 
        customer_id,
        first_name,
        last_name,
        credit_score,
        total_balance,
        account_count,
        risk_tier,
        activity_level,
        case when risk_tier = 'PRIME' and activity_level = 'HIGH_ACTIVITY' then 'PLATINUM'
             when risk_tier in ('PRIME', 'NEAR_PRIME') then 'GOLD'  
             when risk_tier = 'SUBPRIME' then 'SILVER'
             else 'BRONZE'
        end as customer_segment
    FROM risk_classification
    ORDER BY total_balance DESC, credit_score DESC
    LIMIT 1000
    """
    
    print("Complex Banking Query with CTEs and Risk Classification:")
    print(complex_query[:300] + "...\n")
    
    # Generate with COEngine  
    security_config = {
        'enable_masking': False,  # Keep readable for demo
        'enable_audit': True
    }
    
    pipeline = Pipeline(complex_query, engine="co", security_config=security_config, validate=False)
    result = pipeline.parse()
    
    print("Generated Banking-Compliant PySpark Code (first 400 chars):")
    print(result[:400] + "...")
    
    print(f"\nGenerated code includes:")
    print("- Multiple CTE definitions")
    print("- Complex risk classification logic") 
    print("- Customer segmentation")
    print("- Banking compliance headers and audit trail")


if __name__ == "__main__":
    """Run all COEngine demos"""
    try:
        demo_basic_co_engine()
        demo_customer_360_template() 
        demo_risk_management_template()
        demo_compliance_validation()
        demo_complex_banking_query()
        
        print("\n" + "=" * 60)
        print("COEngine Demo Complete!")
        print("=" * 60)
        print("\nKey Features Demonstrated:")
        print("✅ Sensitive data detection and masking")
        print("✅ Automatic audit trail generation")
        print("✅ Banking compliance validation")
        print("✅ Customer 360 view templates")
        print("✅ Risk management templates")
        print("✅ Complex query support (CTEs, JOINs, etc.)")
        print("✅ Comprehensive compliance reporting")
        
    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()