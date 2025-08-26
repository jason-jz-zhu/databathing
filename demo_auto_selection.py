#!/usr/bin/env python3
"""
Auto-Selection Demo Script

Demonstrates the intelligent engine selection capabilities of DataBathing.
Shows how the system automatically chooses between Spark and DuckDB based on
query characteristics, data patterns, and user context.
"""

from databathing import Pipeline, SelectionContext

def print_separator():
    print("=" * 80)

def demo_basic_auto_selection():
    """Demo basic auto-selection without context"""
    print_separator()
    print("🎯 BASIC AUTO-SELECTION DEMO")
    print_separator()
    
    queries = [
        ("Small Interactive Query", 
         "SELECT name, age FROM users WHERE age > 25 LIMIT 10"),
        
        ("Simple Analytics", 
         "SELECT department, COUNT(*) FROM employees GROUP BY department"),
        
        ("Complex ETL",
         """WITH sales_data AS (
                SELECT customer_id, product_id, amount, date
                FROM fact_sales WHERE date >= '2024-01-01'  
            ),
            customer_totals AS (
                SELECT customer_id, SUM(amount) as total_spent
                FROM sales_data GROUP BY customer_id
            )
            SELECT c.name, ct.total_spent 
            FROM customers c
            JOIN customer_totals ct ON c.id = ct.customer_id
            JOIN departments d ON c.dept_id = d.id
            WHERE ct.total_spent > 10000"""),
        
        ("Large Data Processing",
         "SELECT region, SUM(revenue) FROM big_sales_table GROUP BY region")
    ]
    
    for name, query in queries:
        print(f"\n📊 {name}")
        print("-" * 60)
        print(f"SQL: {query[:100]}{'...' if len(query) > 100 else ''}")
        
        pipeline = Pipeline(query, auto_engine=True)
        
        print(f"✅ Selected Engine: {pipeline.engine.upper()}")
        print(f"🎯 Confidence: {pipeline.get_selection_confidence():.0%}")
        print(f"💡 Reasoning: {pipeline.get_selection_reasoning()}")

def demo_context_driven_selection():
    """Demo auto-selection with different contexts"""
    print_separator()
    print("🎯 CONTEXT-DRIVEN SELECTION DEMO")  
    print_separator()
    
    base_query = "SELECT category, AVG(price), COUNT(*) FROM products WHERE price > 100 GROUP BY category"
    
    contexts = [
        ("Dashboard (Speed Priority)", SelectionContext(
            performance_priority="speed",
            latency_requirement="interactive",
            workload_type="dashboard"
        )),
        
        ("Cost Optimization", SelectionContext(
            performance_priority="cost", 
            data_size_hint="medium"
        )),
        
        ("Large Data ETL", SelectionContext(
            data_size_hint="large",
            workload_type="etl",
            fault_tolerance=True
        )),
        
        ("Small Data Analytics", SelectionContext(
            data_size_hint="small",
            latency_requirement="interactive"
        ))
    ]
    
    print(f"Base Query: {base_query}")
    print()
    
    for context_name, context in contexts:
        print(f"📋 Context: {context_name}")
        print("-" * 40)
        
        pipeline = Pipeline(base_query, auto_engine=True, context=context)
        
        print(f"  Selected Engine: {pipeline.engine.upper()}")
        print(f"  Confidence: {pipeline.get_selection_confidence():.0%}")
        print(f"  Reasoning: {pipeline.get_selection_reasoning()}")
        print()

def demo_detailed_analysis():
    """Demo detailed selection analysis"""
    print_separator()
    print("🎯 DETAILED ANALYSIS DEMO")
    print_separator()
    
    query = """
    WITH customer_metrics AS (
        SELECT customer_id, 
               COUNT(*) as order_count,
               SUM(amount) as total_spent,
               AVG(amount) as avg_order_value
        FROM orders 
        WHERE order_date >= '2024-01-01'
        GROUP BY customer_id
    )
    SELECT cm.customer_id, 
           cm.total_spent,
           c.name,
           RANK() OVER (ORDER BY cm.total_spent DESC) as spending_rank
    FROM customer_metrics cm
    JOIN customers c ON cm.customer_id = c.id
    WHERE cm.total_spent > 5000
    ORDER BY cm.total_spent DESC
    """
    
    context = SelectionContext(
        data_size_hint="medium",
        performance_priority="balanced", 
        workload_type="analytics"
    )
    
    pipeline = Pipeline(query, auto_engine=True, context=context)
    
    print("📊 Query Analysis:")
    print(f"   SQL: {query.strip()[:200]}...")
    print()
    
    selection_info = pipeline.get_selection_info()
    print("🎯 Selection Results:")
    print(f"   Engine: {selection_info['selected_engine'].upper()}")
    print(f"   Confidence: {selection_info['confidence']:.0%}")
    print(f"   Rule: {selection_info['rule_name']}")
    print(f"   Analysis Time: {selection_info['analysis_time_ms']:.1f}ms")
    print()
    
    print("💭 Detailed Reasoning:")
    print(f"   {pipeline.get_selection_reasoning()}")
    print()
    
    # Show detailed analysis
    detailed = pipeline.get_detailed_selection_analysis()
    features = detailed['query_features']
    estimate = detailed['data_estimate']
    
    print("📈 Query Characteristics:")
    print(f"   Complexity Score: {features['complexity_score']:.1f}")
    print(f"   Join Count: {features['join_count']}")
    print(f"   Table Count: {features['table_count']}")
    print(f"   Has Aggregations: {features['has_aggregations']}")
    print(f"   Has Window Functions: {features['has_window_functions']}")
    print(f"   Interactive Indicators: {features['interactive_indicators']}")
    print()
    
    print("📊 Data Estimation:")
    print(f"   Estimated Size: {estimate['size_gb']:.1f} GB")
    print(f"   Size Category: {estimate['size_category']}")
    print(f"   Estimation Confidence: {estimate['confidence']:.0%}")
    print(f"   Estimation Method: {estimate['reasoning']}")

def demo_comparison_with_manual():
    """Demo comparison between auto and manual selection"""
    print_separator()
    print("🎯 AUTO vs MANUAL SELECTION COMPARISON")
    print_separator()
    
    query = "SELECT region, SUM(sales), AVG(profit_margin) FROM regional_sales WHERE year = 2024 GROUP BY region HAVING SUM(sales) > 1000000"
    
    print(f"Query: {query}")
    print()
    
    # Auto selection
    auto_pipeline = Pipeline(query, auto_engine=True)
    print("🤖 AUTO-SELECTION:")
    print(f"   Engine: {auto_pipeline.engine.upper()}")
    print(f"   Confidence: {auto_pipeline.get_selection_confidence():.0%}")
    print(f"   Reasoning: {auto_pipeline.get_selection_reasoning()}")
    print()
    
    # Manual selections for comparison
    engines = ["spark", "duckdb", "mojo"]
    print("👤 MANUAL SELECTION OPTIONS:")
    
    for engine in engines:
        manual_pipeline = Pipeline(query, engine=engine)
        print(f"   {engine.upper()}: Available ✅")
        
        # Show what code would be generated
        try:
            code = manual_pipeline.parse()
            code_preview = code.strip()[:100] + "..." if len(code) > 100 else code.strip()
            print(f"      Preview: {code_preview}")
        except Exception as e:
            print(f"      Error: {e}")
        print()

def demo_validation_with_auto_selection():
    """Demo validation system working with auto-selection"""
    print_separator()  
    print("🎯 AUTO-SELECTION + VALIDATION DEMO")
    print_separator()
    
    query = "SELECT customer_name, total_orders FROM customer_summary WHERE total_spent > 10000 ORDER BY total_orders DESC LIMIT 5"
    
    pipeline = Pipeline(query, auto_engine=True, validate=True)
    
    # Parse with validation
    result = pipeline.parse_with_validation()
    
    print(f"Query: {query}")
    print()
    print("🔍 Analysis + Validation Results:")
    print(f"   Selected Engine: {pipeline.engine.upper()}")
    print(f"   Selection Confidence: {pipeline.get_selection_confidence():.0%}")
    print()
    
    if 'auto_selection' in result:
        auto_info = result['auto_selection']
        print("🤖 Auto-Selection Details:")
        print(f"   Rule Applied: {auto_info['rule_name']}")
        print(f"   Reasoning: {auto_info['reasoning']}")
        print(f"   Analysis Time: {auto_info['analysis_time_ms']:.1f}ms")
        print()
    
    print("📋 Code Validation:")
    if result['validation_report']:
        print(f"   Quality Score: {result['score']:.1f}/100")
        print(f"   Grade: {result['grade']}")
        print(f"   Syntactically Valid: {result['is_valid']}")
    else:
        print("   Validation skipped or unavailable")
    
    print()
    print("💻 Generated Code:")
    code_lines = result['code'].strip().split('\n')
    for i, line in enumerate(code_lines[:3], 1):  # Show first 3 lines
        print(f"   {i}: {line}")
    if len(code_lines) > 3:
        print(f"   ... ({len(code_lines) - 3} more lines)")

def main():
    """Run all demos"""
    print("🚀 DataBathing Auto-Selection Demo")
    print("Intelligent Engine Selection: Spark vs DuckDB")
    print()
    
    try:
        demo_basic_auto_selection()
        demo_context_driven_selection() 
        demo_detailed_analysis()
        demo_comparison_with_manual()
        demo_validation_with_auto_selection()
        
        print_separator()
        print("✅ Demo Complete!")
        print()
        print("🎉 Key Benefits Demonstrated:")
        print("   • Automatic engine selection eliminates manual decisions")
        print("   • Context-aware selection optimizes for user priorities")
        print("   • Transparent reasoning explains every decision")  
        print("   • Full backward compatibility with manual selection")
        print("   • Integration with existing validation system")
        print()
        print("📚 Learn More:")
        print("   • Use auto_engine=True for automatic selection")
        print("   • Provide SelectionContext for better decisions")
        print("   • Call get_selection_reasoning() to understand choices")
        print("   • Manual selection still available for all engines")
        
    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()