#!/usr/bin/env python3
"""
Demonstration of the DataBathing Code Validation System

This script shows how to use the validation system to check and score
generated PySpark and DuckDB code for syntax correctness, performance,
readability, and best practices.
"""

from databathing import Pipeline
from databathing.validation.validator_factory import validate_code


def demo_validation_system():
    """Demonstrate the validation system with various SQL queries"""
    
    print("🔍 DataBathing Code Validation System Demo")
    print("=" * 50)
    
    test_queries = [
        {
            "name": "Simple Query",
            "sql": "SELECT name, age FROM users WHERE age > 25"
        },
        {
            "name": "Complex Analytics Query", 
            "sql": """
                SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count
                FROM employees 
                WHERE salary > 50000 AND department IS NOT NULL
                GROUP BY department 
                HAVING COUNT(*) > 5
                ORDER BY avg_salary DESC
                LIMIT 10
            """
        },
        {
            "name": "Query with Performance Issues",
            "sql": "SELECT * FROM large_table ORDER BY created_at"  # SELECT * + ORDER BY without LIMIT
        },
        {
            "name": "Query with Window Functions",
            "sql": """
                SELECT name, salary, 
                       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
                FROM employees
                WHERE salary > 40000
            """
        }
    ]
    
    for i, query_info in enumerate(test_queries, 1):
        print(f"\n🧪 Test {i}: {query_info['name']}")
        print(f"SQL: {query_info['sql'].strip()}")
        print("-" * 40)
        
        # Test both engines
        for engine in ["spark", "duckdb"]:
            print(f"\n🔧 {engine.upper()} Engine:")
            
            try:
                # Generate and validate code
                pipeline = Pipeline(query_info['sql'], engine=engine, validate=True)
                result = pipeline.parse_with_validation()
                
                print(f"✅ Generated Code:")
                print(f"   {result['code'].strip()}")
                
                print(f"\n📊 Validation Results:")
                print(f"   Valid: {'✅ Yes' if result['is_valid'] else '❌ No'}")
                print(f"   Score: {result['score']:.1f}/100")
                print(f"   Grade: {result['grade']}")
                
                # Show detailed metrics
                if result['validation_report']:
                    metrics = result['validation_report'].metrics
                    print(f"   📈 Detailed Metrics:")
                    print(f"      Syntax: {metrics.syntax_score:.1f}/100")
                    print(f"      Complexity: {metrics.complexity_score:.1f}/100 (lower is better)")
                    print(f"      Readability: {metrics.readability_score:.1f}/100")
                    print(f"      Performance: {metrics.performance_score:.1f}/100")
                    print(f"      Maintainability: {metrics.maintainability_score:.1f}/100")
                    
                    # Show issues if any
                    if result['validation_report'].issues:
                        print(f"   ⚠️  Issues Found:")
                        for issue in result['validation_report'].issues:
                            severity_icon = "❌" if issue.severity.value == "failed" else "⚠️"
                            print(f"      {severity_icon} {issue.message}")
                            if issue.suggestion:
                                print(f"         💡 {issue.suggestion}")
                
            except Exception as e:
                print(f"❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 Summary of Validation Features:")
    print("   • Syntax validation - checks if generated code is syntactically correct")
    print("   • Performance analysis - identifies potential performance bottlenecks")
    print("   • Readability scoring - evaluates code clarity and formatting")
    print("   • Best practices checking - suggests improvements")
    print("   • Overall scoring (0-100) with letter grades (A-F)")
    print("   • Detailed issue reporting with suggestions")


def demo_direct_validation():
    """Demonstrate direct code validation without Pipeline"""
    
    print("\n🔬 Direct Code Validation Demo")
    print("=" * 40)
    
    # Example problematic code
    test_codes = [
        {
            "name": "Good PySpark Code",
            "engine": "spark",
            "code": 'final_df = users.filter("age > 25").selectExpr("name", "age").limit(100)'
        },
        {
            "name": "Problematic PySpark Code", 
            "engine": "spark",
            "code": 'final_df = users.filter("age > 25").collect()'  # collect() without limit
        },
        {
            "name": "Good DuckDB Code",
            "engine": "duckdb", 
            "code": 'result = duckdb.sql("SELECT name, age FROM users WHERE age > 25 LIMIT 100")'
        },
        {
            "name": "Problematic DuckDB Code",
            "engine": "duckdb",
            "code": 'result = duckdb.sql("SELECT * FROM large_table ORDER BY created_at")'  # SELECT * without LIMIT
        }
    ]
    
    for test in test_codes:
        print(f"\n🧪 {test['name']} ({test['engine'].upper()}):")
        print(f"Code: {test['code']}")
        
        # Validate directly
        report = validate_code(test['code'], test['engine'])
        
        print(f"Score: {report.metrics.overall_score:.1f}/100 (Grade: {report.get_grade()})")
        print(f"Valid: {'✅ Yes' if report.is_valid else '❌ No'}")
        
        if report.issues:
            print("Issues:")
            for issue in report.issues:
                severity_icon = "❌" if issue.severity.value == "failed" else "⚠️"
                print(f"  {severity_icon} {issue.message}")


def demo_comparison():
    """Compare validation scores between engines"""
    
    print("\n⚖️  Engine Comparison Demo")
    print("=" * 35)
    
    sql = "SELECT department, COUNT(*) as total, AVG(salary) as avg_sal FROM employees GROUP BY department ORDER BY total DESC"
    
    print(f"SQL: {sql}")
    print()
    
    engines = ["spark", "duckdb"]
    results = {}
    
    for engine in engines:
        pipeline = Pipeline(sql, engine=engine, validate=True)
        result = pipeline.parse_with_validation()
        results[engine] = result
        
        print(f"🔧 {engine.upper()}:")
        print(f"   Code: {result['code'].strip()}")
        print(f"   Score: {result['score']:.1f}/100 (Grade: {result['grade']})")
        print(f"   Valid: {'✅' if result['is_valid'] else '❌'}")
        print()
    
    # Compare scores
    spark_score = results['spark']['score']
    duckdb_score = results['duckdb']['score']
    
    if spark_score > duckdb_score:
        print(f"🏆 Spark wins with {spark_score:.1f} vs {duckdb_score:.1f}")
    elif duckdb_score > spark_score:
        print(f"🏆 DuckDB wins with {duckdb_score:.1f} vs {spark_score:.1f}")
    else:
        print(f"🤝 Tie! Both scored {spark_score:.1f}")


if __name__ == "__main__":
    try:
        demo_validation_system()
        demo_direct_validation() 
        demo_comparison()
        
        print("\n✅ Validation system demo completed successfully!")
        print("\n💡 Usage Tips:")
        print("   • Set validate=True in Pipeline() to enable validation")
        print("   • Use pipeline.get_code_score() to get numeric score")
        print("   • Use pipeline.get_validation_report() for detailed analysis")
        print("   • Check pipeline.is_code_valid() before executing generated code")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()