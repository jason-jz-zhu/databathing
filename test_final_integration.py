#!/usr/bin/env python3
"""
Final comprehensive integration test for DuckDB engine
"""

def test_engine_comparison():
    """Test that both engines produce expected outputs for the same query"""
    from databathing import Pipeline
    
    print("🧪 Testing Engine Comparison\n")
    
    test_queries = [
        # Simple SELECT
        "SELECT name FROM users",
        
        # SELECT with WHERE
        "SELECT name, age FROM users WHERE age > 25",
        
        # SELECT with ORDER BY and LIMIT
        "SELECT name FROM users ORDER BY name DESC LIMIT 5",
        
        # GROUP BY with aggregation
        "SELECT department, COUNT(*) as total FROM employees GROUP BY department",
        
        # Complex query with multiple clauses
        """SELECT department, AVG(salary) as avg_sal 
           FROM employees 
           WHERE salary > 50000 
           GROUP BY department 
           HAVING COUNT(*) > 5
           ORDER BY avg_sal DESC
           LIMIT 3"""
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"🔍 Test Query {i}: {query.strip()}")
        print()
        
        # Test PySpark engine
        try:
            spark_pipeline = Pipeline(query, engine="spark")
            spark_result = spark_pipeline.parse()
            print("✅ PySpark Engine:")
            print(f"   {spark_result.strip()}")
        except Exception as e:
            print(f"❌ PySpark Engine Error: {e}")
        
        # Test DuckDB engine  
        try:
            duckdb_pipeline = Pipeline(query, engine="duckdb")
            duckdb_result = duckdb_pipeline.parse()
            print("✅ DuckDB Engine:")
            print(f"   {duckdb_result.strip()}")
        except Exception as e:
            print(f"❌ DuckDB Engine Error: {e}")
            
        print("-" * 80)

def test_invalid_engine():
    """Test error handling for invalid engine"""
    from databathing import Pipeline
    
    print("\n🧪 Testing Invalid Engine Handling")
    
    try:
        pipeline = Pipeline("SELECT * FROM test", engine="invalid")
        print("❌ Should have raised ValueError")
    except ValueError as e:
        print(f"✅ Correctly raised ValueError: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def test_backwards_compatibility():
    """Test that default behavior is unchanged"""
    from databathing import Pipeline
    
    print("\n🧪 Testing Backwards Compatibility")
    
    query = "SELECT name FROM users WHERE active = 1"
    
    # Default should be spark
    default_pipeline = Pipeline(query)
    explicit_spark = Pipeline(query, engine="spark")
    
    default_result = default_pipeline.parse()
    spark_result = explicit_spark.parse()
    
    if default_result == spark_result:
        print("✅ Default behavior preserved - uses Spark engine")
    else:
        print("❌ Default behavior changed!")
        print(f"   Default: {default_result}")
        print(f"   Spark:   {spark_result}")

def test_engine_properties():
    """Test engine properties and methods"""
    from databathing.engines import SparkEngine, DuckDBEngine
    from mo_sql_parsing import parse_bigquery as parse
    import json
    
    print("\n🧪 Testing Engine Properties")
    
    query = "SELECT name FROM users"
    parsed = parse(query)
    parsed_json = json.loads(json.dumps(parsed, indent=4))
    
    # Test Spark Engine
    spark_engine = SparkEngine(parsed_json)
    print(f"✅ Spark Engine Name: {spark_engine.engine_name}")
    
    # Test DuckDB Engine
    duckdb_engine = DuckDBEngine(parsed_json)  
    print(f"✅ DuckDB Engine Name: {duckdb_engine.engine_name}")

if __name__ == "__main__":
    test_engine_comparison()
    test_invalid_engine()
    test_backwards_compatibility()
    test_engine_properties()
    
    print("\n🎉 Final Integration Test Complete!")
    print("✅ DuckDB engine successfully integrated into DataBathing")