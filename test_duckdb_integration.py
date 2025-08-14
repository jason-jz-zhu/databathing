#!/usr/bin/env python3
"""
Quick test script to verify DuckDB integration works
"""

from databathing import Pipeline

def test_simple_query():
    """Test basic SELECT query with both engines"""
    query = "SELECT name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10"
    
    print("=== Testing Simple Query ===")
    print(f"SQL: {query}")
    print()
    
    # Test with Spark (default)
    print("--- PySpark Output ---")
    spark_pipeline = Pipeline(query, engine="spark")
    spark_code = spark_pipeline.parse()
    print(spark_code)
    print()
    
    # Test with DuckDB
    print("--- DuckDB Output ---")
    duckdb_pipeline = Pipeline(query, engine="duckdb")
    duckdb_code = duckdb_pipeline.parse()
    print(duckdb_code)
    print()

def test_complex_query():
    """Test query with GROUP BY and aggregation"""
    query = """
        SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary 
        FROM employees 
        WHERE salary > 50000 
        GROUP BY department 
        HAVING COUNT(*) > 5
        ORDER BY avg_salary DESC
    """
    
    print("=== Testing Complex Query ===")
    print(f"SQL: {query.strip()}")
    print()
    
    # Test with Spark
    print("--- PySpark Output ---")
    spark_pipeline = Pipeline(query, engine="spark")
    spark_code = spark_pipeline.parse()
    print(spark_code)
    print()
    
    # Test with DuckDB
    print("--- DuckDB Output ---")
    duckdb_pipeline = Pipeline(query, engine="duckdb")
    duckdb_code = duckdb_pipeline.parse()
    print(duckdb_code)
    print()

def test_set_operations():
    """Test UNION operation"""
    query = """
        SELECT name FROM table1
        UNION ALL
        SELECT name FROM table2
        ORDER BY name
    """
    
    print("=== Testing Set Operations ===")
    print(f"SQL: {query.strip()}")
    print()
    
    # Test with Spark
    print("--- PySpark Output ---")
    try:
        spark_pipeline = Pipeline(query, engine="spark")
        spark_code = spark_pipeline.parse()
        print(spark_code)
    except Exception as e:
        print(f"Error: {e}")
    print()
    
    # Test with DuckDB
    print("--- DuckDB Output ---")
    try:
        duckdb_pipeline = Pipeline(query, engine="duckdb")
        duckdb_code = duckdb_pipeline.parse()
        print(duckdb_code)
    except Exception as e:
        print(f"Error: {e}")
    print()

if __name__ == "__main__":
    test_simple_query()
    test_complex_query()
    test_set_operations()
    
    print("=== Integration Test Complete ===")