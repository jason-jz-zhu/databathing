#!/usr/bin/env python3
"""
DuckDB Performance Benchmark: Direct SQL vs DataBathing Generated Code

This script demonstrates the performance comparison between:
1. Direct DuckDB SQL execution
2. DataBathing-generated DuckDB code

DuckDB's columnar engine and vectorized execution typically show less performance
difference between SQL and API calls compared to Spark, as DuckDB is optimized
for analytical workloads from the ground up.

Usage:
    python benchmark_duckdb_performance.py

Requirements:
    pip install duckdb psutil databathing
"""

import time
import psutil
import os
import duckdb
from databathing.pipeline import Pipeline


def measure_execution_time(func, *args, **kwargs):
    """Measure execution time and memory usage"""
    process = psutil.Process(os.getpid())
    memory_before = process.memory_info().rss / 1024 / 1024  # MB
    
    start_time = time.perf_counter()
    result = func(*args, **kwargs)
    
    # Force materialization for fair comparison
    if hasattr(result, 'fetchall'):
        result.fetchall()
    elif hasattr(result, 'df'):
        result.df()
    
    end_time = time.perf_counter()
    memory_after = process.memory_info().rss / 1024 / 1024  # MB
    
    execution_time = end_time - start_time
    memory_used = memory_after - memory_before
    
    return execution_time, memory_used, result


def create_test_data(conn):
    """Create test datasets in DuckDB"""
    print("Creating DuckDB test datasets...")
    
    # Create test table with 100K records
    conn.execute("""
        CREATE TABLE test_table AS 
        SELECT 
            i as id,
            'user_' || i as name,
            (i % 100) as category,
            (i * 1.5) as value,
            (i % 10) as group_id
        FROM range(100000) t(i)
    """)
    
    # Create categories table for joins
    conn.execute("""
        CREATE TABLE categories AS
        SELECT 
            i as category_id,
            'category_' || i as category_name,
            'description_' || i as description
        FROM range(100) t(i)
    """)
    
    print("✓ Test data created successfully")


def run_direct_sql(conn, query):
    """Execute SQL query directly in DuckDB"""
    return conn.execute(query)


def run_databathing_query(conn, sql_query):
    """Execute DataBathing-generated DuckDB code"""
    pipeline = Pipeline(sql_query, engine="duckdb")
    generated_code = pipeline.parse()
    
    # The generated code is in format: result = duckdb.sql("SELECT ...")
    # Extract just the SQL part and execute it with our connection
    if 'duckdb.sql("' in generated_code:
        # Find the SQL between the quotes
        start_quote = generated_code.find('"') + 1
        end_quote = generated_code.rfind('"')
        
        if end_quote > start_quote:
            sql_part = generated_code[start_quote:end_quote]
            # Execute with our connection
            return conn.execute(sql_part)
    
    # Fallback: try to execute the raw generated code by replacing duckdb.sql with conn.execute
    modified_code = generated_code.replace('duckdb.sql(', 'conn.execute(')
    exec_globals = {'conn': conn, 'duckdb': duckdb}
    exec(modified_code, exec_globals)
    return exec_globals.get('result')


def benchmark_simple_select(conn):
    """Benchmark simple SELECT operations"""
    print("\n" + "="*50)
    print("DUCKDB BENCHMARK: Simple SELECT with WHERE")
    print("="*50)
    
    sql_query = "SELECT id, name, value FROM test_table WHERE id < 50000"
    
    # Direct SQL
    sql_time, sql_memory, sql_result = measure_execution_time(run_direct_sql, conn, sql_query)
    
    # DataBathing generated
    db_time, db_memory, db_result = measure_execution_time(run_databathing_query, conn, sql_query)
    
    # Results
    improvement = ((sql_time - db_time) / sql_time) * 100 if sql_time > 0 else 0
    print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
    print(f"DataBathing Time: {db_time:.4f}s, Memory: {db_memory:.2f}MB")
    print(f"DataBathing Performance Change: {improvement:.1f}%")
    
    # Show generated code
    pipeline = Pipeline(sql_query, engine="duckdb")
    generated_code = pipeline.parse()
    print(f"Generated Code: {generated_code}")
    
    return improvement


def benchmark_aggregation(conn):
    """Benchmark aggregation operations"""
    print("\n" + "="*50)
    print("DUCKDB BENCHMARK: Aggregation with GROUP BY")
    print("="*50)
    
    sql_query = """
        SELECT category, 
               COUNT(*) as count, 
               AVG(value) as avg_value,
               MAX(value) as max_value,
               MIN(value) as min_value
        FROM test_table 
        GROUP BY category
    """
    
    # Direct SQL
    sql_time, sql_memory, sql_result = measure_execution_time(run_direct_sql, conn, sql_query)
    
    # DataBathing generated
    db_time, db_memory, db_result = measure_execution_time(run_databathing_query, conn, sql_query)
    
    # Results
    improvement = ((sql_time - db_time) / sql_time) * 100 if sql_time > 0 else 0
    print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
    print(f"DataBathing Time: {db_time:.4f}s, Memory: {db_memory:.2f}MB")
    print(f"DataBathing Performance Change: {improvement:.1f}%")
    
    return improvement


def benchmark_join(conn):
    """Benchmark JOIN operations"""
    print("\n" + "="*50)
    print("DUCKDB BENCHMARK: JOIN Operations")
    print("="*50)
    
    # Simpler JOIN without aliases (DuckDB engine has alias issues)
    sql_query = """
        SELECT test_table.id, test_table.name, test_table.value, categories.category_name
        FROM test_table
        JOIN categories ON test_table.category = categories.category_id
        WHERE test_table.value > 50000
    """
    
    # Direct SQL
    sql_time, sql_memory, sql_result = measure_execution_time(run_direct_sql, conn, sql_query)
    
    # DataBathing generated - may not support complex JOINs
    try:
        db_time, db_memory, db_result = measure_execution_time(run_databathing_query, conn, sql_query)
        improvement = ((sql_time - db_time) / sql_time) * 100 if sql_time > 0 else 0
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing Time: {db_time:.4f}s, Memory: {db_memory:.2f}MB")
        print(f"DataBathing Performance Change: {improvement:.1f}%")
    except Exception as e:
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing: Not supported ({str(e)[:50]}...)")
        improvement = 0
    
    return improvement


def benchmark_complex_query(conn):
    """Benchmark complex queries"""
    print("\n" + "="*50)
    print("DUCKDB BENCHMARK: Complex Query with Multiple Operations")
    print("="*50)
    
    # Simplified complex query (no STDDEV which may not be supported)
    sql_query = """
        SELECT category,
               COUNT(*) as record_count,
               AVG(value) as avg_value
        FROM test_table 
        WHERE value BETWEEN 10000 AND 80000
        GROUP BY category 
        HAVING COUNT(*) > 800
        ORDER BY avg_value DESC 
        LIMIT 20
    """
    
    # Direct SQL
    sql_time, sql_memory, sql_result = measure_execution_time(run_direct_sql, conn, sql_query)
    
    # DataBathing generated
    try:
        db_time, db_memory, db_result = measure_execution_time(run_databathing_query, conn, sql_query)
        improvement = ((sql_time - db_time) / sql_time) * 100 if sql_time > 0 else 0
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing Time: {db_time:.4f}s, Memory: {db_memory:.2f}MB")
        print(f"DataBathing Performance Change: {improvement:.1f}%")
    except Exception as e:
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing: Error ({str(e)[:50]}...)")
        improvement = 0
    
    return improvement


def benchmark_window_functions(conn):
    """Benchmark window functions"""
    print("\n" + "="*50)
    print("DUCKDB BENCHMARK: Window Functions")
    print("="*50)
    
    sql_query = """
        SELECT id, name, category, value,
               ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rank,
               LAG(value, 1) OVER (PARTITION BY category ORDER BY id) as prev_value
        FROM test_table 
        WHERE category < 50
    """
    
    # Direct SQL
    sql_time, sql_memory, sql_result = measure_execution_time(run_direct_sql, conn, sql_query)
    
    # For window functions, DuckDB engine may not support all features
    try:
        db_time, db_memory, db_result = measure_execution_time(run_databathing_query, conn, sql_query)
        improvement = ((sql_time - db_time) / sql_time) * 100 if sql_time > 0 else 0
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing Time: {db_time:.4f}s, Memory: {db_memory:.2f}MB")
        print(f"DataBathing Performance Change: {improvement:.1f}%")
    except Exception as e:
        print(f"Direct SQL Time: {sql_time:.4f}s, Memory: {sql_memory:.2f}MB")
        print(f"DataBathing: Not supported ({str(e)[:50]}...)")
        improvement = 0
    
    return improvement


def demonstrate_databathing_conversion():
    """Demonstrate databathing DuckDB conversion"""
    print("\n" + "="*60)
    print("DATABATHING DUCKDB CONVERSION DEMO")
    print("="*60)
    
    sql_query = """
        SELECT category, COUNT(*) as count, AVG(value) as avg_value
        FROM test_table 
        WHERE value > 50000 
        GROUP BY category 
        ORDER BY avg_value DESC
    """
    
    print("Original SQL Query:")
    print(sql_query)
    
    print("\nGenerated DuckDB Code:")
    pipeline = Pipeline(sql_query, engine="duckdb")
    generated_code = pipeline.parse()
    print(generated_code)


def main():
    """Main benchmark execution"""
    print("DataBathing DuckDB Performance Benchmark")
    print("Comparing Direct DuckDB SQL vs DataBathing Generated Code")
    print("="*60)
    
    # Initialize DuckDB connection
    conn = duckdb.connect(':memory:')
    
    try:
        # Create test data
        create_test_data(conn)
        
        # Run benchmarks
        improvements = []
        improvements.append(benchmark_simple_select(conn))
        improvements.append(benchmark_aggregation(conn))
        improvements.append(benchmark_join(conn))
        improvements.append(benchmark_complex_query(conn))
        improvements.append(benchmark_window_functions(conn))
        
        # Filter out failed tests (0 improvement typically means error)
        valid_improvements = [imp for imp in improvements if imp != 0]
        
        # Summary
        if valid_improvements:
            avg_improvement = sum(valid_improvements) / len(valid_improvements)
            print("\n" + "="*60)
            print("DUCKDB PERFORMANCE BENCHMARK SUMMARY")
            print("="*60)
            print(f"Average DataBathing Performance Change: {avg_improvement:.1f}%")
            
            positive_improvements = [imp for imp in valid_improvements if imp > 0]
            negative_improvements = [imp for imp in valid_improvements if imp < 0]
            
            print(f"\nDataBathing was faster in {len(positive_improvements)}/{len(valid_improvements)} test cases")
            print(f"Direct SQL was faster in {len(negative_improvements)}/{len(valid_improvements)} test cases")
            
            print("\nDuckDB Performance Characteristics:")
            print("• Columnar storage optimized for analytics")
            print("• Vectorized execution engine")
            print("• Minimal performance difference between SQL and API")
            print("• Both approaches benefit from same underlying optimizations")
            print("• SQL parsing overhead is minimal in DuckDB")
            
            if abs(avg_improvement) < 10:
                print(f"\nConclusion: Performance is essentially equivalent (±{abs(avg_improvement):.1f}%)")
                print("Choose based on code maintainability and type safety preferences.")
            elif avg_improvement > 0:
                print(f"\nConclusion: DataBathing shows modest improvement (+{avg_improvement:.1f}%)")
            else:
                print(f"\nConclusion: Direct SQL shows modest advantage ({avg_improvement:.1f}%)")
        
        # Demonstrate conversion
        demonstrate_databathing_conversion()
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()