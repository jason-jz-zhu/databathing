#!/usr/bin/env python3
"""
Advanced DataBathing Validation Features Demo

Demonstrates the enhanced validation system with:
- Validation result caching for performance
- Async validation for non-blocking UX
- Custom validation rules API
- Batch processing capabilities
"""

import asyncio
import time
from databathing import Pipeline
from databathing.validation import (
    validate_code, get_validation_cache, AsyncValidator,
    add_regex_rule, add_function_rule, get_custom_rules_registry,
    ValidationIssue, ValidationStatus
)


def demo_caching_performance():
    """Demonstrate validation caching for performance improvement"""
    print("🚀 Validation Caching Performance Demo")
    print("=" * 50)
    
    code = """
    final_df = users\\
    .filter("age > 25 AND department IS NOT NULL")\\
    .groupBy("department")\\
    .agg(avg(col("salary")).alias("avg_salary"))\\
    .orderBy(col("avg_salary").desc())\\
    .limit(10)
    """
    
    # First validation (cache miss)
    start_time = time.time()
    result1 = validate_code(code, "spark", use_cache=True)
    cache_miss_time = time.time() - start_time
    
    # Second validation (cache hit)
    start_time = time.time()
    result2 = validate_code(code, "spark", use_cache=True)
    cache_hit_time = time.time() - start_time
    
    print(f"Cache Miss Time: {cache_miss_time*1000:.1f}ms")
    print(f"Cache Hit Time: {cache_hit_time*1000:.1f}ms")
    print(f"Performance Improvement: {cache_miss_time/cache_hit_time:.1f}x faster")
    
    cache = get_validation_cache()
    print(f"Cache Size: {cache.size()} entries")
    print(f"Validation Score: {result1.metrics.overall_score:.1f}/100")
    print()


async def demo_async_validation():
    """Demonstrate async validation for non-blocking operations"""
    print("⚡ Async Validation Demo")
    print("=" * 30)
    
    async_validator = AsyncValidator()
    
    # Simulate validating multiple queries async
    queries = [
        ("SELECT name FROM users WHERE age > 25", "spark"),
        ("SELECT department, COUNT(*) FROM employees GROUP BY department", "duckdb"),
        ("SELECT * FROM products ORDER BY price DESC LIMIT 5", "spark"),
        ("SELECT AVG(salary) FROM employees WHERE department = 'Engineering'", "duckdb")
    ]
    
    print(f"Validating {len(queries)} queries asynchronously...")
    
    # Create validation requests
    tasks = []
    for sql, engine in queries:
        pipeline = Pipeline(sql, engine=engine, validate=False)  # Don't validate in pipeline
        code = pipeline.parse()
        task = async_validator.validate_async(code, engine, sql)
        tasks.append(task)
    
    # Wait for all validations to complete
    start_time = time.time()
    results = await asyncio.gather(*tasks)
    total_time = time.time() - start_time
    
    print(f"✅ Completed {len(results)} validations in {total_time*1000:.1f}ms")
    
    for i, result in enumerate(results):
        print(f"  Query {i+1}: {result.metrics.overall_score:.1f}/100 (Grade: {result.get_grade()})")
    
    async_validator.shutdown()
    print()


def demo_custom_rules():
    """Demonstrate custom validation rules API"""
    print("🔧 Custom Validation Rules Demo")
    print("=" * 40)
    
    # Add regex-based rule
    add_regex_rule(
        name="no_production_secrets",
        pattern=r'password|secret|api_key|token',
        message="Potential hardcoded secrets detected",
        suggestion="Use environment variables or secure key management",
        severity="failed"  # This will fail validation
    )
    
    # Add function-based rule
    def check_query_complexity(code: str, engine_type: str):
        issues = []
        if engine_type == "spark":
            # Count method chains
            method_count = code.count('.')
            if method_count > 10:
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message=f"Complex query with {method_count} method calls - consider breaking into steps",
                    rule="query_complexity",
                    suggestion="Split into intermediate DataFrames for better readability"
                ))
        return issues
    
    add_function_rule(
        name="query_complexity",
        description="Check for overly complex queries",
        check_function=check_query_complexity
    )
    
    # Test custom rules
    test_cases = [
        {
            "name": "Clean Code",
            "code": 'final_df = users.filter("age > 25").select("name")',
            "engine": "spark"
        },
        {
            "name": "Secret Detection",
            "code": 'final_df = users.filter("password != \'secret123\'")',
            "engine": "spark"
        },
        {
            "name": "Complex Query", 
            "code": 'final_df = users.filter("x").select("y").groupBy("z").agg("w").filter("v").orderBy("u").limit(10)',
            "engine": "spark"
        }
    ]
    
    for test in test_cases:
        print(f"Testing: {test['name']}")
        result = validate_code(test['code'], test['engine'])
        
        print(f"  Score: {result.metrics.overall_score:.1f}/100 (Grade: {result.get_grade()})")
        print(f"  Valid: {'✅' if result.is_valid else '❌'}")
        
        for issue in result.issues:
            if issue.rule in ["no_production_secrets", "query_complexity"]:
                severity_icon = "❌" if issue.severity == ValidationStatus.FAILED else "⚠️"
                print(f"  {severity_icon} Custom Rule: {issue.message}")
        print()
    
    # List all custom rules
    registry = get_custom_rules_registry()
    print("📋 Registered Custom Rules:")
    for name, description in registry.list_rules().items():
        print(f"  • {name}: {description}")
    print()


def demo_batch_processing():
    """Demonstrate batch validation with progress tracking"""
    print("📦 Batch Validation Demo")
    print("=" * 30)
    
    # Sample queries for batch processing
    batch_queries = [
        "SELECT name FROM customers WHERE country = 'USA'",
        "SELECT product_id, SUM(quantity) FROM sales GROUP BY product_id",
        "SELECT * FROM inventory WHERE stock_level < 10",
        "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id HAVING COUNT(*) > 5",
        "SELECT AVG(price) FROM products WHERE category = 'Electronics'"
    ]
    
    print(f"Processing {len(batch_queries)} queries in batch...")
    
    def progress_callback(completed, total):
        percentage = (completed / total) * 100
        print(f"  Progress: {completed}/{total} ({percentage:.0f}%)")
    
    # Create validation requests
    requests = []
    for sql in batch_queries:
        # Generate code for both engines
        spark_pipeline = Pipeline(sql, engine="spark", validate=False)
        duckdb_pipeline = Pipeline(sql, engine="duckdb", validate=False)
        
        requests.extend([
            {
                "code": spark_pipeline.parse(),
                "engine_type": "spark", 
                "original_sql": sql,
                "use_cache": True
            },
            {
                "code": duckdb_pipeline.parse(),
                "engine_type": "duckdb",
                "original_sql": sql,
                "use_cache": True
            }
        ])
    
    # Run batch validation
    from databathing.validation import validate_with_progress
    
    start_time = time.time()
    results = validate_with_progress(requests, progress_callback)
    total_time = time.time() - start_time
    
    print(f"✅ Batch completed in {total_time*1000:.1f}ms")
    
    # Analyze results
    spark_scores = [r.metrics.overall_score for r in results[::2]]  # Even indices
    duckdb_scores = [r.metrics.overall_score for r in results[1::2]]  # Odd indices
    
    print(f"Average Spark Score: {sum(spark_scores)/len(spark_scores):.1f}/100")
    print(f"Average DuckDB Score: {sum(duckdb_scores)/len(duckdb_scores):.1f}/100")
    print()


async def main():
    """Run all advanced validation demos"""
    print("🎯 DataBathing Advanced Validation Features")
    print("=" * 55)
    print()
    
    # Performance caching demo
    demo_caching_performance()
    
    # Async validation demo
    await demo_async_validation()
    
    # Custom rules demo
    demo_custom_rules()
    
    # Batch processing demo
    demo_batch_processing()
    
    print("🎉 All advanced validation features demonstrated!")
    print()
    print("💡 Key Takeaways:")
    print("   • Caching improves validation performance significantly")
    print("   • Async validation enables non-blocking user experiences")
    print("   • Custom rules allow domain-specific validation logic")
    print("   • Batch processing efficiently handles multiple validations")
    print("   • All features integrate seamlessly with existing Pipeline API")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()