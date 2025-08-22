# More SQL Parsing!

[![PyPI Latest Release](https://img.shields.io/pypi/v/databathing.svg)](https://pypi.org/project/databathing/)
[![Build Status](https://circleci.com/gh/jason-jz-zhu/databathing/tree/main.svg?style=svg)](https://app.circleci.com/pipelines/github/jason-jz-zhu/databathing)


Parse SQL into JSON so we can translate it for other datastores!

[See changes](https://github.com/jason-jz-zhu/databathing#version-changes)


## Problem Statement

After converting from sql to spark, data engineers need to write the spark code for ETL pipeline instead of using YAML(SQL) which can improve the performance of ETL job, but it still makes the ETL development longer than before. 

Then we have one question: can we have a solution which can have both good calculation performance (Spark) and quick to develop (YAML - SQL)?

YES, we have !!!

## Objectives

We plan to combine the benefits from Spark and YAML (SQL) to create the platform or library to develop the ETL pipeline. 


## Project Status

August 2025 - There are [over 900 tests](https://app.circleci.com/pipelines/github/jason-jz-zhu/databathing) with comprehensive coverage. This parser supports advanced SQL features and includes a **code validation system** with quality scoring, including:
* `SELECT` feature (including `SELECT DISTINCT`)
* `FROM` feature with table aliases
* `INNER`, `LEFT`, and `RIGHT` JOIN features
* `ON` feature for join conditions
* `WHERE` feature with complex conditions
* `GROUP BY` feature with aggregations
* `HAVING` feature for post-aggregation filtering
* `ORDER BY` feature with ASC/DESC sorting
* `LIMIT` feature for result limiting
* `AGG` feature with aggregation functions (`SUM`, `AVG`, `MAX`, `MIN`, `MEAN`, `COUNT`, `COLLECT_LIST`, `COLLECT_SET`)
* WINDOWS FUNCTION feature with partitioning and ordering
* ALIAS NAME feature for tables and columns
* `WITH` STATEMENT feature (Common Table Expressions/CTEs)
* **SET OPERATIONS feature** - `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
* String functions (`SPLIT`, `STRUCT` creation)
* Complex subqueries and nested operations

## Install

    pip install databathing

### Optional Dependencies
For full functionality:
```bash
pip install duckdb      # For DuckDB engine support
pip install sqlparse   # For code validation system
```


## Generating Spark Code

Generate PySpark Code from SQL queries using the Pipeline. Supports complex queries including set operations.

### Basic Usage
```python
>>> from databathing import Pipeline
>>> pipeline = Pipeline("SELECT * FROM Test WHERE info = 1")
>>> result = pipeline.parse()
>>> print(result)
'final_df = Test\\\n.filter("info = 1")\\\n.selectExpr("a","b","c")\n\n'

# With validation (enabled by default)
>>> pipeline = Pipeline("SELECT name FROM users WHERE age > 25", validate=True)
>>> result = pipeline.parse_with_validation()
>>> print(f"Code: {result['code'].strip()}")
>>> print(f"Quality Score: {result['score']}/100 (Grade: {result['grade']})")
'Code: final_df = users.filter("age > 25").selectExpr("name")'
'Quality Score: 97.2/100 (Grade: A)'
```

### Set Operations Example (New in v0.3.0!)
```python
>>> from databathing import Pipeline
>>> sql_query = """
    SELECT customer_id, name FROM active_customers
    UNION ALL
    SELECT customer_id, name FROM inactive_customers
    WHERE status = 'churned'
    ORDER BY name
    LIMIT 100
"""
>>> pipeline = Pipeline(sql_query)
>>> result = pipeline.parse()
>>> print(result)
'final_df = (active_customers\\\n.selectExpr("customer_id","name")).union((inactive_customers\\\n.filter("status = \'churned\'")\\\n.selectExpr("customer_id","name")))\n.orderBy(col("name").asc())\n.limit(100)\n\n'
```

### Supported Set Operations
- **UNION** - Combines results and removes duplicates
- **UNION ALL** - Combines results keeping all records  
- **INTERSECT** - Returns only records that exist in both queries
- **EXCEPT** - Returns records from first query that don't exist in second

## DuckDB Engine Support (New!)

DataBathing now supports DuckDB as an alternative execution engine alongside PySpark. Generate DuckDB Python code from SQL queries for in-memory columnar processing.

### Quick Start with DuckDB
```python
>>> from databathing import Pipeline
>>> 
>>> # Generate DuckDB code with validation
>>> query = "SELECT name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10"
>>> pipeline = Pipeline(query, engine="duckdb", validate=True)
>>> result = pipeline.parse_with_validation()
>>> print(f"Code: {result['code']}")
>>> print(f"Quality Score: {result['score']}/100 (Grade: {result['grade']})")
'Code: result = duckdb.sql("SELECT name,age FROM users WHERE age > 25 ORDER BY name ASC LIMIT 10")'
'Quality Score: 98.8/100 (Grade: A)'
>>> 
>>> # Compare with PySpark (default behavior)
>>> spark_pipeline = Pipeline(query, engine="spark")  # or just Pipeline(query)
>>> spark_code = spark_pipeline.parse()
>>> print(spark_code)
'final_df = users\\.filter("age > 25")\\.selectExpr("name","age")\\.orderBy(col("name").asc())\\.limit(10)'
```

### Engine Comparison

| Feature | PySpark Engine | DuckDB Engine |
|---------|----------------|---------------|
| **Output Style** | Method chaining | SQL strings |
| **Performance** | Distributed processing | In-memory columnar |
| **Dependencies** | PySpark, Hadoop ecosystem | DuckDB only |
| **Setup Complexity** | High (cluster setup) | Low (pip install) |
| **Memory Usage** | Cluster memory | Single machine memory |
| **Best For** | Big data, distributed workloads | Analytics, fast queries, prototyping |

### Complex Query Example
```python
>>> from databathing import Pipeline
>>> 
>>> complex_query = """
    SELECT department, AVG(salary) as avg_salary, COUNT(*) as emp_count
    FROM employees 
    WHERE salary > 50000 
    GROUP BY department 
    HAVING COUNT(*) > 5
    ORDER BY avg_salary DESC
    LIMIT 3
"""
>>> 
>>> # DuckDB Output - clean SQL
>>> duckdb_pipeline = Pipeline(complex_query, engine="duckdb")
>>> print(duckdb_pipeline.parse())
'result = duckdb.sql("SELECT department,avg(salary) AS avg_salary,count(*) AS emp_count FROM employees WHERE salary > 50000 GROUP BY department HAVING COUNT(*) > 5 ORDER BY avg_salary DESC LIMIT 3")'
>>> 
>>> # PySpark Output - method chaining
>>> spark_pipeline = Pipeline(complex_query, engine="spark")
>>> print(spark_pipeline.parse())
'final_df = employees\\.filter("salary > 50000")\\.groupBy("department")\\.agg(avg(col("salary")).alias("avg_salary"),count(col("*")).alias("emp_count"))\\.filter("COUNT(*) > 5")\\.selectExpr("department","avg_salary","emp_count")\\.orderBy(col("avg_salary").desc())\\.limit(3)'
```

### Using Generated DuckDB Code
```python
import duckdb

# Execute the generated code
result = duckdb.sql("SELECT name,age FROM users WHERE age > 25 ORDER BY name ASC LIMIT 10")

# Convert to different formats
pandas_df = result.df()          # Pandas DataFrame
arrow_table = result.arrow()     # PyArrow Table  
python_data = result.fetchall()  # Python objects
```

### DuckDB Engine Features
- ✅ **All SQL features supported**: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- ✅ **Aggregation functions**: COUNT, SUM, AVG, MIN, MAX, etc.
- ✅ **SELECT DISTINCT** support
- ✅ **WITH statements** (Common Table Expressions)
- ✅ **Complex expressions** and aliases
- ⚠️ **Set operations** (UNION, INTERSECT, EXCEPT) - basic support
- ⚠️ **JOINs** - generates SQL rather than relational API

### Prerequisites for DuckDB Engine
```bash
pip install duckdb  # Required for DuckDB engine
pip install databathing  # This package
```

## Code Validation & Quality Scoring (New!)

DataBathing now includes a comprehensive code validation system that automatically checks generated PySpark and DuckDB code for syntax correctness, performance issues, and best practices. Get instant feedback with quality scores (0-100) and actionable suggestions.

### ✅ Key Features
- **Syntax Validation**: Ensures generated code is syntactically correct before execution
- **Quality Scoring**: 0-100 scores with letter grades (A-F) based on 5 metrics
- **Performance Analysis**: Detects common bottlenecks and anti-patterns  
- **Best Practices**: Suggests improvements for maintainable, efficient code
- **Multi-Engine Support**: Works with both PySpark and DuckDB engines

### Quick Start with Validation

#### Basic Usage with Pipeline
```python
from databathing import Pipeline

# Enable validation (default behavior)
pipeline = Pipeline("SELECT name, age FROM users WHERE age > 25", 
                   engine="spark", validate=True)

# Get code with validation results
result = pipeline.parse_with_validation()

print(f"Generated Code: {result['code']}")
print(f"Quality Score: {result['score']}/100 (Grade: {result['grade']})")
print(f"Code Valid: {result['is_valid']}")

# Output:
# Generated Code: final_df = users.filter("age > 25").selectExpr("name","age")
# Quality Score: 97.2/100 (Grade: A)
# Code Valid: True
```

#### Accessing Detailed Validation Reports
```python
# Get comprehensive validation details
report = pipeline.get_validation_report()

print(f"Syntax Score: {report.metrics.syntax_score}/100")
print(f"Performance Score: {report.metrics.performance_score}/100") 
print(f"Readability Score: {report.metrics.readability_score}/100")

# Show any issues or suggestions
for issue in report.issues:
    print(f"⚠️ {issue.message}")
    if issue.suggestion:
        print(f"💡 {issue.suggestion}")
```

#### Direct Code Validation
```python
from databathing import validate_code

# Validate any generated code directly
code = 'final_df = users.filter("age > 25").selectExpr("name")'
report = validate_code(code, engine="spark", original_sql="SELECT name FROM users WHERE age > 25")

print(f"Overall Score: {report.metrics.overall_score:.1f}/100")
print(f"Grade: {report.get_grade()}")
print(f"Valid: {report.is_valid}")
```

### Quality Scoring System

DataBathing evaluates code across 5 dimensions:

| Metric | Weight | Description |
|--------|--------|-------------|
| **Syntax** | 40% | Code correctness and executability |
| **Complexity** | 20% | Method chaining depth, nesting (lower is better) |
| **Readability** | 20% | Formatting, naming, code clarity |
| **Performance** | 10% | Anti-pattern detection, optimization opportunities |
| **Maintainability** | 10% | Long-term code maintenance ease |

**Grade Scale:** A (90-100) • B (80-89) • C (70-79) • D (60-69) • F (0-59)

### Validation Examples

#### High-Quality Code (Grade A)
```python
# Input SQL
query = "SELECT department, AVG(salary) as avg_sal FROM employees WHERE salary > 50000 GROUP BY department LIMIT 10"

# PySpark Output (Score: 94/100, Grade A)
pipeline = Pipeline(query, engine="spark", validate=True)
result = pipeline.parse_with_validation()
print(result['code'])
# final_df = employees\
# .filter("salary > 50000")\
# .groupBy("department")\
# .agg(avg(col("salary")).alias("avg_sal"))\
# .selectExpr("department","avg_sal")\
# .limit(10)

# DuckDB Output (Score: 98/100, Grade A)  
pipeline = Pipeline(query, engine="duckdb", validate=True)
result = pipeline.parse_with_validation()
print(result['code'])
# result = duckdb.sql("SELECT department,avg(salary) AS avg_sal FROM employees WHERE salary > 50000 GROUP BY department LIMIT 10")
```

#### Problematic Code with Warnings (Grade D)
```python
# Query that generates performance issues
problematic_query = "SELECT * FROM large_table ORDER BY created_at"

pipeline = Pipeline(problematic_query, engine="spark", validate=True)
result = pipeline.parse_with_validation()

print(f"Score: {result['score']}/100 (Grade: {result['grade']})")
# Score: 45/100 (Grade: D)

# View specific issues
for issue in result['validation_report'].issues:
    print(f"⚠️ {issue.message}")
# ⚠️ Performance: SELECT * can be inefficient on large tables
# ⚠️ Best Practice: Consider adding LIMIT to ORDER BY queries
```

#### Engine Comparison with Validation
```python
sql = "SELECT department, COUNT(*) as total FROM employees GROUP BY department"

# Compare validation scores between engines
for engine in ["spark", "duckdb"]:
    pipeline = Pipeline(sql, engine=engine, validate=True)
    result = pipeline.parse_with_validation()
    print(f"{engine.upper()}: {result['score']}/100 (Grade: {result['grade']})")

# Output:
# SPARK: 89/100 (Grade: B)
# DUCKDB: 95/100 (Grade: A)
```

### Installation Requirements

For full validation functionality:
```bash
pip install databathing
pip install sqlparse  # Required for DuckDB SQL validation
```

### Validation Configuration

```python
# Enable/disable validation (default: True)
pipeline = Pipeline(query, engine="spark", validate=True)

# For high-frequency production use, consider disabling for performance
pipeline = Pipeline(query, engine="spark", validate=False)
```

### Performance Notes
- **Validation Overhead**: ~10-50ms per query depending on complexity
- **Recommendation**: Keep enabled for development, consider disabling for high-frequency production use
- **Memory Impact**: Minimal additional memory for validation reports

## Contributing

In the event that the databathing is not working for you, you can help make this better but simply pasting your sql (or JSON) into a new issue. Extra points if you describe the problem. Even more points if you submit a PR with a test. If you also submit a fix, then you also have my gratitude. 

Please follow this blog to update verion - https://circleci.com/blog/publishing-a-python-package/


### Run Tests

See [the tests directory](https://github.com/jason-jz-zhu/databathing/tree/develop/tests) for instructions running tests, or writing new ones.

## Version Changes

### Version 0.3.0

*August 2025*

**Major New Feature: SQL Set Operations**
* **`UNION`** - Combines results from multiple queries and removes duplicates
* **`UNION ALL`** - Combines results from multiple queries keeping all records
* **`INTERSECT`** - Returns only records that exist in all queries
* **`EXCEPT`** - Returns records from first query that don't exist in second query

**Enhanced Features:**
* Complex subqueries with WHERE clauses in set operations
* ORDER BY and LIMIT support on combined results
* Table aliases and complex expressions in set operations
* Seamless integration with existing WITH statements (CTEs)
* Support for multiple chained set operations
* Comprehensive test coverage with 10 additional test cases

**Technical Improvements:**
* Enhanced parsing engine with `_set_operation_analyze()` method
* Improved FROM clause handling for nested set operations
* Maintained full backward compatibility
* Added `__version__` constant for programmatic version checking

### Version 1 (Legacy)

*May 2022*

Core Features and Functionalities - PySpark Version
* `SELECT` feature (including `SELECT DISTINCT`)
* `FROM` feature with table aliases
* `INNER`, `LEFT`, and `RIGHT` JOIN features
* `ON` feature for join conditions
* `WHERE` feature with complex conditions
* `GROUP BY` feature with aggregations
* `HAVING` feature for post-aggregation filtering
* `ORDER BY` feature with ASC/DESC sorting
* `LIMIT` feature for result limiting
* `AGG` feature with aggregation functions
* WINDOWS FUNCTION feature (`SUM`, `AVG`, `MAX`, `MIN`, `MEAN`, `COUNT`)
* ALIAS NAME feature for tables and columns
* `WITH` STATEMENT feature (Common Table Expressions)





