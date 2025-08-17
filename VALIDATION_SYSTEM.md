# DataBathing Code Validation System

A comprehensive code validation and scoring system for DataBathing-generated PySpark and DuckDB code.

## Overview

The validation system ensures that generated code is syntactically correct, follows best practices, and receives quality scores to help users assess code reliability before execution.

## Features

### ✅ **Syntax Validation**
- **PySpark**: Validates Python syntax and DataFrame method chains
- **DuckDB**: Validates SQL syntax and structure within `duckdb.sql()` calls
- Catches syntax errors before code execution

### 📊 **Code Quality Scoring (0-100)**
- **Overall Score**: Weighted combination of all metrics with letter grades (A-F)
- **Syntax Score**: Correctness validation (pass/fail → 100/0)
- **Complexity Score**: Method chaining depth, nesting, operations count (lower is better)
- **Readability Score**: Line length, formatting, variable naming
- **Performance Score**: Anti-pattern detection, optimization opportunities  
- **Maintainability Score**: Based on complexity and readability

### 🔧 **Best Practices Checking**
- **PySpark**: Warns about `collect()`, performance anti-patterns, deprecated methods
- **DuckDB**: Flags `SELECT *`, missing `LIMIT`, inefficient patterns
- Provides actionable suggestions for improvement

### 🎯 **Performance Analysis**
- Detects common performance bottlenecks
- Suggests query optimizations
- Identifies potential Cartesian products and expensive operations

## Quick Start

### Basic Usage with Pipeline

```python
from databathing import Pipeline

# Enable validation (default: True)
pipeline = Pipeline("SELECT name FROM users WHERE age > 25", 
                   engine="spark", validate=True)

# Get code with validation
result = pipeline.parse_with_validation()

print(f"Generated Code: {result['code']}")
print(f"Score: {result['score']}/100 (Grade: {result['grade']})")
print(f"Valid: {result['is_valid']}")

# Access detailed report
report = result['validation_report']
print(f"Issues: {report.error_count} errors, {report.warning_count} warnings")
```

### Direct Code Validation

```python
from databathing import validate_code

# Validate any generated code directly
code = 'final_df = users.filter("age > 25").selectExpr("name")'
report = validate_code(code, engine="spark", original_sql="SELECT name FROM users WHERE age > 25")

print(f"Overall Score: {report.metrics.overall_score:.1f}/100")
print(f"Grade: {report.get_grade()}")

# Show issues and suggestions
for issue in report.issues:
    print(f"⚠️ {issue.message}")
    if issue.suggestion:
        print(f"💡 {issue.suggestion}")
```

## Validation Metrics Breakdown

### Syntax Score (40% weight)
- **100**: Code is syntactically correct and executable
- **0**: Syntax errors detected, code will fail

### Complexity Score (20% weight, inverted)
- **Low (0-20)**: Simple, straightforward operations
- **Medium (21-50)**: Moderate complexity with some chaining/nesting
- **High (51-100)**: Complex queries with deep nesting, many operations

### Readability Score (20% weight)
- **Excellent (90-100)**: Well-formatted, meaningful names, proper spacing
- **Good (70-89)**: Generally readable with minor formatting issues
- **Poor (0-69)**: Hard to read, long lines, unclear structure

### Performance Score (10% weight)
- **Excellent (90-100)**: Optimal patterns, no anti-patterns detected
- **Good (70-89)**: Generally efficient with minor concerns  
- **Poor (0-69)**: Multiple performance issues detected

### Maintainability Score (10% weight)
- Calculated from complexity and readability
- Higher scores indicate easier maintenance

## Engine-Specific Features

### PySpark Validator

**Syntax Validation:**
- Parses Python AST to validate method chains
- Checks for valid Spark DataFrame operations
- Validates imports and function calls

**Performance Checks:**
- Warns about `collect()` on large datasets
- Detects expensive operations like multiple `count()` calls
- Identifies RDD usage over DataFrame API

**Best Practices:**
- Suggests `col()` function for column references
- Recommends proper error handling
- Checks for deprecated methods

### DuckDB Validator

**Syntax Validation:**
- Extracts SQL from `duckdb.sql("...")` calls
- Uses `sqlparse` for SQL structure validation
- Validates table names and keyword order

**Performance Checks:**
- Flags `SELECT *` without `LIMIT`
- Detects leading wildcards in `LIKE` patterns
- Warns about correlated subqueries

**Best Practices:**
- Recommends explicit column selection
- Suggests proper NULL handling
- Checks for efficient JOIN patterns

## Example Validation Results

### High-Quality Code (Grade A)
```python
# Input SQL
"SELECT department, AVG(salary) as avg_sal FROM employees WHERE salary > 50000 GROUP BY department LIMIT 10"

# PySpark Output (Score: 92/100)
final_df = employees\
.filter("salary > 50000")\
.groupBy("department")\
.agg(avg(col("salary")).alias("avg_sal"))\
.selectExpr("department","avg_sal")\
.limit(10)

# DuckDB Output (Score: 98/100)  
result = duckdb.sql("SELECT department,avg(salary) AS avg_sal FROM employees WHERE salary > 50000 GROUP BY department LIMIT 10")
```

### Problem Code (Grade D)
```python
# Problematic PySpark (Score: 45/100)
final_df = large_table.collect()  # ❌ collect() on large data
# Issues: Performance risk, no filtering

# Problematic DuckDB (Score: 38/100)
result = duckdb.sql("SELECT * FROM large_table ORDER BY created_at")  # ❌ SELECT * + ORDER BY without LIMIT
# Issues: Memory usage, performance risk
```

## Integration with Existing Workflow

### Automatic Validation (Recommended)
```python
# Validation runs automatically
pipeline = Pipeline(sql_query, engine="spark")  # validate=True by default
code = pipeline.parse()  # Validation report stored internally

# Check results
if not pipeline.is_code_valid():
    print("⚠️ Code has errors!")
    print(pipeline.get_validation_report())
```

### Manual Validation (Optional)
```python
# Disable auto-validation for performance
pipeline = Pipeline(sql_query, engine="spark", validate=False)
code = pipeline.parse()

# Validate later if needed
from databathing import validate_code
report = validate_code(code, "spark", sql_query)
```

## Testing

Run validation system tests:
```bash
export PYTHONPATH=.
python -m unittest tests.test_validation_system -v
```

Run interactive demo:
```bash
python validation_demo.py
```

## Configuration

### Validation Settings
```python
# In Pipeline constructor
Pipeline(query, engine="spark", validate=True)  # Enable/disable validation

# Custom validation thresholds (future enhancement)
validator = ValidatorFactory.create_validator("spark")
validator.configure(min_score=80, strict_syntax=True)
```

## Architecture

```
Pipeline (validate=True)
    ↓
ValidatorFactory.create_validator(engine)
    ↓
Engine-Specific Validator (SparkValidator | DuckDBValidator)
    ↓
ValidationReport (scores, issues, suggestions)
    ↓
User Access Methods (get_code_score, get_validation_report, etc.)
```

### Class Hierarchy
- `CodeValidator` (abstract base)
  - `SparkValidator` (PySpark-specific)
  - `DuckDBValidator` (DuckDB-specific)
- `ValidationReport` (results container)
- `ValidatorFactory` (validator creation)

## Performance Impact

- **Validation Overhead**: ~10-50ms per query (depending on complexity)
- **Memory Usage**: Minimal additional memory for report objects
- **Recommendation**: Keep validation enabled for development, consider disabling for high-frequency production use

## Future Enhancements

### Planned Features
- **Execution Validation**: Actually run generated code in sandbox for runtime validation
- **Performance Benchmarking**: Measure actual execution time and memory usage
- **Custom Rules**: User-defined validation rules and scoring weights
- **IDE Integration**: Real-time validation in code editors
- **Batch Validation**: Validate multiple queries efficiently

### Additional Engines
- **PostgreSQL Validator**: For SQL generation targeting PostgreSQL
- **SQLite Validator**: For lightweight SQL validation
- **BigQuery Validator**: For Google BigQuery SQL dialect

## Troubleshooting

### Common Issues

**`ModuleNotFoundError: No module named 'sqlparse'`**
```bash
pip install sqlparse
```

**Validation always returns valid=True**
- Check if syntax validation is correctly implemented
- Verify test cases with intentionally invalid code

**Low scores for good code**
- Review scoring weights and thresholds
- Check for false positives in best practices rules

### Debug Information
```python
# Get detailed validation info
report = pipeline.get_validation_report()
print(report.to_dict())  # Full report as dictionary
print(str(report))       # Human-readable summary
```

## Contributing

To add support for new validation rules:

1. **Extend Base Classes**: Add methods to `CodeValidator` base class
2. **Implement in Engines**: Add concrete implementations in engine validators  
3. **Update Scoring**: Modify scoring algorithms and weights
4. **Add Tests**: Create comprehensive test cases
5. **Update Documentation**: Document new features and examples

The validation system is designed to be extensible and can easily accommodate new engines, rules, and scoring mechanisms.