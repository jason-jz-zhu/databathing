# DuckDB Engine for DataBathing

DataBathing now supports DuckDB as an alternative execution engine alongside PySpark. This allows you to generate DuckDB Python code from SQL queries instead of PySpark DataFrame operations.

## Quick Start

```python
from databathing import Pipeline

# Generate DuckDB code
query = "SELECT name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10"
pipeline = Pipeline(query, engine="duckdb")
duckdb_code = pipeline.parse()
print(duckdb_code)
# Output: result = duckdb.sql("SELECT name,age FROM users WHERE age > 25 ORDER BY name ASC LIMIT 10")

# Compare with PySpark (default)
spark_pipeline = Pipeline(query, engine="spark")
spark_code = spark_pipeline.parse()
print(spark_code)
# Output: final_df = users\.filter("age > 25")\.selectExpr("name","age")\.orderBy(col("name").asc())\.limit(10)
```

## Supported SQL Features

### Basic Operations
- ✅ SELECT with column selection and aliases
- ✅ FROM with table names and aliases  
- ✅ WHERE clauses with conditions
- ✅ ORDER BY with ASC/DESC
- ✅ LIMIT clauses
- ✅ SELECT DISTINCT

### Advanced Operations  
- ✅ GROUP BY with aggregations (COUNT, SUM, AVG, etc.)
- ✅ HAVING clauses
- ✅ WITH statements (CTEs)
- ✅ Window functions
- ⚠️ Set operations (UNION, INTERSECT, EXCEPT) - basic support
- ⚠️ JOINs - generates SQL rather than relational API

## Engine Comparison

| Feature | PySpark Engine | DuckDB Engine |
|---------|----------------|---------------|
| **Output Style** | Method chaining | SQL strings |
| **Performance** | Distributed processing | In-memory columnar |
| **Dependencies** | PySpark, Hadoop ecosystem | DuckDB only |
| **File Formats** | Parquet, Delta, etc. | CSV, Parquet, JSON |
| **Memory Usage** | Cluster memory | Single machine memory |
| **Setup Complexity** | High (cluster setup) | Low (pip install) |

## Code Generation Patterns

### Simple Queries
```python
# SQL
query = "SELECT department, COUNT(*) as total FROM employees GROUP BY department"

# DuckDB Output
result = duckdb.sql("SELECT department,count(*) AS total FROM employees GROUP BY department")

# PySpark Output  
final_df = employees\
.groupBy("department")\
.agg(count(col("*")).alias("total"))\
.selectExpr("department","total")
```

### Complex Queries with Multiple Clauses
```python
# SQL
query = """
SELECT department, AVG(salary) as avg_sal 
FROM employees 
WHERE salary > 50000 
GROUP BY department 
HAVING COUNT(*) > 5
ORDER BY avg_sal DESC
LIMIT 3
"""

# DuckDB Output
result = duckdb.sql("SELECT department,avg(salary) AS avg_sal FROM employees WHERE salary > 50000 GROUP BY department HAVING COUNT(*) > 5 ORDER BY avg_sal DESC LIMIT 3")
```

## Installation and Usage

### Prerequisites
```bash
# For DuckDB engine
pip install duckdb

# For PySpark engine (existing)
pip install pyspark
```

### Basic Usage
```python
from databathing import Pipeline

# Method 1: Specify engine in constructor
pipeline = Pipeline("SELECT * FROM table", engine="duckdb")
code = pipeline.parse()

# Method 2: Default to PySpark (backward compatible)
pipeline = Pipeline("SELECT * FROM table")  # engine="spark" 
code = pipeline.parse()
```

### Using Generated Code

#### DuckDB
```python
import duckdb

# Execute generated DuckDB code
result = duckdb.sql("SELECT name,age FROM users WHERE age > 25")

# Convert to different formats
df = result.df()          # Pandas DataFrame
arrow = result.arrow()    # PyArrow Table  
data = result.fetchall()  # Python objects
```

#### PySpark
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataBathing").getOrCreate()

# Execute generated PySpark code
final_df = users\
.filter("age > 25")\
.selectExpr("name","age")
```

## Architecture

The DuckDB engine is built using the same architecture as the PySpark engine:

```
Pipeline (engine="duckdb")
    ↓
DuckDBEngine (extends BaseEngine)  
    ↓
SQL String Generation
    ↓
duckdb.sql("...") calls
```

### Engine Selection Flow
1. **Pipeline.__init__(query, engine)** validates and stores engine choice
2. **Pipeline._get_engine_instance()** factory method creates appropriate engine
3. **Engine.parse()** generates engine-specific code
4. **Pipeline.parse()** handles WITH statements and final code assembly

## Limitations and Future Improvements

### Current Limitations
- Set operations generate basic SQL rather than optimized relational API calls
- Complex JOINs fall back to SQL generation instead of using DuckDB's relational join methods
- Window functions need more comprehensive testing

### Future Enhancements
- **Relational API Integration**: Use DuckDB's method chaining API for better performance
- **Advanced Set Operations**: Optimize UNION/INTERSECT/EXCEPT handling  
- **Join Optimization**: Implement proper relational join methods
- **Extension Support**: Add support for DuckDB extensions
- **Performance Benchmarks**: Add comparative performance testing

## Testing

Run DuckDB-specific tests:
```bash
export PYTHONPATH=.
python -m unittest tests.test_duckdb_engine -v
```

Integration testing:
```bash
python test_duckdb_integration.py
```

## Contributing

To add support for new SQL features in the DuckDB engine:

1. **Extend BaseEngine**: Add abstract methods for new functionality
2. **Implement in DuckDBEngine**: Add concrete implementations  
3. **Update SparkEngine**: Maintain backward compatibility
4. **Add Tests**: Create comprehensive test cases
5. **Update Documentation**: Document new features and examples

The engine architecture makes it easy to add support for additional databases (PostgreSQL, SQLite, etc.) in the future.