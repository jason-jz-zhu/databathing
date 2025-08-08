# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataBathing is a SQL-to-PySpark code generator that parses SQL queries into JSON and generates equivalent PySpark DataFrame operations. The project aims to combine the development speed of SQL/YAML with the performance benefits of Spark.

## Core Architecture

The project has two main components:

1. **py_bathing** (`databathing/py_bathing.py`) - Core SQL parser that converts parsed JSON queries into PySpark DataFrame operations
2. **Pipeline** (`databathing/pipeline.py`) - High-level interface that handles complex queries including WITH statements and generates complete PySpark code

### Key Components:
- Uses `mo-sql-parsing` library with BigQuery parser (`parse_bigquery`)
- Supports SQL features: SELECT, FROM, JOIN (INNER/LEFT/RIGHT), WHERE, GROUP BY, HAVING, ORDER BY, window functions, WITH statements
- Generates PySpark DataFrame method chains (filter, selectExpr, join, groupBy, etc.)

## Development Commands

### Running Tests
```bash
# Install dependencies
pip install -r requirements.txt
pip install -r tests/requirements.txt

# Set Python path and run all tests
export PYTHONPATH=.
python -m unittest discover tests
```

### Package Installation
```bash
pip install databathing
```

## Project Structure

- `databathing/` - Main package
  - `__init__.py` - Exports Pipeline and py_bathing classes
  - `pipeline.py` - High-level SQL-to-PySpark pipeline with WITH statement support
  - `py_bathing.py` - Core SQL parsing and PySpark generation logic
  - `v1.py` - Legacy/experimental code
- `tests/` - Comprehensive test suite (900+ tests)
  - Test files cover specific SQL features: select_from_where, join_on, groupby_having, windows, etc.

## Usage Patterns

Basic usage:
```python
from databathing import pipeline
pipeline = pipeline.Pipeline("SELECT * FROM Test WHERE info = 1")
spark_code = pipeline.parse()
```

The parser generates PySpark code as strings with method chaining:
```python
final_df = Test\
.filter("info = 1")\
.selectExpr("a","b","c")
```

## Testing Approach

- Uses Python's unittest framework
- Each test file focuses on specific SQL functionality
- Tests compare generated PySpark code strings against expected output
- Run individual test files: `python -m unittest tests.test_select_from_where`

## Dependencies

- `mo-sql-parsing` - SQL parsing library (uses BigQuery dialect)
- Standard Python libraries: json, copy, curses

## Development Notes

- The py_bathing class contains debug print statements that output parsed queries
- The code generates PySpark DataFrame operations as string concatenations
- Supports complex queries with subqueries via WITH statements
- Window functions and aggregations are handled through the agg_list configuration

## Git Commit Guidelines

- Simply don't include the "Co-Authored-By: Claude" line in commit messages