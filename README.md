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

August 2025 - There are [over 900 tests](https://app.circleci.com/pipelines/github/jason-jz-zhu/databathing) with comprehensive coverage. This parser supports advanced SQL features, including:
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


## Generating Spark Code

Generate PySpark Code from SQL queries using the Pipeline. Supports complex queries including set operations.

### Basic Usage
```python
>>> from databathing import Pipeline
>>> pipeline = Pipeline("SELECT * FROM Test WHERE info = 1")
>>> result = pipeline.parse()
>>> print(result)
'final_df = Test\\\n.filter("info = 1")\\\n.selectExpr("a","b","c")\n\n'
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





