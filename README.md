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

May 2022 - There are [over 900 tests](https://app.circleci.com/pipelines/github/jason-jz-zhu/databathing). This parser is good enough for basic usage, including:
* `SELECT` feature
* `FROM` feature
* `INNER` JOIN and LEFT JOIN feature
* `ON` feature
* `WHERE` feature
* `GROUP BY` feature
* `HAVING` feature
* `ORDER BY` feature
* `AGG` feature
* WINDOWS FUNCTION feature (`SUM`, `AVG`, `MAX`, `MIN`, `MEAN`, `COUNT`)
* ALIAS NAME feature
* `WITH` STATEMENT feature

## Install

    pip install databathing


## Generating Spark Code

You may also generate PySpark Code from the a given SQL Query. This is done by the Pipeline, which is in Version 1 state (May2022).

    >>> from databathing import Pipeline
    >>> pipeline = Pipeline("SELECT * FROM Test WHERE info = 1")
    'final_df = Test\\\n.filter("info = 1")\\\n.selectExpr("a","b","c")\n\n'

## Contributing

In the event that the databathing is not working for you, you can help make this better but simply pasting your sql (or JSON) into a new issue. Extra points if you describe the problem. Even more points if you submit a PR with a test. If you also submit a fix, then you also have my gratitude. 

Please follow this blog to update verion - https://circleci.com/blog/publishing-a-python-package/


### Run Tests

See [the tests directory](https://github.com/jason-jz-zhu/databathing/tree/develop/tests) for instructions running tests, or writing new ones.

## Version Changes


### Version 1

*May 2022*

Features and Functionalities - PySpark Version
* `SELECT` feature
* `FROM` feature
* `INNER` JOIN and LEFT JOIN feature
* `ON` feature
* `WHERE` feature
* `GROUP BY` feature
* `HAVING` feature
* `ORDER BY` feature
* `AGG` feature
* WINDOWS FUNCTION feature (`SUM`, `AVG`, `MAX`, `MIN`, `MEAN`, `COUNT`)
* ALIAS NAME feature
* `WITH` STATEMENT feature





