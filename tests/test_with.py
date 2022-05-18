from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestWith(TestCase):
    maxDiff = None
    def test_decisive_equailty(self):

        sql = """
            with step1 as (
                select firstname, id from df
            ), step2 as (
                select gender, salary, id from df
            ), step3 as (
                select 
                    s1.id, s1.firstname, s2.gender, s2.salary
                from step1 as s1
                inner join step2 as s2
                on s1.id = s2.id
            )
            select
                *
            from step3
        """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """step1 = df\\\n.selectExpr("firstname","id")\n\nstep2 = df\\\n.selectExpr("gender","salary","id")\n\nstep3 = step1.alias("s1").join(step2.alias("s2"), col("s1.id")==col("s2.id"), "inner")\\\n.selectExpr("s1.id","s1.firstname","s2.gender","s2.salary")\n\nfinal_df = step3\\\n.selectExpr("*")\n\n"""
        self.assertEqual(ans, expected)