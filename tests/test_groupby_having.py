from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline

class TestGroupbyHaving(TestCase):
    def test_decisive_equailty(self):

        sql = """
        select 
            product_id, 
            count(*) cnt
        from Test
        group by product_id
        having cnt > 1
        """

        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = Test\\\n.groupBy("product_id")\\\n.agg(count(col("*")).alias("cnt"))\\\n.filter("cnt > 1")\\\n.selectExpr("product_id","cnt")\n\n"""
        self.assertEqual(ans, expected)