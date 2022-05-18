from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestJoinOn(TestCase):
    def test_decisive_equailty(self):

        sql =  """
        SELECT 
            t1.id as id, 
            t1.val as t1_val,
            t2.val as t1_val
        FROM Test t1
        LEFT JOIN Test t2
        ON t1.id = t2.id
        """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = Test.alias("t1").join(Test.alias("t2"), col("t1.id")==col("t2.id"), "left")\\\n.selectExpr("t1.id AS id","t1.val AS t1_val","t2.val AS t1_val")\n\n"""
        self.assertEqual(ans, expected)