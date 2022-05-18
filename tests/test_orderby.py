from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestOrderby(TestCase):
    def test_decisive_equailty(self):

        sql = """
        SELECT id, name
        FROM Test
        ORDER BY id, name
        """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = Test\\\n.selectExpr("id","name")\\\n.orderBy(col("id").asc(),col("name").asc())\n\n"""
        self.assertEqual(ans, expected)