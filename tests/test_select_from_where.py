from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestSelectFromWhere(TestCase):
    def test_decisive_equailty(self):

        sql = """
        SELECT a, b, c
        FROM Test
        WHERE info = 1
        """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = Test\\\n.filter("info = 1")\\\n.selectExpr("a","b","c")\n\n"""
        self.assertEqual(ans, expected)