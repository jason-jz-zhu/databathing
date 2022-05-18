from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestDistinct(TestCase):
    def test_decisive_equailty(self):

        sql = """
        SELECT distinct a, b, c
        FROM Test
        """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = Test\\\n.selectExpr("a","b","c")\\\n.distinct()\n\n"""
        self.assertEqual(ans, expected)