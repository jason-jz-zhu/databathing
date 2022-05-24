from __future__ import absolute_import, division, unicode_literals

from unittest import TestCase
from mo_sql_parsing import parse

import json
from databathing.pipeline import Pipeline


# python -m unittest discover tests

class TestDistinct(TestCase):
    def test_decisive_equailty(self):

        sql = """
            SELECT 
                struct(firstname as firstname, lastname as lastname) as name
            FROM df
            """
        pipeline = Pipeline(sql)
        ans = pipeline.parse()
        expected = """final_df = df\\\n.selectExpr("STRUCT(firstname AS firstname, lastname AS lastname) AS name")\n\n"""
        self.assertEqual(ans, expected)