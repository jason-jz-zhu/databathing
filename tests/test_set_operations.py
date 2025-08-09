import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from databathing.pipeline import Pipeline


class TestSetOperations(unittest.TestCase):

    def test_union_simple(self):
        """Test simple UNION operation"""
        query = "SELECT a, b FROM table1 UNION SELECT c, d FROM table2"
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).union((table2\\\n.selectExpr("c","d"))).distinct()\n\n'
        self.assertEqual(result, expected)

    def test_union_all_simple(self):
        """Test simple UNION ALL operation"""
        query = "SELECT a, b FROM table1 UNION ALL SELECT c, d FROM table2"
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).union((table2\\\n.selectExpr("c","d")))\n\n'
        self.assertEqual(result, expected)

    def test_intersect_simple(self):
        """Test simple INTERSECT operation"""
        query = "SELECT a, b FROM table1 INTERSECT SELECT c, d FROM table2"
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).intersect((table2\\\n.selectExpr("c","d")))\n\n'
        self.assertEqual(result, expected)

    def test_except_simple(self):
        """Test simple EXCEPT operation"""
        query = "SELECT a, b FROM table1 EXCEPT SELECT c, d FROM table2"
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).exceptAll((table2\\\n.selectExpr("c","d")))\n\n'
        self.assertEqual(result, expected)

    def test_union_with_where(self):
        """Test UNION with WHERE clauses"""
        query = """
        SELECT a, b FROM table1 WHERE a > 10
        UNION ALL 
        SELECT c, d FROM table2 WHERE d < 5
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.filter("a > 10")\\\n.selectExpr("a","b")).union((table2\\\n.filter("d < 5")\\\n.selectExpr("c","d")))\n\n'
        self.assertEqual(result, expected)

    def test_union_with_order_by(self):
        """Test UNION with ORDER BY clause"""
        query = """
        SELECT a, b FROM table1
        UNION ALL 
        SELECT c, d FROM table2
        ORDER BY a
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).union((table2\\\n.selectExpr("c","d")))\n.orderBy(col("a").asc())\n\n'
        self.assertEqual(result, expected)

    def test_union_with_limit(self):
        """Test UNION with LIMIT clause"""
        query = """
        SELECT a, b FROM table1
        UNION 
        SELECT c, d FROM table2
        LIMIT 100
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a","b")).union((table2\\\n.selectExpr("c","d"))).distinct()\n.limit(100)\n\n'
        self.assertEqual(result, expected)

    def test_union_three_tables(self):
        """Test UNION with three tables"""
        query = """
        SELECT a FROM table1
        UNION ALL 
        SELECT b FROM table2
        UNION ALL 
        SELECT c FROM table3
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (table1\\\n.selectExpr("a")).union((table2\\\n.selectExpr("b"))).union((table3\\\n.selectExpr("c")))\n\n'
        self.assertEqual(result, expected)

    def test_intersect_with_where(self):
        """Test INTERSECT with WHERE clauses"""
        query = """
        SELECT id, name FROM users WHERE active = 1
        INTERSECT 
        SELECT id, name FROM customers WHERE status = 'active'
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (users\\\n.filter("active = 1")\\\n.selectExpr("id","name")).intersect((customers\\\n.filter("status = \'active\'")\\\n.selectExpr("id","name")))\n\n'
        self.assertEqual(result, expected)

    def test_except_with_aliases(self):
        """Test EXCEPT with table aliases"""
        query = """
        SELECT u.id, u.name FROM users u WHERE u.age > 18
        EXCEPT 
        SELECT c.user_id, c.full_name FROM customers c WHERE c.deleted = 0
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        expected = 'final_df = (users.alias("u")\\\n.filter("u.age > 18")\\\n.selectExpr("u.id","u.name")).exceptAll((customers.alias("c")\\\n.filter("c.deleted = 0")\\\n.selectExpr("c.user_id","c.full_name")))\n\n'
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()