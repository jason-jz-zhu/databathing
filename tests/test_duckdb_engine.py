import unittest
from mo_sql_parsing import parse_bigquery as parse
import json
from databathing.engines.duckdb_engine import DuckDBEngine


class TestDuckDBEngine(unittest.TestCase):
    
    def setUp(self):
        """Setup common test data"""
        pass
    
    def parse_and_create_engine(self, query):
        """Helper method to parse SQL and create DuckDB engine"""
        parsed = parse(query)
        parsed_json = json.loads(json.dumps(parsed, indent=4))
        return DuckDBEngine(parsed_json)
    
    def test_simple_select(self):
        """Test basic SELECT query"""
        query = "SELECT name, age FROM users"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        expected = 'duckdb.sql("SELECT name,age FROM users")'
        self.assertEqual(result, expected)
    
    def test_select_with_where(self):
        """Test SELECT with WHERE clause"""
        query = "SELECT name FROM users WHERE age > 25"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("WHERE age > 25", result)
        self.assertIn("SELECT name", result)
        self.assertTrue(result.startswith('duckdb.sql("'))
        self.assertTrue(result.endswith('")'))
    
    def test_select_with_order_limit(self):
        """Test SELECT with ORDER BY and LIMIT"""
        query = "SELECT name FROM users ORDER BY name DESC LIMIT 5"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("ORDER BY name DESC", result)
        self.assertIn("LIMIT 5", result)
    
    def test_group_by_aggregation(self):
        """Test GROUP BY with aggregation"""
        query = "SELECT department, COUNT(*) as total FROM employees GROUP BY department"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("GROUP BY department", result)
        # Check for count function (case insensitive)
        self.assertTrue("count(*)" in result.lower() or "COUNT(*)" in result)
    
    def test_distinct_select(self):
        """Test SELECT DISTINCT"""
        query = "SELECT DISTINCT department FROM employees"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("SELECT DISTINCT", result)
        self.assertIn("department", result)
    
    def test_having_clause(self):
        """Test HAVING clause"""
        query = "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("GROUP BY department", result)
        self.assertIn("HAVING COUNT(*) > 5", result)


if __name__ == '__main__':
    unittest.main()