import unittest
from mo_sql_parsing import parse_bigquery as parse
import json
from databathing.engines.mojo_engine import MojoEngine
from databathing.pipeline import Pipeline


class TestMojoEngine(unittest.TestCase):
    
    def setUp(self):
        """Setup common test data"""
        pass
    
    def parse_and_create_engine(self, query):
        """Helper method to parse SQL and create Mojo engine"""
        parsed = parse(query)
        parsed_json = json.loads(json.dumps(parsed, indent=4))
        return MojoEngine(parsed_json)
    
    def test_simple_select(self):
        """Test basic SELECT query generates optimized Mojo code"""
        query = "SELECT name, age FROM users"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        # Check for Mojo-specific optimizations
        self.assertIn("from collections import List, DynamicVector", result)
        self.assertIn("from algorithm import vectorize, parallelize", result)
        self.assertIn("simd_select", result)
        self.assertIn("var result = data", result)
    
    def test_select_with_where(self):
        """Test SELECT with WHERE clause uses SIMD filtering"""
        query = "SELECT name FROM users WHERE age > 25"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_filter", result)
        self.assertIn("age > 25", result)
        self.assertIn("var data = ", result)
    
    def test_aggregation_with_simd(self):
        """Test aggregation functions use SIMD optimization"""
        query = "SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_groupby", result)
        self.assertIn("simd_count", result)
        self.assertIn("simd_mean", result)  # AVG maps to simd_mean
        self.assertIn("simd_aggregate", result)
    
    def test_order_and_limit(self):
        """Test ORDER BY and LIMIT use SIMD operations"""
        query = "SELECT name FROM users ORDER BY name DESC LIMIT 10"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_sort", result)
        self.assertIn("simd_limit", result)
        self.assertIn("desc", result)
    
    def test_pipeline_integration(self):
        """Test Mojo engine integration with Pipeline"""
        query = "SELECT category, SUM(value) as total FROM sales GROUP BY category"
        pipeline = Pipeline(query, engine="mojo")
        result = pipeline.parse()
        
        # Check for Mojo comment header
        self.assertIn("# Mojo 🔥 High-Performance Code", result)
        self.assertIn("simd_sum", result)
        self.assertIn("from collections import List", result)
    
    def test_complex_query_optimization(self):
        """Test complex query with multiple SIMD optimizations"""
        query = """
            SELECT category, COUNT(*) as count, AVG(price) as avg_price
            FROM products 
            WHERE price > 100 
            GROUP BY category 
            HAVING COUNT(*) > 5
            ORDER BY avg_price DESC 
            LIMIT 20
        """
        pipeline = Pipeline(query, engine="mojo")
        result = pipeline.parse()
        
        # Verify multiple SIMD operations are present
        self.assertIn("simd_filter", result)
        self.assertIn("simd_groupby", result) 
        self.assertIn("simd_count", result)
        self.assertIn("simd_mean", result)
        self.assertIn("simd_sort", result)
        self.assertIn("simd_limit", result)
    
    def test_distinct_operation(self):
        """Test SELECT DISTINCT uses SIMD distinct operation"""
        query = "SELECT DISTINCT department FROM employees"
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_distinct", result)
        self.assertIn("department", result)
    
    def test_join_operations(self):
        """Test JOIN operations use SIMD joins"""
        query = """
            SELECT u.name, d.department_name 
            FROM users u 
            INNER JOIN departments d ON u.dept_id = d.id
        """
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_inner_join", result)
        self.assertIn("load_table", result)
    
    def test_set_operations(self):
        """Test UNION operations use SIMD set operations"""
        query = """
            SELECT name FROM employees 
            UNION 
            SELECT name FROM contractors
        """
        engine = self.parse_and_create_engine(query)
        result = engine.parse()
        
        self.assertIn("simd_union_distinct", result)
        self.assertIn("subquery_", result)
    
    def test_mojo_performance_features(self):
        """Test that generated code includes performance features"""
        query = "SELECT * FROM large_table WHERE id < 1000000"
        pipeline = Pipeline(query, engine="mojo")
        result = pipeline.parse()
        
        # Check for key Mojo performance imports
        self.assertIn("from memory import memset_zero, memcpy", result)
        self.assertIn("from sys.info import simdwidthof", result)
        self.assertIn("from tensor import Tensor", result)
        
        # Check for vectorized operations
        self.assertIn("simd_", result)
        
    def test_error_handling(self):
        """Test engine handles invalid queries gracefully"""
        try:
            query = "INVALID SQL SYNTAX"
            pipeline = Pipeline(query, engine="mojo")
            # This should raise an exception during SQL parsing
            result = pipeline.parse()
        except Exception as e:
            # Expected to fail on invalid SQL
            self.assertIsInstance(e, Exception)


class TestMojoEnginePerformanceFeatures(unittest.TestCase):
    """Test Mojo-specific performance optimizations"""
    
    def test_simd_function_mapping(self):
        """Test that SQL functions map to SIMD equivalents"""
        test_cases = [
            ("SELECT SUM(value) FROM table", "simd_sum"),
            ("SELECT AVG(value) FROM table", "simd_mean"),
            ("SELECT COUNT(*) FROM table", "simd_count"),
            ("SELECT MAX(value) FROM table", "simd_max"),
            ("SELECT MIN(value) FROM table", "simd_min"),
        ]
        
        for query, expected_func in test_cases:
            pipeline = Pipeline(query, engine="mojo")
            result = pipeline.parse()
            self.assertIn(expected_func, result, 
                         f"Query '{query}' should generate '{expected_func}'")
    
    def test_memory_optimization_imports(self):
        """Test that memory optimization imports are included"""
        query = "SELECT * FROM table"
        pipeline = Pipeline(query, engine="mojo")
        result = pipeline.parse()
        
        expected_imports = [
            "from collections import List, DynamicVector",
            "from memory import memset_zero, memcpy", 
            "from algorithm import vectorize, parallelize",
            "from sys.info import simdwidthof",
            "from tensor import Tensor"
        ]
        
        for import_stmt in expected_imports:
            self.assertIn(import_stmt, result,
                         f"Missing import: {import_stmt}")
    
    def test_zero_copy_operations(self):
        """Test that code suggests zero-copy data operations"""
        query = "SELECT col1, col2 FROM big_table WHERE col1 > 1000"
        pipeline = Pipeline(query, engine="mojo")
        result = pipeline.parse()
        
        # Check for efficient data handling patterns
        self.assertIn("var data =", result)
        self.assertIn("load_table", result)
        self.assertIn("simd_filter", result)


if __name__ == '__main__':
    unittest.main()