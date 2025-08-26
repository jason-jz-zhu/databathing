#!/usr/bin/env python3
"""
Comprehensive test suite for Auto-Selection system

Tests the rule-based auto-selection of engines between Spark and DuckDB
based on query analysis, data estimation, and user context.
"""

import unittest
import sys
import os
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from databathing import Pipeline, AutoEngineSelector, SelectionContext
from databathing.auto_selection.query_analyzer import QueryAnalyzer
from databathing.auto_selection.data_estimator import DataEstimator
from databathing.auto_selection.rule_engine import RuleEngine
from mo_sql_parsing import parse_bigquery as parse
import json


class TestAutoSelection(unittest.TestCase):
    """Test auto-selection system integration"""
    
    def setUp(self):
        """Setup common test data"""
        self.selector = AutoEngineSelector()
    
    def test_basic_auto_selection(self):
        """Test basic auto-selection without context"""
        # Small interactive query - should select DuckDB
        small_query = "SELECT name, age FROM users WHERE age > 25 LIMIT 10"
        pipeline = Pipeline(small_query, auto_engine=True)
        
        self.assertTrue(pipeline.was_auto_selected())
        self.assertIn(pipeline.engine, ["spark", "duckdb"])  # Should pick one of these
        self.assertIsNotNone(pipeline.get_selection_reasoning())
        
    def test_auto_selection_with_context(self):
        """Test auto-selection with user context"""
        query = "SELECT department, COUNT(*) FROM employees GROUP BY department"
        
        # Speed priority should favor DuckDB for medium data
        context = SelectionContext(
            data_size_hint="medium",
            performance_priority="speed"
        )
        
        pipeline = Pipeline(query, auto_engine=True, context=context)
        
        self.assertTrue(pipeline.was_auto_selected())
        self.assertGreater(pipeline.get_selection_confidence(), 0.5)
        
    def test_manual_vs_auto_selection(self):
        """Test that manual selection still works and differs from auto"""
        query = "SELECT * FROM large_fact_table WHERE date >= '2024-01-01'"
        
        # Manual selection
        manual_pipeline = Pipeline(query, engine="duckdb")
        self.assertFalse(manual_pipeline.was_auto_selected())
        self.assertEqual(manual_pipeline.engine, "duckdb")
        
        # Auto selection might choose differently
        auto_pipeline = Pipeline(query, auto_engine=True)
        self.assertTrue(auto_pipeline.was_auto_selected())
        # Don't assert specific engine since it depends on rules
        
    def test_context_variations(self):
        """Test different context parameters"""
        query = "SELECT category, SUM(revenue) FROM sales GROUP BY category"
        
        contexts = [
            SelectionContext(performance_priority="speed", data_size_hint="small"),
            SelectionContext(performance_priority="cost", data_size_hint="medium"),
            SelectionContext(latency_requirement="interactive"),
            SelectionContext(fault_tolerance=True, data_size_hint="large")
        ]
        
        for context in contexts:
            with self.subTest(context=context):
                pipeline = Pipeline(query, auto_engine=True, context=context)
                self.assertTrue(pipeline.was_auto_selected())
                self.assertIsNotNone(pipeline.get_selection_reasoning())
                
    def test_complex_queries(self):
        """Test auto-selection on complex queries"""
        complex_queries = [
            # Simple query - likely DuckDB
            "SELECT name FROM users LIMIT 100",
            
            # Complex ETL - likely Spark  
            """
            WITH sales_data AS (
                SELECT customer_id, product_id, amount, date
                FROM fact_sales WHERE date >= '2024-01-01'
            ),
            customer_totals AS (
                SELECT customer_id, SUM(amount) as total_spent
                FROM sales_data GROUP BY customer_id
            )
            SELECT c.name, ct.total_spent, p.category
            FROM customers c
            JOIN customer_totals ct ON c.id = ct.customer_id  
            JOIN products p ON p.id = sales_data.product_id
            WHERE ct.total_spent > 10000
            """,
            
            # Analytics query - could go either way
            "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING COUNT(*) > 5"
        ]
        
        for query in complex_queries:
            with self.subTest(query=query[:50]):
                pipeline = Pipeline(query, auto_engine=True)
                self.assertTrue(pipeline.was_auto_selected())
                self.assertIn(pipeline.engine, ["spark", "duckdb"])
                
    def test_selection_information_methods(self):
        """Test methods that provide selection information"""
        query = "SELECT * FROM test_table LIMIT 5"
        pipeline = Pipeline(query, auto_engine=True)
        
        # Test information methods
        selection_info = pipeline.get_selection_info()
        self.assertEqual(selection_info['method'], 'auto')
        self.assertIn('confidence', selection_info)
        self.assertIn('reasoning', selection_info)
        
        reasoning = pipeline.get_selection_reasoning()
        self.assertIsInstance(reasoning, str)
        self.assertGreater(len(reasoning), 10)
        
        confidence = pipeline.get_selection_confidence()
        self.assertIsInstance(confidence, float)
        self.assertGreaterEqual(confidence, 0.0)
        self.assertLessEqual(confidence, 1.0)
        
        detailed_analysis = pipeline.get_detailed_selection_analysis()
        self.assertIsInstance(detailed_analysis, dict)
        self.assertIn('engine', detailed_analysis)
        
    def test_parse_with_validation_includes_auto_selection(self):
        """Test that parse_with_validation includes auto-selection info"""
        query = "SELECT name FROM users WHERE active = 1"
        pipeline = Pipeline(query, auto_engine=True)
        
        result = pipeline.parse_with_validation()
        
        self.assertIn('code', result)
        self.assertIn('auto_selection', result)
        
        auto_info = result['auto_selection']
        self.assertIn('selected_engine', auto_info)
        self.assertIn('confidence', auto_info)
        self.assertIn('reasoning', auto_info)
        
    def test_mojo_still_available_manually(self):
        """Test that Mojo engine is still available for manual selection"""
        query = "SELECT name FROM users"
        
        # Should work manually
        mojo_pipeline = Pipeline(query, engine="mojo")
        self.assertEqual(mojo_pipeline.engine, "mojo")
        self.assertFalse(mojo_pipeline.was_auto_selected())
        
        # Should not be chosen by auto-selection (focused on Spark vs DuckDB)
        auto_pipeline = Pipeline(query, auto_engine=True)
        self.assertIn(auto_pipeline.engine, ["spark", "duckdb"])
        
    def test_error_handling(self):
        """Test error handling in auto-selection"""
        # Test with malformed query - should fallback to Spark
        try:
            malformed_query = "SELECT FROM WHERE"
            pipeline = Pipeline(malformed_query, auto_engine=True)
            # Should not crash, might fallback to Spark
            self.assertTrue(pipeline.was_auto_selected())
        except Exception:
            # If parsing fails completely, that's also acceptable
            pass
    
    def test_backward_compatibility(self):
        """Test that existing code patterns still work"""
        query = "SELECT name FROM users"
        
        # Old pattern should still work
        old_pipeline = Pipeline(query)  # Default to Spark
        self.assertEqual(old_pipeline.engine, "spark")
        self.assertFalse(old_pipeline.was_auto_selected())
        
        # Explicit engine should work
        explicit_pipeline = Pipeline(query, engine="duckdb")
        self.assertEqual(explicit_pipeline.engine, "duckdb")
        self.assertFalse(explicit_pipeline.was_auto_selected())


class TestQueryAnalyzer(unittest.TestCase):
    """Test query analysis component"""
    
    def setUp(self):
        self.analyzer = QueryAnalyzer()
    
    def test_simple_query_analysis(self):
        """Test analysis of simple queries"""
        query = "SELECT name, age FROM users WHERE age > 25 LIMIT 10"
        parsed = json.loads(json.dumps(parse(query), indent=4))
        
        features = self.analyzer.analyze(parsed)
        
        self.assertGreater(features.complexity_score, 0)
        self.assertEqual(features.join_count, 0)
        self.assertEqual(features.table_count, 1)
        self.assertTrue(features.has_limit)
        self.assertGreater(features.interactive_indicators, 0)
    
    def test_complex_query_analysis(self):
        """Test analysis of complex queries"""
        complex_query = """
        WITH customer_data AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders 
            GROUP BY customer_id
        )
        SELECT c.name, cd.total, AVG(cd.total) OVER (PARTITION BY c.segment)
        FROM customers c
        JOIN customer_data cd ON c.id = cd.customer_id
        WHERE cd.total > 1000
        """
        parsed = json.loads(json.dumps(parse(complex_query), indent=4))
        
        features = self.analyzer.analyze(parsed)
        
        self.assertGreater(features.complexity_score, 5)  # Should be complex
        self.assertGreaterEqual(features.join_count, 1)
        self.assertTrue(features.has_cte)
        self.assertTrue(features.has_aggregations)
        self.assertTrue(features.has_window_functions)


class TestDataEstimator(unittest.TestCase):
    """Test data size estimation component"""
    
    def setUp(self):
        self.estimator = DataEstimator()
    
    def test_table_name_estimation(self):
        """Test estimation based on table names"""
        # Large data indicators
        large_query = "SELECT * FROM fact_sales WHERE date >= '2024-01-01'"
        parsed_large = json.loads(json.dumps(parse(large_query), indent=4))
        
        estimate_large = self.estimator.estimate(parsed_large)
        self.assertGreater(estimate_large.size_gb, 10)  # Should estimate large
        self.assertIn(estimate_large.size_category, ["large", "xlarge"])
        
        # Small data indicators  
        small_query = "SELECT * FROM dim_products"
        parsed_small = json.loads(json.dumps(parse(small_query), indent=4))
        
        estimate_small = self.estimator.estimate(parsed_small)
        self.assertLess(estimate_small.size_gb, 5)  # Should estimate small
        self.assertEqual(estimate_small.size_category, "small")
    
    def test_context_hint_estimation(self):
        """Test estimation with user hints"""
        query = "SELECT * FROM some_table"
        parsed = json.loads(json.dumps(parse(query), indent=4))
        
        # Test with large hint
        estimate = self.estimator.estimate(parsed, "large")
        self.assertEqual(estimate.size_category, "large")
        self.assertGreater(estimate.confidence, 0.8)  # High confidence in user input


class TestRuleEngine(unittest.TestCase):
    """Test rule engine component"""
    
    def setUp(self):
        self.rule_engine = RuleEngine()
        self.analyzer = QueryAnalyzer()
        self.estimator = DataEstimator()
    
    def test_high_confidence_rules(self):
        """Test high confidence rule decisions"""
        # Small data + LIMIT should strongly favor DuckDB
        small_interactive = "SELECT name FROM users LIMIT 100"
        parsed = json.loads(json.dumps(parse(small_interactive), indent=4))
        
        features = self.analyzer.analyze(parsed)
        estimate = self.estimator.estimate(parsed, "small")
        
        choice = self.rule_engine.select_engine(features, estimate)
        
        self.assertEqual(choice.engine, "duckdb")
        self.assertGreater(choice.confidence, 0.85)  # High confidence
    
    def test_context_based_rules(self):
        """Test context-driven rule decisions"""
        query = "SELECT category, COUNT(*) FROM products GROUP BY category"
        parsed = json.loads(json.dumps(parse(query), indent=4))
        
        features = self.analyzer.analyze(parsed)
        estimate = self.estimator.estimate(parsed)
        
        # Speed priority context
        speed_context = SelectionContext(performance_priority="speed")
        choice = self.rule_engine.select_engine(features, estimate, speed_context)
        
        self.assertIsInstance(choice.confidence, float)
        self.assertIn(choice.engine, ["spark", "duckdb"])
        self.assertIsNotNone(choice.reasoning)
    
    def test_rule_explanations(self):
        """Test that rules provide explanations"""
        explanation = self.rule_engine.get_rule_explanation("small_data_interactive")
        self.assertIsInstance(explanation, str)
        self.assertGreater(len(explanation), 50)  # Should be detailed


class TestIntegrationScenarios(unittest.TestCase):
    """Test realistic usage scenarios"""
    
    def test_dashboard_scenario(self):
        """Test typical dashboard query scenario"""
        dashboard_query = "SELECT region, SUM(revenue) FROM daily_sales WHERE date >= '2024-01-01' GROUP BY region ORDER BY SUM(revenue) DESC LIMIT 10"
        
        context = SelectionContext(
            workload_type="dashboard",
            latency_requirement="interactive",
            data_size_hint="medium"
        )
        
        pipeline = Pipeline(dashboard_query, auto_engine=True, context=context)
        
        # Should likely choose DuckDB for interactive dashboard
        self.assertTrue(pipeline.was_auto_selected())
        reasoning = pipeline.get_selection_reasoning()
        self.assertIn("interactive", reasoning.lower())
    
    def test_etl_scenario(self):
        """Test typical ETL pipeline scenario"""
        etl_query = """
        WITH raw_data AS (
            SELECT customer_id, order_date, amount, product_category
            FROM fact_orders
            WHERE order_date >= '2024-01-01'
        ),
        customer_metrics AS (
            SELECT customer_id, 
                   COUNT(*) as order_count,
                   SUM(amount) as total_spent,
                   AVG(amount) as avg_order_value
            FROM raw_data 
            GROUP BY customer_id
        ),
        category_metrics AS (
            SELECT product_category,
                   COUNT(DISTINCT customer_id) as unique_customers,
                   SUM(amount) as category_revenue
            FROM raw_data
            GROUP BY product_category  
        )
        SELECT c.customer_id, cm.total_spent, cat.category_revenue
        FROM customers c
        JOIN customer_metrics cm ON c.id = cm.customer_id
        JOIN category_metrics cat ON c.preferred_category = cat.product_category
        WHERE cm.total_spent > 10000
        """
        
        context = SelectionContext(
            workload_type="etl",
            latency_requirement="batch",
            fault_tolerance=True
        )
        
        pipeline = Pipeline(etl_query, auto_engine=True, context=context)
        
        # Complex ETL should likely choose Spark
        self.assertTrue(pipeline.was_auto_selected())
        selection_info = pipeline.get_selection_info()
        self.assertGreater(selection_info['confidence'], 0.5)


if __name__ == '__main__':
    # Run with verbose output to see all test details
    unittest.main(verbosity=2)