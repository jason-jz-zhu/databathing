import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from databathing.pipeline import Pipeline


class TestCTEWithJoins(unittest.TestCase):
    """Test Common Table Expressions (CTEs) with JOIN operations"""

    def test_cte_with_inner_join(self):
        """Test CTE with INNER JOIN"""
        query = """
        WITH step1 AS (
            SELECT customer_id, SUM(amount) as total_spent
            FROM orders
            GROUP BY customer_id
        ),
        step2 AS (
            SELECT customer_id, AVG(rating) as avg_rating
            FROM reviews
            GROUP BY customer_id
        )
        SELECT s1.customer_id, s1.total_spent, s2.avg_rating
        FROM step1 s1
        JOIN step2 s2 ON s1.customer_id = s2.customer_id
        WHERE s1.total_spent > 1000
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify CTE creation
        self.assertIn('step1 = orders', result)
        self.assertIn('step2 = reviews', result)
        
        # Verify JOIN operation exists
        self.assertIn('.join(step2.alias("s2")', result)
        self.assertIn('col("s1.customer_id")==col("s2.customer_id")', result)
        self.assertIn('"inner"', result)
        
        # Verify column references work
        self.assertIn('"s1.customer_id","s1.total_spent","s2.avg_rating"', result)
        
        # Verify WHERE clause
        self.assertIn('.filter("s1.total_spent > 1000")', result)

    def test_cte_with_left_join(self):
        """Test CTE with LEFT JOIN"""
        query = """
        WITH sales AS (
            SELECT customer_id, SUM(amount) as total_sales
            FROM transactions 
            GROUP BY customer_id
        ),
        reviews AS (
            SELECT customer_id, AVG(rating) as avg_rating
            FROM customer_reviews
            GROUP BY customer_id
        )
        SELECT s.customer_id, s.total_sales, r.avg_rating
        FROM sales s
        LEFT JOIN reviews r ON s.customer_id = r.customer_id
        WHERE s.total_sales > 500
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify LEFT JOIN
        self.assertIn('.join(reviews.alias("r")', result)
        self.assertIn('"left"', result)
        
        # Verify column references from both CTEs
        self.assertIn('"s.customer_id","s.total_sales","r.avg_rating"', result)

    def test_cte_with_right_join(self):
        """Test CTE with RIGHT JOIN"""
        query = """
        WITH active_customers AS (
            SELECT customer_id, last_order_date
            FROM customers
            WHERE status = 'active'
        ),
        recent_orders AS (
            SELECT customer_id, COUNT(*) as order_count
            FROM orders
            WHERE order_date >= '2023-01-01'
            GROUP BY customer_id
        )
        SELECT a.customer_id, a.last_order_date, r.order_count
        FROM active_customers a
        RIGHT JOIN recent_orders r ON a.customer_id = r.customer_id
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify RIGHT JOIN
        self.assertIn('.join(recent_orders.alias("r")', result)
        self.assertIn('"right"', result)

    def test_multiple_cte_references(self):
        """Test query that references multiple CTEs without JOIN"""
        query = """
        WITH high_spenders AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
            HAVING SUM(amount) > 1000
        ),
        top_customers AS (
            SELECT customer_id, total
            FROM high_spenders
            ORDER BY total DESC
            LIMIT 10
        )
        SELECT customer_id, total
        FROM top_customers
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify CTE creation
        self.assertIn('high_spenders =', result)
        self.assertIn('top_customers =', result)
        
        # Verify CTE reference (top_customers should reference high_spenders)
        self.assertIn('top_customers = high_spenders', result)

    def test_cte_with_union(self):
        """Test CTE with UNION operations"""
        query = """
        WITH current_year AS (
            SELECT customer_id, amount
            FROM orders_2023
        ),
        previous_year AS (
            SELECT customer_id, amount  
            FROM orders_2022
        )
        SELECT customer_id, amount FROM current_year
        UNION ALL
        SELECT customer_id, amount FROM previous_year
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify UNION operation
        self.assertIn('.union(', result)
        self.assertIn('current_year', result)
        self.assertIn('previous_year', result)

    def test_cte_with_window_functions(self):
        """Test CTE with window functions"""
        query = """
        WITH ranked_orders AS (
            SELECT customer_id, order_date, amount,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rn
            FROM orders
        )
        SELECT customer_id, order_date, amount
        FROM ranked_orders
        WHERE rn = 1
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify window function handling
        self.assertIn('ROW_NUMBER() OVER', result)
        self.assertIn('ranked_orders =', result)
        
        # Verify CTE reference in final query
        self.assertIn('final_df = ranked_orders', result)

    def test_nested_cte_reference(self):
        """Test CTE that references another CTE"""
        query = """
        WITH base_data AS (
            SELECT customer_id, order_date, amount
            FROM orders
            WHERE order_date >= '2023-01-01'
        ),
        monthly_totals AS (
            SELECT customer_id, DATE_TRUNC('month', order_date) as month, SUM(amount) as monthly_total
            FROM base_data
            GROUP BY customer_id, DATE_TRUNC('month', order_date)
        )
        SELECT customer_id, AVG(monthly_total) as avg_monthly_spend
        FROM monthly_totals
        GROUP BY customer_id
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify CTE creation
        self.assertIn('base_data =', result)
        self.assertIn('monthly_totals =', result)
        
        # Verify that monthly_totals references base_data
        self.assertIn('monthly_totals = base_data', result)
        
        # Verify final query references monthly_totals
        self.assertIn('final_df = monthly_totals', result)

    def test_complex_join_with_multiple_conditions(self):
        """Test CTE JOIN with complex ON conditions"""
        query = """
        WITH customer_stats AS (
            SELECT customer_id, region, SUM(amount) as total_spent
            FROM orders
            GROUP BY customer_id, region
        ),
        region_averages AS (
            SELECT region, AVG(amount) as avg_amount
            FROM orders
            GROUP BY region
        )
        SELECT c.customer_id, c.total_spent, r.avg_amount
        FROM customer_stats c
        JOIN region_averages r ON c.region = r.region
        WHERE c.total_spent > r.avg_amount
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify JOIN with proper column references
        self.assertIn('.join(region_averages.alias("r")', result)
        self.assertIn('col("c.region")==col("r.region")', result)
        
        # Verify complex WHERE clause with cross-CTE references
        self.assertIn('"c.total_spent > r.avg_amount"', result)

    def test_multiple_joins_with_base_table_and_ctes(self):
        """Test multiple JOINs between base table and CTEs (regression test for multi-JOIN bug)"""
        query = """
        WITH customer_stats AS (
            SELECT customer_id, SUM(amount) as total_spent
            FROM orders
            WHERE order_date >= '2023-01-01'
            GROUP BY customer_id
        ),
        high_value AS (
            SELECT customer_id
            FROM customer_stats
            WHERE total_spent > 1000
        )
        SELECT c.name, cs.total_spent
        FROM customers c
        JOIN high_value hv ON c.customer_id = hv.customer_id
        JOIN customer_stats cs ON c.customer_id = cs.customer_id
        ORDER BY cs.total_spent DESC
        LIMIT 10
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify both JOINs are generated
        self.assertEqual(result.count('.join('), 2, "Should generate exactly 2 JOIN operations")
        
        # Verify first JOIN (customers to high_value)
        self.assertIn('.join(high_value.alias("hv")', result)
        self.assertIn('col("c.customer_id")==col("hv.customer_id")', result)
        
        # Verify second JOIN (chained to customer_stats)  
        self.assertIn('.join(customer_stats.alias("cs")', result)
        self.assertIn('col("c.customer_id")==col("cs.customer_id")', result)
        
        # Verify column references from all tables work
        self.assertIn('"c.name","cs.total_spent"', result)
        
        # Verify ORDER BY and LIMIT
        self.assertIn('.orderBy(col("cs.total_spent").desc())', result)
        self.assertIn('.limit(10)', result)

    def test_four_way_join_without_aliases(self):
        """Test four-way JOIN without table aliases"""
        query = """
        SELECT *
        FROM t1
        JOIN t2 ON t1.id = t2.id
        JOIN t3 ON t2.id = t3.id 
        LEFT JOIN t4 ON t3.id = t4.id
        """
        pipeline = Pipeline(query)
        result = pipeline.parse()
        
        # Verify all 3 JOINs are generated
        self.assertEqual(result.count('.join('), 3, "Should generate exactly 3 JOIN operations")
        
        # Verify JOIN types
        self.assertIn('"inner"', result)  # Should have inner joins
        self.assertIn('"left"', result)   # Should have one left join
        
        # Verify base table starts the chain
        self.assertIn('final_df = t1.join(', result)


if __name__ == '__main__':
    unittest.main()