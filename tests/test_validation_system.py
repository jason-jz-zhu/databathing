import unittest
from databathing import Pipeline
from databathing.validation.validator_factory import ValidatorFactory, validate_code
from databathing.validation.spark_validator import SparkValidator
from databathing.validation.duckdb_validator import DuckDBValidator
from databathing.validation.validation_report import ValidationStatus


class TestValidationSystem(unittest.TestCase):
    
    def setUp(self):
        """Setup test data"""
        self.simple_sql = "SELECT name, age FROM users WHERE age > 25"
        self.complex_sql = """
            SELECT department, AVG(salary) as avg_sal, COUNT(*) as emp_count
            FROM employees 
            WHERE salary > 50000 
            GROUP BY department 
            HAVING COUNT(*) > 5
            ORDER BY avg_sal DESC
            LIMIT 3
        """
        
    def test_validator_factory(self):
        """Test validator factory creates correct validators"""
        spark_validator = ValidatorFactory.create_validator("spark")
        duckdb_validator = ValidatorFactory.create_validator("duckdb")
        
        self.assertIsInstance(spark_validator, SparkValidator)
        self.assertIsInstance(duckdb_validator, DuckDBValidator)
        
        # Test unsupported engine
        with self.assertRaises(ValueError):
            ValidatorFactory.create_validator("unsupported")
    
    def test_spark_validation_valid_code(self):
        """Test Spark validator with valid code"""
        valid_spark_code = 'final_df = users.filter("age > 25").selectExpr("name", "age")'
        
        report = validate_code(valid_spark_code, "spark", self.simple_sql)
        
        self.assertEqual(report.engine_type, "spark")
        self.assertTrue(report.is_valid)
        self.assertGreater(report.metrics.syntax_score, 0)
        self.assertGreater(report.metrics.overall_score, 0)
    
    def test_spark_validation_invalid_code(self):
        """Test Spark validator with invalid code"""
        invalid_spark_code = 'final_df = users.invalid_method("age > 25"'  # Missing closing parenthesis
        
        report = validate_code(invalid_spark_code, "spark", self.simple_sql)
        
        self.assertEqual(report.engine_type, "spark")
        self.assertFalse(report.is_valid)
        self.assertEqual(report.metrics.syntax_score, 0)
        self.assertGreater(report.error_count, 0)
    
    def test_duckdb_validation_valid_code(self):
        """Test DuckDB validator with valid code"""
        valid_duckdb_code = 'result = duckdb.sql("SELECT name, age FROM users WHERE age > 25")'
        
        report = validate_code(valid_duckdb_code, "duckdb", self.simple_sql)
        
        self.assertEqual(report.engine_type, "duckdb")
        self.assertTrue(report.is_valid)
        self.assertGreater(report.metrics.syntax_score, 0)
        self.assertGreater(report.metrics.overall_score, 0)
    
    def test_duckdb_validation_invalid_code(self):
        """Test DuckDB validator with invalid code"""
        invalid_duckdb_code = 'result = duckdb.sql("SELECT name FROM WHERE age > 25")'  # Missing table
        
        report = validate_code(invalid_duckdb_code, "duckdb", self.simple_sql)
        
        self.assertEqual(report.engine_type, "duckdb")
        self.assertFalse(report.is_valid)
        self.assertEqual(report.metrics.syntax_score, 0)
    
    def test_pipeline_with_validation_spark(self):
        """Test Pipeline integration with Spark validation"""
        pipeline = Pipeline(self.simple_sql, engine="spark", validate=True)
        result = pipeline.parse_with_validation()
        
        self.assertIn('code', result)
        self.assertIn('validation_report', result)
        self.assertIn('score', result)
        self.assertIn('grade', result)
        self.assertIn('is_valid', result)
        
        # Check that validation ran
        self.assertIsNotNone(result['validation_report'])
        self.assertIsNotNone(result['score'])
        self.assertIn(result['grade'], ['A', 'B', 'C', 'D', 'F'])
    
    def test_pipeline_with_validation_duckdb(self):
        """Test Pipeline integration with DuckDB validation"""
        pipeline = Pipeline(self.simple_sql, engine="duckdb", validate=True)
        result = pipeline.parse_with_validation()
        
        self.assertIn('code', result)
        self.assertIn('validation_report', result)
        self.assertIn('score', result)
        self.assertIn('grade', result)
        self.assertIn('is_valid', result)
        
        # Check that validation ran
        self.assertIsNotNone(result['validation_report'])
        self.assertIsNotNone(result['score'])
    
    def test_pipeline_without_validation(self):
        """Test Pipeline with validation disabled"""
        pipeline = Pipeline(self.simple_sql, engine="spark", validate=False)
        code = pipeline.parse()
        
        # Validation should not run
        self.assertIsNone(pipeline.get_validation_report())
        self.assertIsNone(pipeline.get_code_score())
        self.assertIsNone(pipeline.get_code_grade())
    
    def test_complex_query_validation(self):
        """Test validation with complex query"""
        # Test Spark
        spark_pipeline = Pipeline(self.complex_sql, engine="spark", validate=True)
        spark_result = spark_pipeline.parse_with_validation()
        
        self.assertTrue(spark_result['is_valid'])
        self.assertGreater(spark_result['score'], 0)
        
        # Test DuckDB
        duckdb_pipeline = Pipeline(self.complex_sql, engine="duckdb", validate=True)
        duckdb_result = duckdb_pipeline.parse_with_validation()
        
        self.assertTrue(duckdb_result['is_valid'])
        self.assertGreater(duckdb_result['score'], 0)
    
    def test_validation_metrics_calculation(self):
        """Test that all metrics are calculated properly"""
        pipeline = Pipeline(self.simple_sql, engine="spark", validate=True)
        pipeline.parse()
        
        report = pipeline.get_validation_report()
        self.assertIsNotNone(report)
        
        # All metrics should be calculated
        self.assertGreaterEqual(report.metrics.syntax_score, 0)
        self.assertGreaterEqual(report.metrics.complexity_score, 0)
        self.assertGreaterEqual(report.metrics.readability_score, 0)
        self.assertGreaterEqual(report.metrics.performance_score, 0)
        self.assertGreaterEqual(report.metrics.maintainability_score, 0)
        self.assertGreaterEqual(report.metrics.overall_score, 0)
    
    def test_best_practices_check(self):
        """Test best practices checking"""
        # Create code with potential issues
        problematic_spark_code = 'final_df = users.filter("age > 25").collect()'  # collect() is flagged
        
        report = validate_code(problematic_spark_code, "spark")
        
        # Should have warnings about best practices
        warning_issues = report.get_issues_by_severity(ValidationStatus.WARNING)
        self.assertGreater(len(warning_issues), 0)
        
        # Should mention performance issue with collect()
        warning_messages = [issue.message for issue in warning_issues]
        self.assertTrue(any('collect()' in msg for msg in warning_messages))
    
    def test_validation_report_serialization(self):
        """Test validation report can be serialized to dict"""
        pipeline = Pipeline(self.simple_sql, engine="spark", validate=True)
        pipeline.parse()
        
        report = pipeline.get_validation_report()
        report_dict = report.to_dict()
        
        # Check required fields are present
        required_fields = ['engine_type', 'status', 'overall_score', 'grade', 'error_count', 'warning_count', 'metrics']
        for field in required_fields:
            self.assertIn(field, report_dict)
    
    def test_validation_report_string_representation(self):
        """Test validation report string representation"""
        pipeline = Pipeline(self.simple_sql, engine="spark", validate=True)
        pipeline.parse()
        
        report = pipeline.get_validation_report()
        report_str = str(report)
        
        # Should contain key information
        self.assertIn("Validation Report", report_str)
        self.assertIn("Status:", report_str)
        self.assertIn("Overall Score:", report_str)


if __name__ == '__main__':
    unittest.main()