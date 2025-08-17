import ast
import re
from typing import List, Dict, Any
from .code_validator import CodeValidator


class SparkValidator(CodeValidator):
    """Validator for PySpark generated code"""
    
    def __init__(self):
        super().__init__()
        self.spark_methods = {
            'select', 'selectExpr', 'filter', 'where', 'groupBy', 'agg', 
            'orderBy', 'sort', 'limit', 'join', 'union', 'unionAll',
            'distinct', 'drop', 'withColumn', 'withColumnRenamed',
            'alias', 'collect', 'show', 'count', 'first', 'take'
        }
        
        self.spark_functions = {
            'col', 'lit', 'when', 'otherwise', 'isNull', 'isNotNull',
            'sum', 'avg', 'max', 'min', 'count', 'countDistinct',
            'first', 'last', 'collect_list', 'collect_set',
            'desc', 'asc', 'row_number', 'rank', 'dense_rank'
        }
        
        # Performance anti-patterns
        self.performance_antipatterns = [
            (r'\.collect\(\)', 'Avoid collect() on large datasets'),
            (r'\.toPandas\(\)', 'Use toPandas() carefully with large data'),
            (r'\.count\(\).*\.count\(\)', 'Multiple count() calls can be expensive'),
            (r'for.*\.collect\(\)', 'Avoid iterating over collected data'),
            (r'\.rdd\.', 'Using RDD API instead of DataFrame API reduces optimization')
        ]
    
    def validate_syntax(self, code: str) -> bool:
        """Validate PySpark code syntax"""
        try:
            # Remove variable assignment prefix (e.g., "final_df = ")
            code_to_parse = self._extract_expression(code)
            
            # Try to parse as Python expression
            ast.parse(code_to_parse, mode='eval')
            
            # Check for basic PySpark structure
            if not self._has_valid_spark_structure(code):
                return False
                
            return True
            
        except SyntaxError:
            return False
        except Exception:
            # If we can't parse it as expression, try as statement
            try:
                ast.parse(code)
                return self._has_valid_spark_structure(code)
            except:
                return False
    
    def _extract_expression(self, code: str) -> str:
        """Extract the DataFrame expression from assignment"""
        # Remove common variable assignments
        code = code.strip()
        
        # Remove assignment patterns like "final_df = ", "df = ", etc.
        assignment_pattern = r'^\w+\s*=\s*'
        code = re.sub(assignment_pattern, '', code)
        
        return code.strip()
    
    def _has_valid_spark_structure(self, code: str) -> bool:
        """Check if code has valid Spark DataFrame structure"""
        # Should have at least one DataFrame method or start with a DataFrame variable
        has_dataframe_ref = bool(re.search(r'\w+\.', code))  # Something.method()
        has_spark_method = any(method in code for method in self.spark_methods)
        
        return has_dataframe_ref or has_spark_method
    
    def calculate_complexity(self, code: str) -> float:
        """Calculate complexity based on method chains, nesting, conditions"""
        complexity_score = 0
        
        # Method chaining complexity
        chain_count = self._count_method_chains(code)
        complexity_score += min(chain_count * 2, 20)  # Max 20 points for chaining
        
        # Nested function calls
        nesting_depth = self._count_nested_parentheses(code)
        complexity_score += min(nesting_depth * 3, 30)  # Max 30 points for nesting
        
        # Number of operations
        operation_count = len([m for m in self.spark_methods if m in code])
        complexity_score += min(operation_count * 2, 20)  # Max 20 points for operations
        
        # Join complexity
        join_count = code.count('.join(')
        complexity_score += join_count * 10  # Joins are complex
        
        # Conditional logic
        condition_count = code.count('when(') + code.count('otherwise(')
        complexity_score += condition_count * 5
        
        return min(complexity_score, 100)
    
    def calculate_readability(self, code: str) -> float:
        """Calculate readability score"""
        readability = 100
        
        # Deduct for very long lines
        lines = code.split('\n')
        for line in lines:
            if len(line.strip()) > 120:
                readability -= 5
        
        # Deduct for very long method chains without breaks
        if '\\' not in code and len(code) > 100:
            readability -= 10
        
        # Bonus for proper line breaks in chaining
        if '\\' in code and code.count('.') > 3:
            readability += 10
        
        # Deduct for unclear variable names
        if not self._has_meaningful_variable_names(code):
            readability -= 15
        
        # Deduct for missing selectExpr clarity
        if 'selectExpr(' in code:
            # Check if selectExpr has complex expressions
            select_expr_pattern = r'selectExpr\(["\']([^"\']+)["\']'
            matches = re.findall(select_expr_pattern, code)
            for match in matches:
                if len(match) > 50:  # Very long expressions
                    readability -= 5
        
        return max(0, min(readability, 100))
    
    def calculate_performance(self, code: str) -> float:
        """Estimate performance based on known patterns"""
        performance = 100
        
        # Check for performance anti-patterns
        for pattern, reason in self.performance_antipatterns:
            if re.search(pattern, code):
                performance -= 20
        
        # Bonus for efficient operations
        if '.filter(' in code and '.select(' in code:
            # Filter before select is good
            filter_pos = code.find('.filter(')
            select_pos = code.find('.select(')
            if 0 <= filter_pos < select_pos:
                performance += 5
        
        # Deduct for potential Cartesian products
        if '.join(' in code and 'ON' not in code.upper():
            performance -= 15
        
        # Bonus for using built-in functions
        function_count = sum(1 for func in self.spark_functions if func in code)
        performance += min(function_count * 2, 10)
        
        return max(0, min(performance, 100))
    
    def check_best_practices(self, code: str) -> List[str]:
        """Check for PySpark best practice violations"""
        issues = []
        
        # Check for performance issues
        for pattern, message in self.performance_antipatterns:
            if re.search(pattern, code):
                issues.append(f"Performance: {message}")
        
        # Check for readability issues
        if code.count('.') > 5 and '\\' not in code:
            issues.append("Readability: Consider breaking long method chains across lines")
        
        # Check for proper column references
        if '"' in code and 'col(' not in code:
            # Using string literals instead of col() function
            issues.append("Best Practice: Consider using col() function for column references")
        
        # Check for deprecated methods
        if '.unionAll(' in code:
            issues.append("Deprecated: unionAll() is deprecated, use union() instead")
        
        # Check for inefficient groupBy
        if '.groupBy(' in code and '.agg(' not in code and '.count()' in code:
            issues.append("Performance: Consider using agg() with groupBy for better performance")
        
        # Check for missing alias in complex expressions
        complex_expr_pattern = r'selectExpr\(["\'][^"\']*\([^"\']*\)[^"\']*["\']'
        if re.search(complex_expr_pattern, code) and ' AS ' not in code.upper():
            issues.append("Readability: Consider adding aliases to complex expressions")
        
        return issues