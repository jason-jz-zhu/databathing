import re
from typing import List, Dict, Any
from .code_validator import CodeValidator

try:
    import sqlparse
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False


class DuckDBValidator(CodeValidator):
    """Validator for DuckDB generated code"""
    
    def __init__(self):
        super().__init__()
        self.duckdb_functions = {
            'count', 'sum', 'avg', 'max', 'min', 'stddev', 'variance',
            'first', 'last', 'array_agg', 'string_agg', 'bool_and', 'bool_or',
            'regexp_matches', 'regexp_replace', 'strftime', 'date_part',
            'coalesce', 'nullif', 'greatest', 'least', 'abs', 'ceil', 'floor',
            'round', 'sqrt', 'power', 'log', 'exp', 'sin', 'cos', 'tan'
        }
        
        self.sql_keywords = {
            'SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY',
            'LIMIT', 'OFFSET', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
            'UNION', 'UNION ALL', 'INTERSECT', 'EXCEPT', 'WITH', 'AS',
            'DISTINCT', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE'
        }
        
        # Performance anti-patterns for SQL
        self.performance_antipatterns = [
            (r'SELECT\s+\*.*ORDER BY', 'SELECT * with ORDER BY can be inefficient on large tables'),
            (r'WHERE.*LIKE\s+["\']%.*%["\']', 'Leading wildcard in LIKE can prevent index usage'),
            (r'WHERE.*\(\s*SELECT', 'Correlated subqueries can be slow'),
            (r'DISTINCT.*ORDER BY', 'DISTINCT with ORDER BY may require sorting large datasets'),
            (r'GROUP BY.*HAVING.*COUNT\(\*\)', 'HAVING with COUNT(*) processes all groups before filtering')
        ]
    
    def validate_syntax(self, code: str) -> bool:
        """Validate DuckDB code syntax"""
        try:
            # Extract SQL from duckdb.sql("...") wrapper
            sql = self._extract_sql(code)
            if not sql:
                return False
            
            # Try to parse SQL with sqlparse if available
            if HAS_SQLPARSE:
                try:
                    parsed = sqlparse.parse(sql)
                    if not parsed:
                        return False
                    
                    # Check for basic SQL structure
                    return self._has_valid_sql_structure(sql)
                    
                except Exception:
                    # If sqlparse fails, try basic pattern matching
                    return self._basic_sql_validation(sql)
            else:
                # Fallback to basic validation if sqlparse not available
                import warnings
                warnings.warn(
                    "sqlparse not available. Using basic SQL validation. "
                    "Install with 'pip install sqlparse' for advanced SQL validation.",
                    UserWarning,
                    stacklevel=2
                )
                return self._basic_sql_validation(sql)
                
        except Exception:
            return False
    
    def _extract_sql(self, code: str) -> str:
        """Extract SQL string from DuckDB code"""
        code = code.strip()
        
        # Remove variable assignment (e.g., "result = ")
        assignment_pattern = r'^\w+\s*=\s*'
        code = re.sub(assignment_pattern, '', code)
        
        # More robust SQL extraction that handles nested quotes and escape sequences
        # Try different quote patterns
        patterns = [
            r'duckdb\.sql\("""([^"]+(?:"{1,2}[^"]*)*?)"""\)',  # Triple quotes
            r'duckdb\.sql\(\'\'\'([^\']+(?:\'{1,2}[^\']*)*?)\'\'\'\)',  # Triple single quotes
            r'duckdb\.sql\("([^"\\]*(?:\\.[^"\\]*)*)"\)',  # Double quotes with escapes
            r'duckdb\.sql\(\'([^\'\\]*(?:\\.[^\'\\]*)*)\'\)',  # Single quotes with escapes
            r'duckdb\.sql\(["\']([^"\']+)["\']\)'  # Simple fallback
        ]
        
        for pattern in patterns:
            match = re.search(pattern, code, re.DOTALL)
            if match:
                return match.group(1).strip()
        
        return ""
    
    def _has_valid_sql_structure(self, sql: str) -> bool:
        """Check if SQL has valid basic structure"""
        sql_upper = sql.upper()
        
        # Must have SELECT
        if 'SELECT' not in sql_upper:
            return False
        
        # Check for balanced parentheses
        paren_count = sql.count('(') - sql.count(')')
        if paren_count != 0:
            return False
        
        # Check for FROM clause (most queries need it)
        if 'FROM' not in sql_upper and 'SELECT' in sql_upper:
            # Allow simple queries without FROM like "SELECT 1"
            if not re.search(r'SELECT\s+\d+', sql_upper):
                # Check for common missing FROM patterns
                if re.search(r'WHERE', sql_upper):
                    return False  # WHERE without FROM is invalid
        
        # Check for proper FROM structure
        if 'FROM' in sql_upper:
            # Find what comes after FROM but before any other keywords
            from_match = re.search(r'FROM\s+([A-Z_][A-Z0-9_]*)', sql_upper)
            if from_match:
                table_name = from_match.group(1).strip()
                # Check if it's actually a SQL keyword, not a table name
                sql_keywords = {'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'SELECT'}
                if table_name in sql_keywords:
                    return False  # FROM followed by keyword, not table name
            else:
                # FROM keyword exists but no valid table name found
                return False
        
        # Check for basic keyword order
        keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']
        last_pos = -1
        
        for keyword in keywords:
            if keyword in sql_upper:
                pos = sql_upper.find(keyword)
                if pos < last_pos:
                    return False  # Keywords out of order
                last_pos = pos
        
        return True
    
    def _basic_sql_validation(self, sql: str) -> bool:
        """Basic SQL validation using pattern matching"""
        sql_upper = sql.upper().strip()
        
        # Must start with SELECT, WITH, or similar
        valid_starts = ['SELECT', 'WITH', '(SELECT']
        if not any(sql_upper.startswith(start) for start in valid_starts):
            return False
        
        # Check for unmatched quotes
        single_quotes = sql.count("'")
        double_quotes = sql.count('"')
        
        if single_quotes % 2 != 0 or double_quotes % 2 != 0:
            return False
        
        return True
    
    def calculate_complexity(self, code: str) -> float:
        """Calculate SQL complexity"""
        sql = self._extract_sql(code)
        if not sql:
            return 100  # Max complexity if we can't parse
        
        complexity_score = 0
        sql_upper = sql.upper()
        
        # Subquery complexity
        subquery_count = sql.count('(SELECT') + sql.count('( SELECT')
        complexity_score += subquery_count * 15
        
        # Join complexity
        join_count = sql_upper.count('JOIN')
        complexity_score += join_count * 10
        
        # Aggregate function complexity
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'GROUP_CONCAT']
        agg_count = sum(sql_upper.count(func) for func in agg_functions)
        complexity_score += agg_count * 5
        
        # Window function complexity
        window_count = sql_upper.count('OVER')
        complexity_score += window_count * 20
        
        # Union complexity
        union_count = sql_upper.count('UNION')
        complexity_score += union_count * 10
        
        # WHERE clause complexity (number of conditions)
        condition_keywords = ['AND', 'OR', 'IN', 'EXISTS', 'BETWEEN', 'LIKE']
        condition_count = sum(sql_upper.count(keyword) for keyword in condition_keywords)
        complexity_score += condition_count * 3
        
        # Nested parentheses
        nesting_depth = self._count_nested_parentheses(sql)
        complexity_score += nesting_depth * 5
        
        return min(complexity_score, 100)
    
    def calculate_readability(self, code: str) -> float:
        """Calculate SQL readability score"""
        sql = self._extract_sql(code)
        if not sql:
            return 0
        
        readability = 100
        
        # Deduct for very long single lines
        lines = sql.split('\n')
        max_line_length = max(len(line.strip()) for line in lines) if lines else 0
        if max_line_length > 150:
            readability -= 20
        elif max_line_length > 100:
            readability -= 10
        
        # Bonus for proper formatting (keywords on separate lines)
        if len(lines) > 1:
            readability += 10
        
        # Deduct for lack of proper spacing
        if re.search(r'\w+,\w+', sql):  # No space after comma
            readability -= 5
        
        # Deduct for very complex WHERE clauses
        where_match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            condition_count = where_clause.upper().count('AND') + where_clause.upper().count('OR')
            if condition_count > 5:
                readability -= condition_count * 2
        
        # Bonus for meaningful table aliases
        alias_pattern = r'\s+AS\s+(\w+)'
        aliases = re.findall(alias_pattern, sql, re.IGNORECASE)
        meaningful_aliases = [a for a in aliases if len(a) > 1 and not a.lower().startswith('t')]
        if aliases and meaningful_aliases:
            readability += min(len(meaningful_aliases) * 3, 15)
        
        return max(0, min(readability, 100))
    
    def calculate_performance(self, code: str) -> float:
        """Estimate SQL performance"""
        sql = self._extract_sql(code)
        if not sql:
            return 0
        
        performance = 100
        sql_upper = sql.upper()
        
        # Check for performance anti-patterns
        for pattern, reason in self.performance_antipatterns:
            if re.search(pattern, sql, re.IGNORECASE):
                performance -= 15
        
        # Deduct for SELECT *
        if re.search(r'SELECT\s+\*', sql, re.IGNORECASE):
            performance -= 10
        
        # Deduct for functions in WHERE clause (prevents index usage)
        where_functions = re.findall(r'WHERE.*?(\w+)\s*\(', sql, re.IGNORECASE)
        if where_functions:
            performance -= len(where_functions) * 5
        
        # Bonus for proper filtering
        if 'WHERE' in sql_upper and 'LIMIT' in sql_upper:
            performance += 5
        
        # Deduct for potential Cartesian products
        if 'JOIN' in sql_upper and 'ON' not in sql_upper:
            performance -= 25
        
        # Bonus for using indexes-friendly operations
        if 'WHERE' in sql_upper and '=' in sql:
            performance += 5
        
        return max(0, min(performance, 100))
    
    def check_best_practices(self, code: str) -> List[str]:
        """Check for SQL/DuckDB best practice violations"""
        sql = self._extract_sql(code)
        if not sql:
            return ["Unable to extract SQL from DuckDB code"]
        
        issues = []
        sql_upper = sql.upper()
        
        # Check for performance issues
        for pattern, message in self.performance_antipatterns:
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(f"Performance: {message}")
        
        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql, re.IGNORECASE):
            issues.append("Best Practice: Avoid SELECT *, specify column names explicitly")
        
        # Check for missing table aliases in joins
        if 'JOIN' in sql_upper and len(re.findall(r'\bJOIN\b', sql_upper)) > 1:
            if not re.search(r'\s+AS\s+\w+', sql, re.IGNORECASE):
                issues.append("Readability: Consider using table aliases in complex joins")
        
        # Check for very long WHERE clauses
        where_match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if where_match and len(where_match.group(1)) > 200:
            issues.append("Readability: Consider breaking complex WHERE clauses into CTEs")
        
        # Check for missing LIMIT in exploratory queries
        if 'ORDER BY' in sql_upper and 'LIMIT' not in sql_upper:
            issues.append("Best Practice: Consider adding LIMIT to ORDER BY queries for better performance")
        
        # Check for inefficient HAVING clauses
        if 'HAVING' in sql_upper and 'WHERE' not in sql_upper:
            issues.append("Performance: Consider using WHERE instead of HAVING when possible")
        
        # Check for proper NULL handling
        if 'NULL' in sql_upper and 'COALESCE' not in sql_upper and 'ISNULL' not in sql_upper:
            issues.append("Best Practice: Consider explicit NULL handling with COALESCE or IS NULL checks")
        
        return issues