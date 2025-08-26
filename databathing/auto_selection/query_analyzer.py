"""
Query Analyzer for Auto Engine Selection

Analyzes SQL queries to extract features relevant for Spark vs DuckDB selection.
Focus on characteristics that indicate optimal engine choice.
"""

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class QueryFeatures:
    """Container for extracted query features"""
    complexity_score: float
    join_count: int
    table_count: int
    has_aggregations: bool
    has_window_functions: bool
    has_limit: bool
    has_cte: bool
    is_simple_aggregation: bool
    has_complex_etl_pattern: bool
    interactive_indicators: int
    math_operations_count: int
    subquery_depth: int
    distinct_count: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert features to dictionary for debugging"""
        return {
            'complexity_score': self.complexity_score,
            'join_count': self.join_count,
            'table_count': self.table_count,
            'has_aggregations': self.has_aggregations,
            'has_window_functions': self.has_window_functions,
            'has_limit': self.has_limit,
            'has_cte': self.has_cte,
            'is_simple_aggregation': self.is_simple_aggregation,
            'has_complex_etl_pattern': self.has_complex_etl_pattern,
            'interactive_indicators': self.interactive_indicators,
            'math_operations_count': self.math_operations_count,
            'subquery_depth': self.subquery_depth,
            'distinct_count': self.distinct_count
        }


class QueryAnalyzer:
    """Analyzes SQL queries to extract features for engine selection"""
    
    def __init__(self):
        # SQL aggregation functions
        self.agg_functions = [
            'sum', 'avg', 'count', 'max', 'min', 'mean',
            'collect_list', 'collect_set', 'stddev', 'variance'
        ]
        
        # Mathematical operations indicating computational intensity
        self.math_operations = [
            'sqrt', 'power', 'log', 'ln', 'exp', 'abs', 'round', 'floor', 'ceil',
            'sin', 'cos', 'tan', 'asin', 'acos', 'atan'
        ]
        
        # Window function indicators
        self.window_functions = [
            'rank', 'dense_rank', 'row_number', 'lead', 'lag',
            'first_value', 'last_value', 'ntile'
        ]
    
    def analyze(self, parsed_query: Dict[str, Any]) -> QueryFeatures:
        """
        Analyze parsed SQL query and extract relevant features
        
        Args:
            parsed_query: JSON representation of parsed SQL query
            
        Returns:
            QueryFeatures object containing extracted features
        """
        try:
            # Extract basic structural features
            join_count = self._count_joins(parsed_query)
            table_count = self._count_tables(parsed_query)
            has_aggregations = self._has_aggregations(parsed_query)
            has_window_functions = self._has_window_functions(parsed_query)
            has_limit = self._has_limit(parsed_query)
            has_cte = self._has_cte(parsed_query)
            
            # Analyze query complexity and patterns
            complexity_score = self._calculate_complexity_score(parsed_query)
            is_simple_aggregation = self._is_simple_aggregation(parsed_query)
            has_complex_etl = self._has_complex_etl_pattern(parsed_query)
            interactive_indicators = self._count_interactive_indicators(parsed_query)
            math_ops_count = self._count_math_operations(parsed_query)
            subquery_depth = self._calculate_subquery_depth(parsed_query)
            distinct_count = self._count_distinct_operations(parsed_query)
            
            return QueryFeatures(
                complexity_score=complexity_score,
                join_count=join_count,
                table_count=table_count,
                has_aggregations=has_aggregations,
                has_window_functions=has_window_functions,
                has_limit=has_limit,
                has_cte=has_cte,
                is_simple_aggregation=is_simple_aggregation,
                has_complex_etl_pattern=has_complex_etl,
                interactive_indicators=interactive_indicators,
                math_operations_count=math_ops_count,
                subquery_depth=subquery_depth,
                distinct_count=distinct_count
            )
            
        except Exception as e:
            # Return safe defaults if parsing fails
            return self._get_default_features()
    
    def _count_joins(self, query: Dict[str, Any]) -> int:
        """Count JOIN operations in the query"""
        join_count = 0
        
        def count_joins_recursive(node):
            nonlocal join_count
            if isinstance(node, dict):
                # Count different types of joins
                for join_type in ['join', 'left join', 'right join', 'inner join', 'full join']:
                    if join_type in node:
                        join_count += 1
                        count_joins_recursive(node[join_type])
                
                # Recurse through other parts
                for key, value in node.items():
                    if key not in ['join', 'left join', 'right join', 'inner join', 'full join']:
                        count_joins_recursive(value)
            elif isinstance(node, list):
                for item in node:
                    count_joins_recursive(item)
        
        count_joins_recursive(query)
        return join_count
    
    def _count_tables(self, query: Dict[str, Any]) -> int:
        """Count number of tables referenced in the query"""
        tables = set()
        
        def extract_tables_recursive(node):
            if isinstance(node, dict):
                # Handle FROM clause
                if 'from' in node:
                    from_clause = node['from']
                    if isinstance(from_clause, str):
                        tables.add(from_clause.lower())
                    elif isinstance(from_clause, dict) and 'value' in from_clause:
                        if isinstance(from_clause['value'], str):
                            tables.add(from_clause['value'].lower())
                
                # Handle JOIN clauses
                for join_type in ['join', 'left join', 'right join', 'inner join', 'full join']:
                    if join_type in node:
                        join_clause = node[join_type]
                        if isinstance(join_clause, dict) and 'value' in join_clause:
                            if isinstance(join_clause['value'], str):
                                tables.add(join_clause['value'].lower())
                
                # Recurse through all values
                for value in node.values():
                    extract_tables_recursive(value)
            elif isinstance(node, list):
                for item in node:
                    extract_tables_recursive(item)
        
        extract_tables_recursive(query)
        return len(tables)
    
    def _has_aggregations(self, query: Dict[str, Any]) -> bool:
        """Check if query contains aggregation functions"""
        query_str = json.dumps(query).lower()
        return any(func in query_str for func in self.agg_functions)
    
    def _has_window_functions(self, query: Dict[str, Any]) -> bool:
        """Check if query contains window functions"""
        query_str = json.dumps(query).lower()
        return any(func in query_str for func in self.window_functions) or 'over' in query_str
    
    def _has_limit(self, query: Dict[str, Any]) -> bool:
        """Check if query has LIMIT clause"""
        return 'limit' in query or 'top' in query
    
    def _has_cte(self, query: Dict[str, Any]) -> bool:
        """Check if query uses Common Table Expressions (WITH statements)"""
        return 'with' in query
    
    def _calculate_complexity_score(self, query: Dict[str, Any]) -> float:
        """Calculate overall query complexity score"""
        score = 1.0  # Base score for any query
        
        # Base complexity from structure
        if self._has_cte(query):
            score += 2.0
        
        join_count = self._count_joins(query)
        score += join_count * 1.5
        
        if self._has_aggregations(query):
            score += 1.0
        
        if self._has_window_functions(query):
            score += 2.0
        
        # WHERE clause adds some complexity
        if 'where' in query:
            score += 0.5
            
        # SELECT with multiple columns adds complexity
        if 'select' in query:
            select_clause = query['select']
            if isinstance(select_clause, list) and len(select_clause) > 1:
                score += 0.3
        
        # Subquery complexity
        score += self._calculate_subquery_depth(query) * 0.5
        
        # Mathematical operations add complexity
        score += self._count_math_operations(query) * 0.2
        
        return score
    
    def _is_simple_aggregation(self, query: Dict[str, Any]) -> bool:
        """Determine if this is a simple aggregation query suitable for DuckDB"""
        # Simple aggregation criteria:
        # - Has aggregations
        # - Low join count (0-2)
        # - No complex window functions
        # - Not too many tables
        
        if not self._has_aggregations(query):
            return False
        
        join_count = self._count_joins(query)
        table_count = self._count_tables(query)
        
        return (join_count <= 2 and 
                table_count <= 3 and 
                not self._has_complex_window_functions(query))
    
    def _has_complex_etl_pattern(self, query: Dict[str, Any]) -> bool:
        """Identify complex ETL patterns that benefit from Spark"""
        # Complex ETL indicators:
        # - Multiple CTEs
        # - Many joins (>3)
        # - Complex transformations
        # - Many tables
        
        cte_count = self._count_ctes(query)
        join_count = self._count_joins(query)
        table_count = self._count_tables(query)
        
        return (cte_count > 2 or 
                join_count > 3 or 
                table_count > 4 or
                self._calculate_complexity_score(query) > 8.0)
    
    def _count_interactive_indicators(self, query: Dict[str, Any]) -> int:
        """Count indicators that suggest interactive/dashboard usage"""
        indicators = 0
        
        if self._has_limit(query):
            indicators += 2  # Strong indicator
        
        if self._is_simple_aggregation(query):
            indicators += 1
        
        # Single table queries are often interactive
        if self._count_tables(query) == 1:
            indicators += 1
        
        # Low join count suggests simpler, faster queries
        if self._count_joins(query) <= 1:
            indicators += 1
        
        return indicators
    
    def _count_math_operations(self, query: Dict[str, Any]) -> int:
        """Count mathematical operations in the query"""
        query_str = json.dumps(query).lower()
        return sum(1 for op in self.math_operations if op in query_str)
    
    def _calculate_subquery_depth(self, query: Dict[str, Any]) -> int:
        """Calculate maximum subquery nesting depth"""
        max_depth = 0
        
        def calculate_depth_recursive(node, current_depth=0):
            nonlocal max_depth
            max_depth = max(max_depth, current_depth)
            
            if isinstance(node, dict):
                # Look for nested queries
                for key, value in node.items():
                    if key in ['select', 'from', 'where', 'having'] and isinstance(value, dict):
                        if 'select' in value:  # This is a subquery
                            calculate_depth_recursive(value, current_depth + 1)
                        else:
                            calculate_depth_recursive(value, current_depth)
                    else:
                        calculate_depth_recursive(value, current_depth)
            elif isinstance(node, list):
                for item in node:
                    calculate_depth_recursive(item, current_depth)
        
        calculate_depth_recursive(query)
        return max_depth
    
    def _count_distinct_operations(self, query: Dict[str, Any]) -> int:
        """Count DISTINCT operations"""
        query_str = json.dumps(query).lower()
        return query_str.count('distinct')
    
    def _has_complex_window_functions(self, query: Dict[str, Any]) -> bool:
        """Check for complex window functions"""
        query_str = json.dumps(query).lower()
        complex_window_patterns = ['partition by', 'rows between', 'range between']
        return any(pattern in query_str for pattern in complex_window_patterns)
    
    def _count_ctes(self, query: Dict[str, Any]) -> int:
        """Count Common Table Expressions"""
        if not self._has_cte(query):
            return 0
        
        with_clause = query.get('with', [])
        if isinstance(with_clause, list):
            return len(with_clause)
        elif isinstance(with_clause, dict):
            return 1
        return 0
    
    def _get_default_features(self) -> QueryFeatures:
        """Return default features when analysis fails"""
        return QueryFeatures(
            complexity_score=3.0,  # Medium complexity default
            join_count=0,
            table_count=1,
            has_aggregations=False,
            has_window_functions=False,
            has_limit=False,
            has_cte=False,
            is_simple_aggregation=False,
            has_complex_etl_pattern=False,
            interactive_indicators=1,
            math_operations_count=0,
            subquery_depth=0,
            distinct_count=0
        )