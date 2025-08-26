"""
Data Size Estimator for Auto Engine Selection

Estimates data size and characteristics to help choose between Spark and DuckDB.
Uses heuristics based on table names, query patterns, and structural analysis.
"""

import json
import re
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass


@dataclass
class DataSizeEstimate:
    """Container for data size estimation results"""
    size_gb: float
    confidence: float  # 0.0 to 1.0
    reasoning: str
    size_category: str  # "small", "medium", "large", "xlarge"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert estimate to dictionary for debugging"""
        return {
            'size_gb': self.size_gb,
            'confidence': self.confidence,
            'reasoning': self.reasoning,
            'size_category': self.size_category
        }


class DataEstimator:
    """Estimates data size and characteristics for engine selection"""
    
    def __init__(self):
        # Table name patterns that indicate data size
        self.size_patterns = {
            # Large data indicators (GB scale)
            'large': ['fact_', 'big_', 'large_', 'huge_', 'raw_', 'staging_', 'events_', 
                     'logs_', 'transactions_', 'clickstream_', 'realtime_'],
            
            # Medium data indicators (MB to few GB)
            'medium': ['agg_', 'summary_', 'daily_', 'hourly_', 'monthly_', 'reports_',
                      'analytics_', 'metrics_', 'kpi_'],
            
            # Small data indicators (KB to MB)  
            'small': ['dim_', 'lookup_', 'ref_', 'config_', 'meta_', 'settings_',
                     'temp_', 'cache_', 'reference_']
        }
        
        # Size estimates in GB for different categories
        self.size_estimates = {
            'xlarge': 100.0,  # 100+ GB - definitely Spark territory
            'large': 20.0,    # 20-100 GB - probably Spark
            'medium': 5.0,    # 1-20 GB - could go either way
            'small': 0.5      # < 1 GB - probably DuckDB
        }
        
        # Confidence levels for different estimation methods
        self.confidence_levels = {
            'table_name_strong': 0.8,    # Clear naming pattern
            'table_name_weak': 0.6,      # Partial naming pattern  
            'query_pattern': 0.7,        # Based on query structure
            'join_complexity': 0.6,      # Based on join patterns
            'default_fallback': 0.3      # No clear indicators
        }
    
    def estimate(self, parsed_query: Dict[str, Any], context_hint: Optional[str] = None) -> DataSizeEstimate:
        """
        Estimate data size based on query analysis and optional context hints
        
        Args:
            parsed_query: JSON representation of parsed SQL query
            context_hint: Optional user hint about data size ("small", "medium", "large", "xlarge")
            
        Returns:
            DataSizeEstimate with size prediction and confidence
        """
        try:
            # If user provided explicit hint, use it with high confidence
            if context_hint and context_hint.lower() in self.size_estimates:
                return self._estimate_from_context_hint(context_hint.lower())
            
            # Extract tables from query
            tables = self._extract_table_names(parsed_query)
            
            # Try estimation methods in order of confidence
            
            # 1. Table name analysis (highest confidence when patterns match)
            table_estimate = self._estimate_from_table_names(tables)
            if table_estimate.confidence > 0.7:
                return table_estimate
            
            # 2. Query pattern analysis
            pattern_estimate = self._estimate_from_query_patterns(parsed_query, tables)
            if pattern_estimate.confidence > 0.6:
                return pattern_estimate
            
            # 3. Join complexity analysis
            join_estimate = self._estimate_from_join_complexity(parsed_query)
            if join_estimate.confidence > 0.5:
                return join_estimate
            
            # 4. Default fallback
            return self._get_default_estimate()
            
        except Exception as e:
            # Return safe medium estimate if anything fails
            return self._get_default_estimate()
    
    def _extract_table_names(self, query: Dict[str, Any]) -> List[str]:
        """Extract table names from the parsed query"""
        tables = []
        
        def extract_tables_recursive(node):
            if isinstance(node, dict):
                # Handle FROM clause
                if 'from' in node:
                    from_clause = node['from']
                    if isinstance(from_clause, str):
                        tables.append(from_clause.lower().strip())
                    elif isinstance(from_clause, dict) and 'value' in from_clause:
                        if isinstance(from_clause['value'], str):
                            tables.append(from_clause['value'].lower().strip())
                
                # Handle JOIN clauses
                for join_type in ['join', 'left join', 'right join', 'inner join', 'full join']:
                    if join_type in node:
                        join_clause = node[join_type]
                        if isinstance(join_clause, dict) and 'value' in join_clause:
                            if isinstance(join_clause['value'], str):
                                tables.append(join_clause['value'].lower().strip())
                
                # Handle WITH clause (CTE table names)
                if 'with' in node:
                    with_clause = node['with']
                    if isinstance(with_clause, list):
                        for cte in with_clause:
                            if isinstance(cte, dict) and 'name' in cte:
                                # CTE names are virtual, don't add them to main table list
                                pass
                    elif isinstance(with_clause, dict) and 'name' in with_clause:
                        # Single CTE
                        pass
                
                # Recurse through all values
                for value in node.values():
                    extract_tables_recursive(value)
            elif isinstance(node, list):
                for item in node:
                    extract_tables_recursive(item)
        
        extract_tables_recursive(query)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tables = []
        for table in tables:
            if table not in seen:
                seen.add(table)
                unique_tables.append(table)
        
        return unique_tables
    
    def _estimate_from_table_names(self, tables: List[str]) -> DataSizeEstimate:
        """Estimate data size based on table naming patterns"""
        if not tables:
            return self._get_default_estimate()
        
        max_size = 0.0
        size_category = "small"
        confidence = 0.0
        reasoning_parts = []
        
        for table in tables:
            table_size, table_confidence, table_reasoning = self._analyze_single_table_name(table)
            
            if table_size > max_size:
                max_size = table_size
                confidence = max(confidence, table_confidence)
            
            if table_reasoning:
                reasoning_parts.append(f"{table}: {table_reasoning}")
        
        # Determine size category
        if max_size >= self.size_estimates['xlarge']:
            size_category = "xlarge"
        elif max_size >= self.size_estimates['large']:
            size_category = "large"
        elif max_size >= self.size_estimates['medium']:
            size_category = "medium"
        else:
            size_category = "small"
        
        reasoning = "; ".join(reasoning_parts) if reasoning_parts else "No clear table name patterns"
        
        return DataSizeEstimate(
            size_gb=max_size,
            confidence=confidence,
            reasoning=f"Table name analysis: {reasoning}",
            size_category=size_category
        )
    
    def _analyze_single_table_name(self, table_name: str) -> tuple[float, float, str]:
        """Analyze a single table name for size indicators"""
        table_lower = table_name.lower()
        
        # Check for strong size indicators
        for pattern_list in self.size_patterns['large']:
            if pattern_list in table_lower:
                return (self.size_estimates['large'], 
                       self.confidence_levels['table_name_strong'],
                       f"large data pattern '{pattern_list}'")
        
        for pattern_list in self.size_patterns['small']:
            if pattern_list in table_lower:
                return (self.size_estimates['small'],
                       self.confidence_levels['table_name_strong'], 
                       f"small data pattern '{pattern_list}'")
        
        for pattern_list in self.size_patterns['medium']:
            if pattern_list in table_lower:
                return (self.size_estimates['medium'],
                       self.confidence_levels['table_name_strong'],
                       f"medium data pattern '{pattern_list}'")
        
        # Check for weak indicators (partial matches)
        if any(word in table_lower for word in ['fact', 'transaction', 'event', 'log']):
            return (self.size_estimates['large'],
                   self.confidence_levels['table_name_weak'],
                   "likely large data based on name")
        
        if any(word in table_lower for word in ['dim', 'ref', 'lookup']):
            return (self.size_estimates['small'],
                   self.confidence_levels['table_name_weak'],
                   "likely small reference data")
        
        # No clear indicators
        return (self.size_estimates['medium'], 0.2, "no clear size indicators")
    
    def _estimate_from_query_patterns(self, query: Dict[str, Any], tables: List[str]) -> DataSizeEstimate:
        """Estimate data size based on query structure patterns"""
        confidence = self.confidence_levels['query_pattern']
        reasoning_parts = []
        size_multiplier = 1.0
        
        # Count joins - more joins often indicate larger data processing
        join_count = self._count_joins(query)
        if join_count > 3:
            size_multiplier *= 2.0
            reasoning_parts.append(f"many joins ({join_count})")
            confidence += 0.1
        elif join_count > 1:
            size_multiplier *= 1.5
            reasoning_parts.append(f"multiple joins ({join_count})")
        
        # Check for aggregations without LIMIT (suggests large data processing)
        has_agg = self._has_aggregations(query)
        has_limit = self._has_limit(query)
        
        if has_agg and not has_limit:
            size_multiplier *= 1.8
            reasoning_parts.append("aggregation without limit")
            confidence += 0.1
        elif has_limit:
            size_multiplier *= 0.7  # LIMIT suggests smaller result sets
            reasoning_parts.append("has limit clause (smaller data)")
            confidence += 0.05
        
        # CTEs often indicate complex data processing
        if self._has_cte(query):
            cte_count = self._count_ctes(query)
            if cte_count > 2:
                size_multiplier *= 2.2
                reasoning_parts.append(f"complex CTEs ({cte_count})")
                confidence += 0.15
            else:
                size_multiplier *= 1.3
                reasoning_parts.append("uses CTEs")
                confidence += 0.05
        
        # Window functions often used on larger datasets
        if self._has_window_functions(query):
            size_multiplier *= 1.5
            reasoning_parts.append("window functions")
            confidence += 0.1
        
        # Multiple tables suggest larger data operations
        table_count = len(tables)
        if table_count > 4:
            size_multiplier *= 1.8
            reasoning_parts.append(f"many tables ({table_count})")
        elif table_count > 2:
            size_multiplier *= 1.3
            reasoning_parts.append(f"multiple tables ({table_count})")
        
        # Calculate base size and apply multiplier
        base_size = self.size_estimates['medium']  # Start with medium estimate
        estimated_size = base_size * size_multiplier
        
        # Determine category
        if estimated_size >= self.size_estimates['xlarge']:
            size_category = "xlarge"
        elif estimated_size >= self.size_estimates['large']:
            size_category = "large"
        elif estimated_size >= self.size_estimates['medium']:
            size_category = "medium"
        else:
            size_category = "small"
        
        reasoning = f"Query patterns: {'; '.join(reasoning_parts)}"
        
        return DataSizeEstimate(
            size_gb=estimated_size,
            confidence=min(confidence, 0.9),  # Cap confidence
            reasoning=reasoning,
            size_category=size_category
        )
    
    def _estimate_from_join_complexity(self, query: Dict[str, Any]) -> DataSizeEstimate:
        """Estimate based on JOIN complexity alone"""
        join_count = self._count_joins(query)
        confidence = self.confidence_levels['join_complexity']
        
        if join_count == 0:
            return DataSizeEstimate(
                size_gb=self.size_estimates['small'],
                confidence=confidence,
                reasoning="Single table query",
                size_category="small"
            )
        elif join_count <= 2:
            return DataSizeEstimate(
                size_gb=self.size_estimates['medium'],
                confidence=confidence,
                reasoning=f"Moderate joins ({join_count})",
                size_category="medium"
            )
        else:
            return DataSizeEstimate(
                size_gb=self.size_estimates['large'],
                confidence=confidence,
                reasoning=f"Complex joins ({join_count})",
                size_category="large"
            )
    
    def _estimate_from_context_hint(self, hint: str) -> DataSizeEstimate:
        """Create estimate based on user-provided context hint"""
        size_gb = self.size_estimates[hint]
        
        return DataSizeEstimate(
            size_gb=size_gb,
            confidence=0.9,  # High confidence in user input
            reasoning=f"User provided size hint: {hint}",
            size_category=hint
        )
    
    def _get_default_estimate(self) -> DataSizeEstimate:
        """Return default medium estimate when no clear indicators"""
        return DataSizeEstimate(
            size_gb=self.size_estimates['medium'],
            confidence=self.confidence_levels['default_fallback'],
            reasoning="No clear size indicators - using medium default",
            size_category="medium"
        )
    
    # Helper methods for query analysis
    def _count_joins(self, query: Dict[str, Any]) -> int:
        """Count JOIN operations in the query"""
        join_count = 0
        
        def count_joins_recursive(node):
            nonlocal join_count
            if isinstance(node, dict):
                for join_type in ['join', 'left join', 'right join', 'inner join', 'full join']:
                    if join_type in node:
                        join_count += 1
                        count_joins_recursive(node[join_type])
                
                for key, value in node.items():
                    if key not in ['join', 'left join', 'right join', 'inner join', 'full join']:
                        count_joins_recursive(value)
            elif isinstance(node, list):
                for item in node:
                    count_joins_recursive(item)
        
        count_joins_recursive(query)
        return join_count
    
    def _has_aggregations(self, query: Dict[str, Any]) -> bool:
        """Check if query contains aggregation functions"""
        query_str = json.dumps(query).lower()
        agg_functions = ['sum', 'avg', 'count', 'max', 'min', 'mean']
        return any(func in query_str for func in agg_functions)
    
    def _has_limit(self, query: Dict[str, Any]) -> bool:
        """Check if query has LIMIT clause"""
        return 'limit' in query or 'top' in query
    
    def _has_cte(self, query: Dict[str, Any]) -> bool:
        """Check if query uses Common Table Expressions"""
        return 'with' in query
    
    def _count_ctes(self, query: Dict[str, Any]) -> int:
        """Count number of CTEs"""
        if not self._has_cte(query):
            return 0
        
        with_clause = query.get('with', [])
        if isinstance(with_clause, list):
            return len(with_clause)
        elif isinstance(with_clause, dict):
            return 1
        return 0
    
    def _has_window_functions(self, query: Dict[str, Any]) -> bool:
        """Check if query contains window functions"""
        query_str = json.dumps(query).lower()
        window_functions = ['rank', 'dense_rank', 'row_number', 'lead', 'lag', 'over']
        return any(func in query_str for func in window_functions)