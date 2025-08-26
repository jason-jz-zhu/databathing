"""
Auto Engine Selector - Main Orchestration Class

Coordinates query analysis, data estimation, and rule-based engine selection
to automatically choose between Spark and DuckDB.
"""

import time
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .query_analyzer import QueryAnalyzer, QueryFeatures
from .data_estimator import DataEstimator, DataSizeEstimate
from .rule_engine import RuleEngine, EngineChoice


@dataclass
class SelectionContext:
    """Context object for user hints and preferences"""
    data_size_hint: Optional[str] = None      # "small", "medium", "large", "xlarge"
    performance_priority: str = "balanced"    # "speed", "cost", "scale"
    workload_type: Optional[str] = None       # "dashboard", "etl", "analytics"
    latency_requirement: str = "normal"       # "interactive", "normal", "batch"
    fault_tolerance: bool = False             # True = prefer Spark for reliability
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for debugging"""
        return {
            'data_size_hint': self.data_size_hint,
            'performance_priority': self.performance_priority,
            'workload_type': self.workload_type,
            'latency_requirement': self.latency_requirement,
            'fault_tolerance': self.fault_tolerance
        }


@dataclass
class SelectionResult:
    """Complete result of engine selection process"""
    engine: str  # Selected engine
    confidence: float  # Selection confidence (0.0-1.0)
    reasoning: str  # Human-readable explanation
    rule_name: str  # Name of rule that made the decision
    
    # Analysis details
    query_features: QueryFeatures
    data_estimate: DataSizeEstimate
    context: Optional[SelectionContext]
    
    # Alternative choices
    alternatives: Optional[Dict[str, float]] = None
    
    # Performance metrics
    analysis_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization/debugging"""
        return {
            'engine': self.engine,
            'confidence': self.confidence,
            'reasoning': self.reasoning,
            'rule_name': self.rule_name,
            'query_features': self.query_features.to_dict(),
            'data_estimate': self.data_estimate.to_dict(),
            'context': self.context.to_dict() if self.context else None,
            'alternatives': self.alternatives or {},
            'analysis_time_ms': self.analysis_time_ms
        }
    
    def get_summary(self) -> str:
        """Get concise summary for display"""
        return f"{self.engine.upper()} (confidence: {self.confidence:.0%}) - {self.reasoning}"


class AutoEngineSelector:
    """Main orchestration class for automatic engine selection"""
    
    def __init__(self):
        self.query_analyzer = QueryAnalyzer()
        self.data_estimator = DataEstimator()
        self.rule_engine = RuleEngine()
        
        # State for debugging and transparency
        self.last_selection: Optional[SelectionResult] = None
        
        # Performance tracking
        self.selection_count = 0
        self.total_analysis_time = 0.0
    
    def select_engine(self, sql_query: str, parsed_query: Dict[str, Any], 
                     context: Optional[SelectionContext] = None) -> SelectionResult:
        """
        Automatically select engine based on query analysis
        
        Args:
            sql_query: Original SQL query string
            parsed_query: JSON representation of parsed query
            context: Optional user context and preferences
            
        Returns:
            SelectionResult with engine choice and detailed analysis
        """
        start_time = time.perf_counter()
        
        try:
            # Step 1: Analyze query features
            query_features = self.query_analyzer.analyze(parsed_query)
            
            # Step 2: Estimate data characteristics
            data_size_hint = context.data_size_hint if context else None
            data_estimate = self.data_estimator.estimate(parsed_query, data_size_hint)
            
            # Step 3: Apply rules to make decision
            engine_choice = self.rule_engine.select_engine(query_features, data_estimate, context)
            
            # Calculate analysis time
            analysis_time = (time.perf_counter() - start_time) * 1000  # Convert to ms
            
            # Create result
            result = SelectionResult(
                engine=engine_choice.engine,
                confidence=engine_choice.confidence,
                reasoning=engine_choice.reasoning,
                rule_name=engine_choice.rule_name,
                query_features=query_features,
                data_estimate=data_estimate,
                context=context,
                alternatives=engine_choice.alternatives,
                analysis_time_ms=analysis_time
            )
            
            # Update tracking
            self.last_selection = result
            self.selection_count += 1
            self.total_analysis_time += analysis_time
            
            return result
            
        except Exception as e:
            # Fallback to safe default if analysis fails
            analysis_time = (time.perf_counter() - start_time) * 1000
            
            return SelectionResult(
                engine="spark",  # Safe default
                confidence=0.3,
                reasoning=f"Analysis failed ({str(e)[:50]}...) - using Spark as safe default",
                rule_name="error_fallback",
                query_features=self.query_analyzer._get_default_features(),
                data_estimate=self.data_estimator._get_default_estimate(),
                context=context,
                analysis_time_ms=analysis_time
            )
    
    def explain_selection(self, result: SelectionResult) -> str:
        """
        Provide detailed explanation of selection decision
        
        Args:
            result: SelectionResult to explain
            
        Returns:
            Detailed explanation string
        """
        explanation_parts = [
            f"Selected Engine: {result.engine.upper()}",
            f"Confidence: {result.confidence:.0%}",
            f"Rule Applied: {result.rule_name}",
            "",
            f"Reasoning: {result.reasoning}",
            "",
            "Analysis Details:",
            f"  • Query complexity: {result.query_features.complexity_score:.1f}",
            f"  • Join count: {result.query_features.join_count}",
            f"  • Table count: {result.query_features.table_count}",
            f"  • Has aggregations: {result.query_features.has_aggregations}",
            f"  • Has LIMIT: {result.query_features.has_limit}",
            f"  • Interactive indicators: {result.query_features.interactive_indicators}",
            "",
            f"  • Estimated data size: {result.data_estimate.size_gb:.1f} GB ({result.data_estimate.size_category})",
            f"  • Size estimation confidence: {result.data_estimate.confidence:.0%}",
            f"  • Size reasoning: {result.data_estimate.reasoning}",
        ]
        
        if result.context:
            explanation_parts.extend([
                "",
                "User Context:",
                f"  • Data size hint: {result.context.data_size_hint or 'None'}",
                f"  • Performance priority: {result.context.performance_priority}",
                f"  • Workload type: {result.context.workload_type or 'None'}",
                f"  • Latency requirement: {result.context.latency_requirement}",
                f"  • Fault tolerance: {result.context.fault_tolerance}"
            ])
        
        if result.alternatives:
            explanation_parts.extend([
                "",
                "Alternative Engines:",
                *[f"  • {engine}: {score:.0%}" for engine, score in result.alternatives.items()]
            ])
        
        explanation_parts.extend([
            "",
            f"Analysis completed in {result.analysis_time_ms:.1f}ms"
        ])
        
        return "\n".join(explanation_parts)
    
    def get_recommendation_summary(self, result: SelectionResult) -> Dict[str, Any]:
        """
        Get concise recommendation summary for display
        
        Args:
            result: SelectionResult to summarize
            
        Returns:
            Dictionary with key recommendation info
        """
        return {
            'recommended_engine': result.engine,
            'confidence': f"{result.confidence:.0%}",
            'primary_reason': result.reasoning,
            'data_size_category': result.data_estimate.size_category,
            'query_complexity': 'high' if result.query_features.complexity_score > 6 else 
                              'medium' if result.query_features.complexity_score > 3 else 'low',
            'interactive_query': result.query_features.interactive_indicators > 0,
            'analysis_time': f"{result.analysis_time_ms:.1f}ms"
        }
    
    def get_engine_comparison(self, sql_query: str, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare how both engines would perform for the given query
        
        Args:
            sql_query: Original SQL query
            parsed_query: Parsed query JSON
            
        Returns:
            Dictionary comparing Spark vs DuckDB for this query
        """
        # Get default recommendation
        default_result = self.select_engine(sql_query, parsed_query)
        
        # Analyze characteristics that favor each engine
        features = default_result.query_features
        estimate = default_result.data_estimate
        
        spark_advantages = []
        duckdb_advantages = []
        
        # Spark advantages
        if estimate.size_gb > 20:
            spark_advantages.append(f"Large data size ({estimate.size_gb:.1f}GB)")
        if features.join_count > 3:
            spark_advantages.append(f"Complex joins ({features.join_count})")
        if features.has_complex_etl_pattern:
            spark_advantages.append("Complex ETL pattern")
        if features.has_window_functions:
            spark_advantages.append("Window functions on large data")
        
        # DuckDB advantages  
        if estimate.size_gb < 10:
            duckdb_advantages.append(f"Small-medium data ({estimate.size_gb:.1f}GB)")
        if features.has_limit:
            duckdb_advantages.append("Interactive query with LIMIT")
        if features.is_simple_aggregation:
            duckdb_advantages.append("Simple aggregation")
        if features.interactive_indicators > 1:
            duckdb_advantages.append("Multiple interactive indicators")
        
        return {
            'recommended': default_result.engine,
            'confidence': default_result.confidence,
            'reasoning': default_result.reasoning,
            'spark_advantages': spark_advantages,
            'duckdb_advantages': duckdb_advantages,
            'data_size_gb': estimate.size_gb,
            'query_complexity': features.complexity_score,
            'analysis_details': default_result.to_dict()
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for the selector"""
        avg_time = self.total_analysis_time / max(self.selection_count, 1)
        
        return {
            'total_selections': self.selection_count,
            'total_analysis_time_ms': self.total_analysis_time,
            'average_analysis_time_ms': avg_time,
            'last_analysis_time_ms': self.last_selection.analysis_time_ms if self.last_selection else 0.0
        }
    
    def reset_stats(self):
        """Reset performance tracking statistics"""
        self.selection_count = 0
        self.total_analysis_time = 0.0
        self.last_selection = None