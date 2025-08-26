"""
Rule Engine for Auto Engine Selection

Applies rule-based decision logic to choose between Spark and DuckDB
based on query features, data estimates, and user context.
"""

from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from .query_analyzer import QueryFeatures
from .data_estimator import DataSizeEstimate


@dataclass
class EngineChoice:
    """Container for engine selection decision"""
    engine: str  # "spark" or "duckdb"
    confidence: float  # 0.0 to 1.0
    reasoning: str
    rule_name: str
    alternatives: Optional[Dict[str, float]] = None  # Alternative engines with scores
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert choice to dictionary for debugging"""
        return {
            'engine': self.engine,
            'confidence': self.confidence,
            'reasoning': self.reasoning,
            'rule_name': self.rule_name,
            'alternatives': self.alternatives or {}
        }


class RuleEngine:
    """Rule-based engine for Spark vs DuckDB selection"""
    
    def __init__(self):
        # Rule configuration - can be tuned based on testing
        self.rules = self._initialize_rules()
        
        # Confidence thresholds for rule categories
        self.confidence_thresholds = {
            'high': 0.85,
            'medium': 0.70,
            'low': 0.50
        }
    
    def select_engine(self, query_features: QueryFeatures, data_estimate: DataSizeEstimate, 
                     context: Optional[Any] = None) -> EngineChoice:
        """
        Select engine based on rules, features, and context
        
        Args:
            query_features: Extracted query characteristics
            data_estimate: Data size and characteristics estimate
            context: Optional user context with preferences
            
        Returns:
            EngineChoice with selected engine, confidence, and reasoning
        """
        # Apply rules in order of priority (high confidence first)
        
        # 1. High confidence rules - clear-cut decisions
        high_conf_choice = self._apply_high_confidence_rules(query_features, data_estimate, context)
        if high_conf_choice:
            return high_conf_choice
        
        # 2. Medium confidence rules - contextual decisions
        medium_conf_choice = self._apply_medium_confidence_rules(query_features, data_estimate, context)
        if medium_conf_choice:
            return medium_conf_choice
        
        # 3. Low confidence rules - safe defaults
        return self._apply_fallback_rules(query_features, data_estimate, context)
    
    def _initialize_rules(self) -> Dict[str, Callable]:
        """Initialize rule functions"""
        return {
            # High confidence rules
            'small_data_interactive': self._rule_small_data_interactive,
            'large_data_distributed': self._rule_large_data_distributed,
            'simple_analytics': self._rule_simple_analytics,
            'complex_etl': self._rule_complex_etl,
            
            # Medium confidence rules
            'speed_priority': self._rule_speed_priority,
            'cost_priority': self._rule_cost_priority,
            'fault_tolerance': self._rule_fault_tolerance,
            'interactive_requirement': self._rule_interactive_requirement,
            'batch_processing': self._rule_batch_processing,
            
            # Fallback rules
            'size_based_default': self._rule_size_based_default,
            'complexity_based_default': self._rule_complexity_based_default
        }
    
    def _apply_high_confidence_rules(self, features: QueryFeatures, estimate: DataSizeEstimate, 
                                   context: Optional[Any]) -> Optional[EngineChoice]:
        """Apply high confidence rules (0.85+ confidence)"""
        
        # Rule 1: Small data + interactive indicators → DuckDB
        if estimate.size_gb < 1.0 and features.has_limit:
            return EngineChoice(
                engine="duckdb",
                confidence=0.95,
                reasoning="Small data (<1GB) with LIMIT clause indicates interactive query - DuckDB optimal",
                rule_name="small_data_interactive"
            )
        
        # Rule 2: Very large data or many joins → Spark
        if estimate.size_gb > 50.0 or features.join_count > 5:
            return EngineChoice(
                engine="spark",
                confidence=0.92,
                reasoning=f"Large data ({estimate.size_gb:.1f}GB) or complex joins ({features.join_count}) requires distributed processing",
                rule_name="large_data_distributed"
            )
        
        # Rule 3: Simple aggregation on manageable data → DuckDB
        if (features.is_simple_aggregation and 
            estimate.size_gb < 5.0 and 
            features.join_count <= 2):
            return EngineChoice(
                engine="duckdb",
                confidence=0.90,
                reasoning="Simple aggregation on small-medium data - DuckDB columnar advantage",
                rule_name="simple_analytics"
            )
        
        # Rule 4: Complex ETL patterns → Spark
        if features.has_complex_etl_pattern:
            return EngineChoice(
                engine="spark",
                confidence=0.88,
                reasoning="Complex ETL pattern with multiple transformations requires distributed processing",
                rule_name="complex_etl"
            )
        
        return None
    
    def _apply_medium_confidence_rules(self, features: QueryFeatures, estimate: DataSizeEstimate,
                                     context: Optional[Any]) -> Optional[EngineChoice]:
        """Apply medium confidence rules (0.70-0.85 confidence)"""
        
        # Context-based rules if context is available
        if context:
            # Speed priority with manageable data → DuckDB
            if (getattr(context, 'performance_priority', '') == 'speed' and 
                estimate.size_gb < 10.0):
                return EngineChoice(
                    engine="duckdb",
                    confidence=0.82,
                    reasoning="Speed priority with manageable data - DuckDB zero-overhead advantage",
                    rule_name="speed_priority"
                )
            
            # Cost priority with reasonable data size → DuckDB
            if (getattr(context, 'performance_priority', '') == 'cost' and 
                estimate.size_gb < 20.0):
                return EngineChoice(
                    engine="duckdb", 
                    confidence=0.80,
                    reasoning="Cost optimization priority - DuckDB eliminates cluster overhead",
                    rule_name="cost_priority"
                )
            
            # Fault tolerance requirement → Spark
            if getattr(context, 'fault_tolerance', False) and estimate.size_gb > 5.0:
                return EngineChoice(
                    engine="spark",
                    confidence=0.78,
                    reasoning="Fault tolerance requirement with significant data - Spark reliability",
                    rule_name="fault_tolerance"
                )
            
            # Interactive requirement → DuckDB (if not too large)
            if (getattr(context, 'latency_requirement', '') == 'interactive' and 
                estimate.size_gb < 15.0):
                return EngineChoice(
                    engine="duckdb",
                    confidence=0.75,
                    reasoning="Interactive latency requirement - DuckDB instant startup",
                    rule_name="interactive_requirement"
                )
            
            # Batch processing → Spark (if substantial data)
            if (getattr(context, 'latency_requirement', '') == 'batch' and 
                estimate.size_gb > 10.0):
                return EngineChoice(
                    engine="spark",
                    confidence=0.73,
                    reasoning="Batch processing with substantial data - Spark distributed advantage",
                    rule_name="batch_processing"
                )
        
        # Query pattern based rules
        
        # Many interactive indicators → DuckDB
        if features.interactive_indicators >= 2 and estimate.size_gb < 10.0:
            return EngineChoice(
                engine="duckdb",
                confidence=0.77,
                reasoning=f"Multiple interactive indicators ({features.interactive_indicators}) suggest dashboard/analytics use",
                rule_name="interactive_patterns"
            )
        
        # Window functions with large data → Spark
        if features.has_window_functions and estimate.size_gb > 15.0:
            return EngineChoice(
                engine="spark",
                confidence=0.72,
                reasoning="Window functions on large data benefit from distributed processing",
                rule_name="windowed_analytics"
            )
        
        return None
    
    def _apply_fallback_rules(self, features: QueryFeatures, estimate: DataSizeEstimate,
                            context: Optional[Any]) -> EngineChoice:
        """Apply fallback rules when no high/medium confidence rules match"""
        
        # Size-based fallback
        if estimate.size_gb < 10.0:
            confidence = 0.70 if estimate.size_gb < 5.0 else 0.60
            return EngineChoice(
                engine="duckdb",
                confidence=confidence,
                reasoning=f"Small-medium data ({estimate.size_gb:.1f}GB) - DuckDB cost-effective default",
                rule_name="size_based_default"
            )
        else:
            confidence = 0.65 if estimate.size_gb > 20.0 else 0.55
            return EngineChoice(
                engine="spark",
                confidence=confidence,
                reasoning=f"Large data ({estimate.size_gb:.1f}GB) - Spark scale-safe default",
                rule_name="size_based_default"
            )
    
    def get_rule_explanation(self, rule_name: str) -> str:
        """Get detailed explanation of a specific rule"""
        explanations = {
            'small_data_interactive': 
                "Queries on small data (<1GB) with LIMIT clauses are typically interactive and benefit from DuckDB's zero-overhead architecture",
            
            'large_data_distributed':
                "Large datasets (>50GB) or complex multi-table joins require Spark's distributed processing capabilities",
            
            'simple_analytics':
                "Simple aggregations on small-medium data leverage DuckDB's columnar storage and vectorized execution",
            
            'complex_etl':
                "Complex ETL patterns with multiple CTEs and transformations need Spark's distributed fault-tolerant processing",
            
            'speed_priority':
                "When speed is prioritized for manageable data sizes, DuckDB's zero cluster overhead provides fastest results",
            
            'cost_priority':
                "DuckDB eliminates cluster costs and is optimal for cost-conscious workloads on reasonable data sizes",
            
            'fault_tolerance':
                "Spark provides automatic failure recovery and fault tolerance for business-critical workloads",
            
            'interactive_requirement':
                "Interactive applications need instant startup and response times that DuckDB's embedded architecture provides",
            
            'size_based_default':
                "Conservative defaults based on data size: DuckDB for <10GB (cost-effective), Spark for >10GB (scale-safe)"
        }
        
        return explanations.get(rule_name, f"No explanation available for rule: {rule_name}")
    
    # Individual rule functions (could be moved to separate file if needed)
    
    def _rule_small_data_interactive(self, features: QueryFeatures, estimate: DataSizeEstimate, 
                                   context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for small interactive queries"""
        if estimate.size_gb < 1.0 and features.has_limit:
            return EngineChoice("duckdb", 0.95, "Small data + interactive", "small_data_interactive")
        return None
    
    def _rule_large_data_distributed(self, features: QueryFeatures, estimate: DataSizeEstimate,
                                   context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for large data requiring distribution"""
        if estimate.size_gb > 50.0 or features.join_count > 5:
            return EngineChoice("spark", 0.92, "Large/complex data", "large_data_distributed")
        return None
    
    def _rule_simple_analytics(self, features: QueryFeatures, estimate: DataSizeEstimate,
                             context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for simple analytical queries"""
        if features.is_simple_aggregation and estimate.size_gb < 5.0:
            return EngineChoice("duckdb", 0.90, "Simple analytics", "simple_analytics")
        return None
    
    def _rule_complex_etl(self, features: QueryFeatures, estimate: DataSizeEstimate,
                        context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for complex ETL patterns"""
        if features.has_complex_etl_pattern:
            return EngineChoice("spark", 0.88, "Complex ETL", "complex_etl")
        return None
    
    def _rule_speed_priority(self, features: QueryFeatures, estimate: DataSizeEstimate,
                           context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for speed-prioritized queries"""
        if context and getattr(context, 'performance_priority', '') == 'speed' and estimate.size_gb < 10.0:
            return EngineChoice("duckdb", 0.82, "Speed priority", "speed_priority")
        return None
    
    def _rule_cost_priority(self, features: QueryFeatures, estimate: DataSizeEstimate,
                          context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for cost-optimized queries"""  
        if context and getattr(context, 'performance_priority', '') == 'cost' and estimate.size_gb < 20.0:
            return EngineChoice("duckdb", 0.80, "Cost priority", "cost_priority")
        return None
    
    def _rule_fault_tolerance(self, features: QueryFeatures, estimate: DataSizeEstimate,
                            context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for fault tolerance requirements"""
        if context and getattr(context, 'fault_tolerance', False) and estimate.size_gb > 5.0:
            return EngineChoice("spark", 0.78, "Fault tolerance", "fault_tolerance")
        return None
    
    def _rule_interactive_requirement(self, features: QueryFeatures, estimate: DataSizeEstimate,
                                    context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for interactive latency requirements"""
        if context and getattr(context, 'latency_requirement', '') == 'interactive' and estimate.size_gb < 15.0:
            return EngineChoice("duckdb", 0.75, "Interactive requirement", "interactive_requirement")
        return None
    
    def _rule_batch_processing(self, features: QueryFeatures, estimate: DataSizeEstimate,
                             context: Optional[Any]) -> Optional[EngineChoice]:
        """Rule for batch processing workloads"""
        if context and getattr(context, 'latency_requirement', '') == 'batch' and estimate.size_gb > 10.0:
            return EngineChoice("spark", 0.73, "Batch processing", "batch_processing")
        return None
    
    def _rule_size_based_default(self, features: QueryFeatures, estimate: DataSizeEstimate,
                               context: Optional[Any]) -> EngineChoice:
        """Fallback rule based on data size"""
        if estimate.size_gb < 10.0:
            confidence = 0.70 if estimate.size_gb < 5.0 else 0.60
            return EngineChoice("duckdb", confidence, "Size-based default", "size_based_default")
        else:
            confidence = 0.65 if estimate.size_gb > 20.0 else 0.55
            return EngineChoice("spark", confidence, "Size-based default", "size_based_default")
    
    def _rule_complexity_based_default(self, features: QueryFeatures, estimate: DataSizeEstimate,
                                     context: Optional[Any]) -> EngineChoice:
        """Fallback rule based on query complexity"""
        if features.complexity_score > 8.0:
            return EngineChoice("spark", 0.60, "High complexity default", "complexity_based_default")
        else:
            return EngineChoice("duckdb", 0.55, "Low complexity default", "complexity_based_default")