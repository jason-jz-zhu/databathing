"""
Auto-Selection Module for Intelligent Engine Selection

This module provides rule-based automatic engine selection between Spark and DuckDB
based on query analysis, data estimation, and performance requirements.

Main components:
- QueryAnalyzer: Extracts features from SQL queries
- DataEstimator: Estimates data size and characteristics  
- RuleEngine: Applies decision rules with confidence scoring
- AutoEngineSelector: Orchestrates the selection process
"""

from .query_analyzer import QueryAnalyzer, QueryFeatures
from .data_estimator import DataEstimator, DataSizeEstimate
from .rule_engine import RuleEngine, EngineChoice
from .engine_selector import AutoEngineSelector, SelectionContext

__all__ = [
    "QueryAnalyzer",
    "QueryFeatures", 
    "DataEstimator",
    "DataSizeEstimate",
    "RuleEngine",
    "EngineChoice",
    "AutoEngineSelector",
    "SelectionContext"
]