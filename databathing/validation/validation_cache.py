import hashlib
import json
from typing import Dict, Optional, Any
from .validation_report import ValidationReport


class ValidationCache:
    """Cache validation results for identical code to improve performance"""
    
    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        self.access_order = []  # For LRU eviction
    
    def _generate_cache_key(self, code: str, engine_type: str, original_sql: str = "") -> str:
        """Generate a unique cache key for the validation request"""
        # Create a hash of the inputs to use as cache key
        content = f"{code}|{engine_type}|{original_sql}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def get(self, code: str, engine_type: str, original_sql: str = "") -> Optional[ValidationReport]:
        """Retrieve cached validation result if available"""
        cache_key = self._generate_cache_key(code, engine_type, original_sql)
        
        if cache_key in self.cache:
            # Update access order for LRU
            self.access_order.remove(cache_key)
            self.access_order.append(cache_key)
            
            # Reconstruct ValidationReport from cached data
            cached_data = self.cache[cache_key]
            return self._deserialize_report(cached_data)
        
        return None
    
    def set(self, code: str, engine_type: str, original_sql: str, report: ValidationReport):
        """Cache a validation result"""
        cache_key = self._generate_cache_key(code, engine_type, original_sql)
        
        # Evict oldest entries if cache is full
        if len(self.cache) >= self.max_size and cache_key not in self.cache:
            oldest_key = self.access_order.pop(0)
            del self.cache[oldest_key]
        
        # Cache the serialized report
        self.cache[cache_key] = self._serialize_report(report)
        
        # Update access order
        if cache_key in self.access_order:
            self.access_order.remove(cache_key)
        self.access_order.append(cache_key)
    
    def _serialize_report(self, report: ValidationReport) -> Dict[str, Any]:
        """Convert ValidationReport to cacheable dictionary"""
        return report.to_dict()
    
    def _deserialize_report(self, data: Dict[str, Any]) -> ValidationReport:
        """Reconstruct ValidationReport from cached dictionary"""
        from .validation_report import ValidationReport, ValidationStatus, ValidationMetrics, ValidationIssue
        
        # Reconstruct metrics
        metrics_data = data.get('metrics', {})
        metrics = ValidationMetrics(
            syntax_score=metrics_data.get('syntax_score', 0.0),
            complexity_score=metrics_data.get('complexity_score', 0.0),
            readability_score=metrics_data.get('readability_score', 0.0),
            performance_score=metrics_data.get('performance_score', 0.0),
            maintainability_score=metrics_data.get('maintainability_score', 0.0)
        )
        
        # Reconstruct issues
        issues = []
        for issue_data in data.get('issues', []):
            issue = ValidationIssue(
                severity=ValidationStatus(issue_data.get('severity', 'warning')),
                message=issue_data.get('message', ''),
                line_number=issue_data.get('line_number'),
                rule=issue_data.get('rule'),
                suggestion=issue_data.get('suggestion')
            )
            issues.append(issue)
        
        # Reconstruct report
        report = ValidationReport(
            engine_type=data.get('engine_type', ''),
            generated_code='',  # Don't store code in cache
            original_sql=''     # Don't store SQL in cache
        )
        
        report.status = ValidationStatus(data.get('status', 'failed'))
        report.issues = issues
        report.metrics = metrics
        report.metadata = data.get('metadata', {})
        
        return report
    
    def clear(self):
        """Clear all cached validation results"""
        self.cache.clear()
        self.access_order.clear()
    
    def size(self) -> int:
        """Get current cache size"""
        return len(self.cache)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'hit_ratio': getattr(self, '_hits', 0) / max(getattr(self, '_requests', 1), 1)
        }


# Global cache instance
_global_cache = ValidationCache()

def get_validation_cache() -> ValidationCache:
    """Get the global validation cache instance"""
    return _global_cache