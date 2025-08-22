from typing import Optional
from .code_validator import CodeValidator
from .spark_validator import SparkValidator
from .duckdb_validator import DuckDBValidator


class ValidatorFactory:
    """Factory class to create appropriate validators based on engine type"""
    
    @staticmethod
    def create_validator(engine_type: str) -> Optional[CodeValidator]:
        """Create validator instance based on engine type"""
        engine_type = engine_type.lower().strip()
        
        if engine_type in ["spark", "pyspark"]:
            return SparkValidator()
        elif engine_type in ["duckdb"]:
            return DuckDBValidator()
        else:
            raise ValueError(f"Unsupported engine type: {engine_type}. Supported types: spark, duckdb")
    
    @staticmethod
    def get_supported_engines() -> list:
        """Get list of supported engine types"""
        return ["spark", "duckdb"]


def validate_code(code: str, engine_type: str = None, original_sql: str = "", use_cache: bool = True, engine: str = None):
    """Convenience function to validate code with appropriate validator
    
    Args:
        code: The code to validate
        engine_type: Engine type (preferred parameter name)
        original_sql: Original SQL query (optional)
        use_cache: Whether to use caching
        engine: Engine type (alternative parameter name for backward compatibility)
    """
    # Handle backward compatibility - accept both 'engine_type' and 'engine'
    if engine_type is None and engine is not None:
        engine_type = engine
    elif engine_type is None and engine is None:
        raise ValueError("Must specify either 'engine_type' or 'engine' parameter")
    if use_cache:
        from .validation_cache import get_validation_cache
        cache = get_validation_cache()
        
        # Try to get from cache first
        cached_result = cache.get(code, engine_type, original_sql)
        if cached_result is not None:
            return cached_result
    
    # Generate new validation result
    validator = ValidatorFactory.create_validator(engine_type)
    result = validator.validate(code, original_sql, engine_type)
    
    # Cache the result if caching is enabled
    if use_cache:
        cache.set(code, engine_type, original_sql, result)
    
    return result