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


def validate_code(code: str, engine_type: str, original_sql: str = ""):
    """Convenience function to validate code with appropriate validator"""
    validator = ValidatorFactory.create_validator(engine_type)
    return validator.validate(code, original_sql, engine_type)