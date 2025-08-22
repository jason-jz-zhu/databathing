from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable, Optional
from .validation_report import ValidationIssue, ValidationStatus
import re


class ValidationRule(ABC):
    """Abstract base class for custom validation rules"""
    
    def __init__(self, name: str, description: str, severity: ValidationStatus = ValidationStatus.WARNING):
        self.name = name
        self.description = description
        self.severity = severity
    
    @abstractmethod
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        """Check code against this rule and return any issues found"""
        pass


class RegexRule(ValidationRule):
    """Validation rule based on regex pattern matching"""
    
    def __init__(
        self, 
        name: str, 
        description: str, 
        pattern: str, 
        message: str,
        suggestion: str = None,
        severity: ValidationStatus = ValidationStatus.WARNING,
        applicable_engines: List[str] = None
    ):
        super().__init__(name, description, severity)
        self.pattern = re.compile(pattern)
        self.message = message
        self.suggestion = suggestion
        self.applicable_engines = applicable_engines or ["spark", "duckdb"]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        """Check code against regex pattern"""
        if engine_type not in self.applicable_engines:
            return []
        
        issues = []
        matches = list(self.pattern.finditer(code))
        
        for match in matches:
            # Calculate line number
            line_number = code[:match.start()].count('\n') + 1
            
            issue = ValidationIssue(
                severity=self.severity,
                message=self.message,
                line_number=line_number,
                rule=self.name,
                suggestion=self.suggestion
            )
            issues.append(issue)
        
        return issues


class FunctionRule(ValidationRule):
    """Validation rule based on custom function"""
    
    def __init__(
        self,
        name: str,
        description: str,
        check_function: Callable[[str, str], List[ValidationIssue]],
        severity: ValidationStatus = ValidationStatus.WARNING
    ):
        super().__init__(name, description, severity)
        self.check_function = check_function
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        """Check code using custom function"""
        return self.check_function(code, engine_type)


class CustomRulesRegistry:
    """Registry for managing custom validation rules"""
    
    def __init__(self):
        self.rules: Dict[str, ValidationRule] = {}
        self._load_default_rules()
    
    def register_rule(self, rule: ValidationRule):
        """Register a custom validation rule"""
        self.rules[rule.name] = rule
    
    def unregister_rule(self, rule_name: str):
        """Remove a custom validation rule"""
        if rule_name in self.rules:
            del self.rules[rule_name]
    
    def get_rule(self, rule_name: str) -> Optional[ValidationRule]:
        """Get a specific validation rule"""
        return self.rules.get(rule_name)
    
    def get_rules_for_engine(self, engine_type: str) -> List[ValidationRule]:
        """Get all rules applicable to a specific engine"""
        applicable_rules = []
        
        for rule in self.rules.values():
            # Check if rule is applicable to this engine
            if hasattr(rule, 'applicable_engines'):
                if engine_type in rule.applicable_engines:
                    applicable_rules.append(rule)
            else:
                # If no engine restriction, apply to all
                applicable_rules.append(rule)
        
        return applicable_rules
    
    def check_all_rules(self, code: str, engine_type: str) -> List[ValidationIssue]:
        """Run all applicable rules against code"""
        all_issues = []
        
        for rule in self.get_rules_for_engine(engine_type):
            try:
                issues = rule.check(code, engine_type)
                all_issues.extend(issues)
            except Exception as e:
                # If rule fails, create an issue about the rule failure
                issue = ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message=f"Custom rule '{rule.name}' failed: {str(e)}",
                    rule=rule.name
                )
                all_issues.append(issue)
        
        return all_issues
    
    def _load_default_rules(self):
        """Load some useful default custom rules"""
        
        # Rule: Avoid hardcoded values in production code
        hardcoded_rule = RegexRule(
            name="no_hardcoded_values",
            description="Avoid hardcoded values in production code",
            pattern=r'["\'][0-9]{4}-[0-9]{2}-[0-9]{2}["\']|["\']localhost["\']|["\']127\.0\.0\.1["\']',
            message="Hardcoded values detected - consider using configuration",
            suggestion="Use environment variables or configuration files",
            severity=ValidationStatus.WARNING
        )
        self.register_rule(hardcoded_rule)
        
        # Rule: Spark - prefer DataFrame API over SQL strings
        spark_api_rule = RegexRule(
            name="prefer_dataframe_api",
            description="Prefer DataFrame API over SQL strings in PySpark",
            pattern=r'\.sql\(["\'].*SELECT.*["\']',
            message="Consider using DataFrame API instead of SQL strings",
            suggestion="Use .select(), .filter(), .groupBy() methods",
            applicable_engines=["spark"],
            severity=ValidationStatus.WARNING
        )
        self.register_rule(spark_api_rule)
        
        # Rule: DuckDB - avoid very long SQL strings
        long_sql_rule = RegexRule(
            name="avoid_long_sql",
            description="Avoid very long SQL strings",
            pattern=r'duckdb\.sql\(["\'][^"\']{500,}["\']',
            message="Very long SQL string detected",
            suggestion="Consider breaking into smaller queries or using CTEs",
            applicable_engines=["duckdb"],
            severity=ValidationStatus.WARNING
        )
        self.register_rule(long_sql_rule)
    
    def list_rules(self) -> Dict[str, str]:
        """List all registered rules with descriptions"""
        return {name: rule.description for name, rule in self.rules.items()}


# Global registry instance
_global_registry = CustomRulesRegistry()

def get_custom_rules_registry() -> CustomRulesRegistry:
    """Get the global custom rules registry"""
    return _global_registry


# Convenience functions for common use cases
def add_regex_rule(
    name: str, 
    pattern: str, 
    message: str, 
    suggestion: str = None,
    engines: List[str] = None,
    severity: str = "warning"
):
    """Add a regex-based custom rule"""
    registry = get_custom_rules_registry()
    
    severity_enum = ValidationStatus.WARNING
    if severity.lower() == "failed":
        severity_enum = ValidationStatus.FAILED
    
    rule = RegexRule(
        name=name,
        description=f"Custom rule: {message}",
        pattern=pattern,
        message=message,
        suggestion=suggestion,
        applicable_engines=engines or ["spark", "duckdb"],
        severity=severity_enum
    )
    
    registry.register_rule(rule)


def add_function_rule(
    name: str,
    description: str,
    check_function: Callable[[str, str], List[ValidationIssue]],
    severity: str = "warning"
):
    """Add a function-based custom rule"""
    registry = get_custom_rules_registry()
    
    severity_enum = ValidationStatus.WARNING
    if severity.lower() == "failed":
        severity_enum = ValidationStatus.FAILED
    
    rule = FunctionRule(
        name=name,
        description=description,
        check_function=check_function,
        severity=severity_enum
    )
    
    registry.register_rule(rule)