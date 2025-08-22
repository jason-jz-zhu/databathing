import ast
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from .validation_report import ValidationReport, ValidationStatus, ValidationMetrics


class CodeValidator(ABC):
    """Abstract base class for code validators"""
    
    def __init__(self):
        self.report = None
    
    @abstractmethod
    def validate_syntax(self, code: str) -> bool:
        """Validate syntax correctness"""
        pass
    
    @abstractmethod
    def calculate_complexity(self, code: str) -> float:
        """Calculate code complexity score (0-100, lower is better)"""
        pass
    
    @abstractmethod
    def calculate_readability(self, code: str) -> float:
        """Calculate readability score (0-100, higher is better)"""
        pass
    
    @abstractmethod
    def calculate_performance(self, code: str) -> float:
        """Estimate performance score (0-100, higher is better)"""
        pass
    
    @abstractmethod
    def check_best_practices(self, code: str) -> List[str]:
        """Check for best practice violations"""
        pass
    
    def calculate_maintainability(self, code: str) -> float:
        """Calculate maintainability score based on complexity and readability"""
        complexity = self.calculate_complexity(code)
        readability = self.calculate_readability(code)
        
        # Maintainability is inversely related to complexity and positively to readability
        maintainability = (readability + (100 - complexity)) / 2
        return min(100, max(0, maintainability))
    
    def validate(self, code: str, original_sql: str = "", engine_type: str = "") -> ValidationReport:
        """Perform complete validation and return report"""
        
        report = ValidationReport(
            engine_type=engine_type,
            generated_code=code,
            original_sql=original_sql
        )
        
        # 1. Syntax validation
        syntax_valid = self.validate_syntax(code)
        if syntax_valid:
            report.metrics.syntax_score = 100.0
            report.status = ValidationStatus.PASSED
        else:
            report.metrics.syntax_score = 0.0
            report.add_issue(
                ValidationStatus.FAILED,
                "Syntax validation failed",
                rule="syntax_check",
                suggestion="Check for missing imports, invalid method calls, or typos"
            )
        
        # 2. Calculate metrics (only if syntax is valid)
        if syntax_valid:
            try:
                report.metrics.complexity_score = self.calculate_complexity(code)
                report.metrics.readability_score = self.calculate_readability(code)
                report.metrics.performance_score = self.calculate_performance(code)
                report.metrics.maintainability_score = self.calculate_maintainability(code)
            except Exception as e:
                report.add_issue(
                    ValidationStatus.WARNING,
                    f"Error calculating metrics: {str(e)}",
                    rule="metrics_calculation"
                )
        
        # 3. Best practices check
        try:
            best_practice_issues = self.check_best_practices(code)
            for issue in best_practice_issues:
                report.add_issue(
                    ValidationStatus.WARNING,
                    issue,
                    rule="best_practices"
                )
        except Exception as e:
            report.add_issue(
                ValidationStatus.WARNING,
                f"Error checking best practices: {str(e)}",
                rule="best_practices_check"
            )
        
        # 4. Custom rules check
        try:
            from .custom_rules import get_custom_rules_registry
            registry = get_custom_rules_registry()
            custom_issues = registry.check_all_rules(code, engine_type)
            for issue in custom_issues:
                report.issues.append(issue)
        except Exception as e:
            report.add_issue(
                ValidationStatus.WARNING,
                f"Error checking custom rules: {str(e)}",
                rule="custom_rules_check"
            )
        
        # 4. Add metadata
        report.metadata.update({
            'code_length': len(code),
            'line_count': len(code.split('\n')),
            'sql_length': len(original_sql) if original_sql else 0
        })
        
        return report
    
    def _count_method_chains(self, code: str) -> int:
        """Count method chaining depth more accurately"""
        # Remove strings to avoid counting dots in string literals
        code_no_strings = re.sub(r'["\'][^"\']*["\']', '', code)
        
        # Split by lines and count max chaining per logical statement
        max_chain = 0
        
        # Handle line continuation with backslashes
        lines = code_no_strings.replace('\\\n', ' ').split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Count method calls (dots followed by method names)
            # Avoid counting decimal numbers or attribute access
            method_pattern = r'\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
            methods = re.findall(method_pattern, line)
            chain_length = len(methods)
            
            max_chain = max(max_chain, chain_length)
        
        return max_chain
    
    def _count_nested_parentheses(self, code: str) -> int:
        """Count maximum nesting depth of parentheses"""
        max_depth = 0
        current_depth = 0
        
        for char in code:
            if char == '(':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == ')':
                current_depth -= 1
        
        return max_depth
    
    def _has_meaningful_variable_names(self, code: str) -> bool:
        """Check if variable names are meaningful (not single letters)"""
        # Look for variable assignments
        var_pattern = r'(\w+)\s*='
        variables = re.findall(var_pattern, code)
        
        meaningful_count = 0
        total_count = len(variables)
        
        if total_count == 0:
            return True
        
        for var in variables:
            if len(var) > 2 and not var.startswith('_'):
                meaningful_count += 1
        
        return meaningful_count / total_count >= 0.8  # 80% should be meaningful