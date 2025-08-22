from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum


class ValidationStatus(Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class ValidationIssue:
    """Represents a single validation issue"""
    severity: ValidationStatus
    message: str
    line_number: Optional[int] = None
    column: Optional[int] = None
    rule: Optional[str] = None
    suggestion: Optional[str] = None


@dataclass
class ValidationMetrics:
    """Metrics for code quality scoring"""
    syntax_score: float = 0.0  # 0-100, syntax correctness
    complexity_score: float = 0.0  # 0-100, lower is better
    readability_score: float = 0.0  # 0-100, code readability
    performance_score: float = 0.0  # 0-100, estimated performance
    maintainability_score: float = 0.0  # 0-100, maintainability
    
    @property
    def overall_score(self) -> float:
        """Calculate weighted overall score"""
        weights = {
            'syntax': 0.3,          # Reduced from 40% to 30%
            'complexity': 0.15,     # Reduced from 20% to 15%
            'readability': 0.15,    # Reduced from 20% to 15%
            'performance': 0.3,     # Increased from 10% to 30% - performance is critical!
            'maintainability': 0.1  # Kept at 10%
        }
        
        return (
            self.syntax_score * weights['syntax'] +
            (100 - self.complexity_score) * weights['complexity'] +  # Lower complexity is better
            self.readability_score * weights['readability'] +
            self.performance_score * weights['performance'] +
            self.maintainability_score * weights['maintainability']
        )


@dataclass
class ValidationReport:
    """Complete validation report for generated code"""
    
    # Basic info
    engine_type: str
    generated_code: str
    original_sql: str
    
    # Validation results
    status: ValidationStatus = ValidationStatus.FAILED
    issues: List[ValidationIssue] = field(default_factory=list)
    metrics: ValidationMetrics = field(default_factory=ValidationMetrics)
    
    # Additional data
    execution_time: Optional[float] = None
    memory_usage: Optional[int] = None
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_issue(self, severity: ValidationStatus, message: str, **kwargs):
        """Add a validation issue"""
        issue = ValidationIssue(severity=severity, message=message, **kwargs)
        self.issues.append(issue)
        
        # Update overall status based on worst issue
        if severity == ValidationStatus.FAILED:
            self.status = ValidationStatus.FAILED
        elif severity == ValidationStatus.WARNING and self.status != ValidationStatus.FAILED:
            self.status = ValidationStatus.WARNING
    
    def get_issues_by_severity(self, severity: ValidationStatus) -> List[ValidationIssue]:
        """Get all issues of a specific severity"""
        return [issue for issue in self.issues if issue.severity == severity]
    
    @property
    def error_count(self) -> int:
        """Count of error-level issues"""
        return len(self.get_issues_by_severity(ValidationStatus.FAILED))
    
    @property
    def warning_count(self) -> int:
        """Count of warning-level issues"""
        return len(self.get_issues_by_severity(ValidationStatus.WARNING))
    
    @property
    def is_valid(self) -> bool:
        """True if code passed validation (no errors)"""
        return self.status != ValidationStatus.FAILED
    
    def get_grade(self) -> str:
        """Get letter grade based on overall score"""
        score = self.metrics.overall_score
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary for serialization"""
        return {
            'engine_type': self.engine_type,
            'status': self.status.value,
            'overall_score': self.metrics.overall_score,
            'grade': self.get_grade(),
            'error_count': self.error_count,
            'warning_count': self.warning_count,
            'issues': [
                {
                    'severity': issue.severity.value,
                    'message': issue.message,
                    'line_number': issue.line_number,
                    'rule': issue.rule,
                    'suggestion': issue.suggestion
                }
                for issue in self.issues
            ],
            'metrics': {
                'syntax_score': self.metrics.syntax_score,
                'complexity_score': self.metrics.complexity_score,
                'readability_score': self.metrics.readability_score,
                'performance_score': self.metrics.performance_score,
                'maintainability_score': self.metrics.maintainability_score,
                'overall_score': self.metrics.overall_score
            },
            'metadata': self.metadata
        }
    
    def __str__(self) -> str:
        """Human-readable report summary"""
        lines = [
            f"Validation Report for {self.engine_type.upper()} Code",
            f"Status: {self.status.value.upper()}",
            f"Overall Score: {self.metrics.overall_score:.1f}/100 (Grade: {self.get_grade()})",
            f"Issues: {self.error_count} errors, {self.warning_count} warnings"
        ]
        
        if self.issues:
            lines.append("\nIssues:")
            for issue in self.issues:
                prefix = "❌" if issue.severity == ValidationStatus.FAILED else "⚠️"
                lines.append(f"  {prefix} {issue.message}")
                if issue.suggestion:
                    lines.append(f"     💡 {issue.suggestion}")
        
        return "\n".join(lines)