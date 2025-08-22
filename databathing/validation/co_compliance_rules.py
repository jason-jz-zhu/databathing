"""
Capital One Banking Compliance Validation Rules
Enhanced validation rules specifically designed for financial services and banking compliance
"""

import re
from typing import List, Dict, Any
from .custom_rules import ValidationRule, ValidationIssue, ValidationStatus


class PCIComplianceRule(ValidationRule):
    """PCI DSS compliance validation for credit card data handling"""
    
    def __init__(self):
        super().__init__(
            name="pci_dss_compliance", 
            description="Validates PCI DSS compliance for credit card data handling",
            severity=ValidationStatus.FAILED
        )
        
        self.credit_card_patterns = [
            r'\b(?:\d{4}[-\s]?){3}\d{4}\b',  # Credit card numbers
            r'\b4\d{15}\b',  # Visa
            r'\b5[1-5]\d{14}\b',  # MasterCard
            r'\b3[47]\d{13}\b',  # American Express
            r'\b6011\d{12}\b'  # Discover
        ]
        
        self.pci_violations = [
            (r'(?i)(?:select|from)\s+.*(?:cc|credit_card|card_number).*(?:where|having).*[=<>].*\d{13,19}', 
             "Hardcoded credit card numbers detected in query"),
            (r'(?i)cvv|cvc|security_code.*[=].*\d{3,4}', 
             "Hardcoded CVV/CVC codes detected"),
            (r'(?i)(?:select|from).*(?:full_card|primary_account_number)(?!.*mask)', 
             "Unmasked credit card data selection without proper masking"),
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check for hardcoded credit card patterns
        for pattern in self.credit_card_patterns:
            matches = list(re.finditer(pattern, code))
            for match in matches:
                line_number = code[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    severity=ValidationStatus.FAILED,
                    message="Potential credit card number found in code - PCI DSS violation",
                    line_number=line_number,
                    rule=self.name,
                    suggestion="Remove hardcoded credit card data and use proper tokenization"
                ))
        
        # Check for PCI violations
        for pattern, message in self.pci_violations:
            matches = list(re.finditer(pattern, code))
            for match in matches:
                line_number = code[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    severity=ValidationStatus.FAILED,
                    message=message,
                    line_number=line_number,
                    rule=self.name,
                    suggestion="Implement proper PCI DSS data handling procedures"
                ))
        
        return issues


class PIIDataDetectionRule(ValidationRule):
    """Detect and validate Personally Identifiable Information (PII) handling"""
    
    def __init__(self):
        super().__init__(
            name="pii_data_detection", 
            description="Detects PII data and validates proper handling",
            severity=ValidationStatus.WARNING
        )
        
        self.pii_patterns = {
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'address': r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr)\b'
        }
        
        self.sensitive_column_indicators = [
            'ssn', 'social_security', 'tax_id', 'sin',
            'customer_id', 'account_number', 'routing_number',
            'email', 'phone', 'mobile', 'address', 'zipcode',
            'dob', 'date_of_birth', 'birthdate', 'birth_date',
            'salary', 'income', 'balance', 'credit_score',
            'mother_maiden', 'maiden_name'
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check for hardcoded PII values
        for pii_type, pattern in self.pii_patterns.items():
            matches = list(re.finditer(pattern, code))
            for match in matches:
                line_number = code[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    severity=ValidationStatus.FAILED,
                    message=f"Hardcoded {pii_type.upper()} detected in code",
                    line_number=line_number,
                    rule=self.name,
                    suggestion=f"Remove hardcoded {pii_type} and implement proper data masking"
                ))
        
        # Check for unmasked sensitive columns
        for indicator in self.sensitive_column_indicators:
            pattern = rf'(?i)(?:select|selectExpr).*\b{indicator}\b(?!.*(?:mask|encrypt|hash))'
            matches = list(re.finditer(pattern, code))
            for match in matches:
                line_number = code[:match.start()].count('\n') + 1
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message=f"Potentially sensitive column '{indicator}' selected without masking",
                    line_number=line_number,
                    rule=self.name,
                    suggestion="Consider implementing data masking for sensitive columns"
                ))
        
        return issues


class SOXComplianceRule(ValidationRule):
    """Sarbanes-Oxley Act compliance validation"""
    
    def __init__(self):
        super().__init__(
            name="sox_compliance", 
            description="Validates SOX compliance for financial reporting data",
            severity=ValidationStatus.WARNING
        )
        
        self.financial_tables = [
            'revenue', 'income', 'expense', 'asset', 'liability', 'equity',
            'financial', 'accounting', 'ledger', 'journal', 'transaction',
            'payment', 'receipt', 'invoice', 'billing'
        ]
        
        self.audit_requirements = [
            'processing_timestamp', 'processed_by', 'processing_system',
            'data_source', 'created_date', 'modified_date', 'audit_trail'
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check if financial data is being processed
        financial_data_detected = False
        for table in self.financial_tables:
            if re.search(rf'(?i)\b{table}\b', code):
                financial_data_detected = True
                break
        
        if financial_data_detected:
            # Check for audit trail columns
            has_audit_columns = any(
                re.search(rf'(?i)\b{col}\b', code) for col in self.audit_requirements
            )
            
            if not has_audit_columns:
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message="Financial data processing detected without audit trail columns",
                    line_number=1,
                    rule=self.name,
                    suggestion="Add audit trail columns (timestamp, user, system) for SOX compliance"
                ))
        
        return issues


class GLBAComplianceRule(ValidationRule):
    """Gramm-Leach-Bliley Act compliance validation"""
    
    def __init__(self):
        super().__init__(
            name="glba_compliance", 
            description="Validates GLBA compliance for customer financial information",
            severity=ValidationStatus.WARNING
        )
        
        self.customer_financial_indicators = [
            'customer', 'client', 'consumer', 'account_holder',
            'financial_info', 'nonpublic_personal', 'npi'
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check for customer financial information access
        customer_data_detected = any(
            re.search(rf'(?i)\b{indicator}\b', code) for indicator in self.customer_financial_indicators
        )
        
        if customer_data_detected:
            # Check for proper data classification
            if not re.search(r'(?i)(?:confidential|restricted|internal)', code):
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message="Customer financial information accessed without proper data classification",
                    line_number=1,
                    rule=self.name,
                    suggestion="Ensure proper data classification and access controls for customer financial data"
                ))
        
        return issues


class AMLComplianceRule(ValidationRule):
    """Anti-Money Laundering (AML) compliance validation"""
    
    def __init__(self):
        super().__init__(
            name="aml_compliance", 
            description="Validates AML compliance for transaction monitoring",
            severity=ValidationStatus.WARNING
        )
        
        self.aml_indicators = [
            'transaction', 'transfer', 'payment', 'wire',
            'cash', 'deposit', 'withdrawal', 'money_transfer'
        ]
        
        self.suspicious_patterns = [
            r'(?i)amount\s*[>]\s*10000',  # Large transactions
            r'(?i)cash.*amount',  # Cash transactions
            r'(?i)foreign.*transfer',  # International transfers
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check for transaction data processing
        transaction_data_detected = any(
            re.search(rf'(?i)\b{indicator}\b', code) for indicator in self.aml_indicators
        )
        
        if transaction_data_detected:
            # Check for suspicious activity patterns
            for pattern in self.suspicious_patterns:
                if re.search(pattern, code):
                    issues.append(ValidationIssue(
                        severity=ValidationStatus.WARNING,
                        message="Transaction processing detected - ensure AML monitoring is in place",
                        line_number=1,
                        rule=self.name,
                        suggestion="Implement proper AML transaction monitoring and suspicious activity reporting"
                    ))
                    break
        
        return issues


class DataRetentionRule(ValidationRule):
    """Data retention policy compliance validation"""
    
    def __init__(self):
        super().__init__(
            name="data_retention_compliance", 
            description="Validates data retention policy compliance",
            severity=ValidationStatus.WARNING
        )
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        # Check for historical data access without date filters
        if re.search(r'(?i)(?:select|from).*(?:history|historical|archive)', code):
            if not re.search(r'(?i)(?:where|filter).*(?:date|timestamp|year)', code):
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message="Historical data access without date-based filtering",
                    line_number=1,
                    rule=self.name,
                    suggestion="Add date-based filters to comply with data retention policies"
                ))
        
        return issues


class EncryptionRequirementRule(ValidationRule):
    """Encryption requirement validation for sensitive data"""
    
    def __init__(self):
        super().__init__(
            name="encryption_requirement", 
            description="Validates encryption requirements for sensitive data",
            severity=ValidationStatus.WARNING
        )
        
        self.encryption_required_fields = [
            'ssn', 'tax_id', 'account_number', 'routing_number',
            'credit_card', 'cc_number', 'card_number', 'cvv', 'cvc',
            'pin', 'password', 'secret', 'key'
        ]
    
    def check(self, code: str, engine_type: str) -> List[ValidationIssue]:
        issues = []
        
        for field in self.encryption_required_fields:
            pattern = rf'(?i)(?:select|selectExpr).*\b{field}\b(?!.*(?:encrypt|aes|decrypt))'
            if re.search(pattern, code):
                issues.append(ValidationIssue(
                    severity=ValidationStatus.WARNING,
                    message=f"Field '{field}' may require encryption at rest and in transit",
                    line_number=1,
                    rule=self.name,
                    suggestion="Implement proper encryption for sensitive fields"
                ))
        
        return issues


# Banking compliance rules registry
BANKING_COMPLIANCE_RULES = [
    PCIComplianceRule(),
    PIIDataDetectionRule(),
    SOXComplianceRule(),
    GLBAComplianceRule(),
    AMLComplianceRule(),
    DataRetentionRule(),
    EncryptionRequirementRule()
]


def get_banking_compliance_rules() -> List[ValidationRule]:
    """Get all banking compliance rules"""
    return BANKING_COMPLIANCE_RULES


def validate_banking_compliance(code: str, engine_type: str) -> Dict[str, Any]:
    """Run all banking compliance validations"""
    all_issues = []
    compliance_scores = {}
    
    for rule in BANKING_COMPLIANCE_RULES:
        try:
            issues = rule.check(code, engine_type)
            all_issues.extend(issues)
            
            # Calculate rule-specific compliance score
            if issues:
                failed_issues = [i for i in issues if i.severity == ValidationStatus.FAILED]
                warning_issues = [i for i in issues if i.severity == ValidationStatus.WARNING]
                score = 100 - (len(failed_issues) * 30) - (len(warning_issues) * 10)
                compliance_scores[rule.name] = max(0, score)
            else:
                compliance_scores[rule.name] = 100
                
        except Exception as e:
            all_issues.append(ValidationIssue(
                severity=ValidationStatus.WARNING,
                message=f"Compliance rule '{rule.name}' failed to execute: {str(e)}",
                rule=rule.name
            ))
            compliance_scores[rule.name] = 50
    
    # Calculate overall compliance score
    overall_score = sum(compliance_scores.values()) / len(compliance_scores) if compliance_scores else 0
    
    return {
        'issues': all_issues,
        'rule_scores': compliance_scores,
        'overall_compliance_score': overall_score,
        'compliance_grade': _get_compliance_grade(overall_score),
        'critical_issues': len([i for i in all_issues if i.severity == ValidationStatus.FAILED]),
        'warning_issues': len([i for i in all_issues if i.severity == ValidationStatus.WARNING])
    }


def _get_compliance_grade(score: float) -> str:
    """Convert compliance score to letter grade"""
    if score >= 95:
        return 'A+'
    elif score >= 90:
        return 'A'
    elif score >= 85:
        return 'B+'
    elif score >= 80:
        return 'B'
    elif score >= 75:
        return 'C+'
    elif score >= 70:
        return 'C'
    elif score >= 60:
        return 'D'
    else:
        return 'F'