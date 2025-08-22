"""
Banking Domain Templates for Capital One
Pre-built SQL templates for common banking operations and regulatory requirements
"""

from .customer_360 import Customer360Templates
from .risk_management import RiskManagementTemplates
from .regulatory_reporting import RegulatoryReportingTemplates
from .fraud_detection import FraudDetectionTemplates
from .aml_patterns import AMLPatternTemplates

__all__ = [
    "Customer360Templates",
    "RiskManagementTemplates", 
    "RegulatoryReportingTemplates",
    "FraudDetectionTemplates",
    "AMLPatternTemplates"
]