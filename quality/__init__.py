"""
Quality Module - Data Quality Monitoring and Validation

This module provides comprehensive data quality checking capabilities
for the market data warehouse, including validation, anomaly detection,
and quality reporting.

Main Components:
- DataQualityChecker: Primary class for quality validation
- Completeness, accuracy, and consistency checks
- Price and volume anomaly detection
- Timeliness monitoring and alerting

Usage:
    from quality import DataQualityChecker
    
    checker = DataQualityChecker()
    results = checker.run_all_checks()
"""

from .data_quality_checker import DataQualityChecker

__all__ = [
    'DataQualityChecker',
]

__version__ = '1.0.0'
