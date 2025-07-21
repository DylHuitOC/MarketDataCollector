"""
Extract Module - Market Data Extraction from FMP API

This module handles all data extraction operations from the Financial Modeling Prep API.
It provides classes and functions for fetching real-time and historical market data
with proper error handling, rate limiting, and data validation.

Main Components:
- MarketDataExtractor: Primary class for extracting market data
- API rate limiting and error handling utilities
- Data validation and cleaning functions

Usage:
    from extract import MarketDataExtractor
    
    extractor = MarketDataExtractor()
    data = extractor.extract_all_current_data()
"""

from .market_data_extractor import MarketDataExtractor

# Export the main classes and functions that other modules should use
__all__ = [
    'MarketDataExtractor',
]

# Module version
__version__ = '1.0.0'
