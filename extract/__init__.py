"""
Extract Module - Market Data Extraction from FMP API

This module handles all data extraction operations from the Financial Modeling Prep API.
It provides simple, focused extractors for different market data types.

Main Components:
- MarketDataExtractor: Primary orchestrator for all data extraction
- StockExtractor: Simple extractor for stock market data (15-minute OHLCV)
- IndexExtractor: Simple extractor for market indexes (5min->15min aggregation)
- CommodityExtractor: Simple extractor for commodity data
- BondExtractor: Simple extractor for treasury bond rates

Architecture:
Uses composition pattern with specialized extractors for better separation of concerns,
easier testing, and maintainability. Each extractor is simple and focused.

Usage:
    from extract import MarketDataExtractor
    
    extractor = MarketDataExtractor()
    data = extractor.extract_all_current_data()
"""

from .market_data_extractor import MarketDataExtractor
from .stock_extractor import StockExtractor
from .index_extractor import IndexExtractor
from .commodity_extractor import CommodityExtractor
from .bond_extractor import BondExtractor

# Export the main classes and functions that other modules should use
__all__ = [
    'MarketDataExtractor',
    'StockExtractor', 
    'IndexExtractor',
    'CommodityExtractor',
    'BondExtractor'
]

# Module version
__version__ = '2.0.0'
