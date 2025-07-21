"""
Transform Module - Analytics and Data Transformation

This module handles data transformations, technical analysis calculations,
and analytics generation for the market data warehouse.

Main Components:
- AnalyticsTransformer: Primary class for data transformations
- Technical indicator calculations (SMA, EMA, MACD, RSI, Bollinger Bands)
- Market aggregation and performance metrics
- Daily/weekly/monthly rollup operations

Usage:
    from transform import AnalyticsTransformer
    
    transformer = AnalyticsTransformer()
    transformer.run_incremental_transforms()
"""

from .analytics_transformer import AnalyticsTransformer

__all__ = [
    'AnalyticsTransformer',
]

__version__ = '1.0.0'
