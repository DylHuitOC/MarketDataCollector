"""
Load Module - Data Warehouse Loading Operations

This module handles loading extracted market data into the MySQL data warehouse.
It provides robust loading capabilities with staging tables, batch processing,
error handling, and transaction management.

Main Components:
- DataWarehouseLoader: Primary class for loading data into the warehouse
- Staging table management
- Batch processing utilities
- Database connection management

Usage:
    from load import DataWarehouseLoader
    
    loader = DataWarehouseLoader()
    result = loader.load_extracted_data(extracted_data)
"""

from .data_warehouse_loader import DataWarehouseLoader

__all__ = [
    'DataWarehouseLoader',
]

__version__ = '1.0.0'
