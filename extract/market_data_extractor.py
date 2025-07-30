"""
Market Data Extractor - Main Orchestrator
Simple orchestrator for all market data extraction
"""

import os
from datetime import datetime

from config import STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import setup_logging

# Import specialized extractors
from .stock_extractor import StockExtractor
from .index_extractor import IndexExtractor
from .commodity_extractor import CommodityExtractor
from .bond_extractor import BondExtractor

class MarketDataExtractor:
    def __init__(self):
        self.logger = setup_logging('market_data_extractor')
        
        # Initialize specialized extractors
        self.stock_extractor = StockExtractor()
        self.index_extractor = IndexExtractor()
        self.commodity_extractor = CommodityExtractor()
        self.bond_extractor = BondExtractor()
    
    def extract_all_current_data(self):
        """Extract current data from all sources"""
        self.logger.info("Starting current data extraction for all market types")
        
        all_data = {}
        csv_files = {}
        
        # Extract stocks
        try:
            stock_data = self.stock_extractor.extract_current_data()
            if stock_data:
                all_data['stocks'] = stock_data
                csv_path = self.stock_extractor.save_to_csv(stock_data)
                if csv_path:
                    csv_files['stocks'] = csv_path
                self.logger.info(f"[SUCCESS] Extracted data for {len(stock_data)} stocks")
        except Exception as e:
            self.logger.error(f"[ERROR] Stock extraction failed: {e}")
        
        # Extract indexes
        try:
            index_data = self.index_extractor.extract_current_data()
            if index_data:
                all_data['indexes'] = index_data
                csv_path = self.index_extractor.save_to_csv(index_data)
                if csv_path:
                    csv_files['indexes'] = csv_path
                self.logger.info(f"[SUCCESS] Extracted data for {len(index_data)} indexes")
        except Exception as e:
            self.logger.error(f"[ERROR] Index extraction failed: {e}")
        
        # Extract commodities
        try:
            commodity_data = self.commodity_extractor.extract_current_data()
            if commodity_data:
                all_data['commodities'] = commodity_data
                csv_path = self.commodity_extractor.save_to_csv(commodity_data)
                if csv_path:
                    csv_files['commodities'] = csv_path
                self.logger.info(f"[SUCCESS] Extracted data for {len(commodity_data)} commodities")
        except Exception as e:
            self.logger.error(f"[ERROR] Commodity extraction failed: {e}")
        
        # Extract bonds
        try:
            bond_data = self.bond_extractor.extract_current_data()
            if bond_data:
                all_data['bonds'] = bond_data
                csv_path = self.bond_extractor.save_to_csv(bond_data)
                if csv_path:
                    csv_files['bonds'] = csv_path
                self.logger.info(f"[SUCCESS] Extracted bond data")
        except Exception as e:
            self.logger.error(f"[ERROR] Bond extraction failed: {e}")
        
        self.logger.info(f"Extraction complete. Total data types: {len(all_data)}")
        
        # Return data in format expected by existing scripts
        return {
            'stocks': all_data.get('stocks', {}),
            'indexes': all_data.get('indexes', {}),
            'commodities': all_data.get('commodities', {}),
            'bonds': all_data.get('bonds', {}),
            'csv_files': csv_files,
            'extraction_time': datetime.now()
        }
    
    def extract_historical_data(self, start_date, end_date):
        """Extract historical data from all sources"""
        self.logger.info(f"Starting historical data extraction from {start_date.date()} to {end_date.date()}")
        
        all_data = {}
        
        # Extract stocks
        try:
            stock_data = self.stock_extractor.extract_historical_data(STOCK_SYMBOLS, start_date, end_date)
            if stock_data:
                all_data['stocks'] = stock_data
                self.logger.info(f"[SUCCESS] Extracted historical data for {len(stock_data)} stocks")
        except Exception as e:
            self.logger.error(f"[ERROR] Historical stock extraction failed: {e}")
        
        # Extract indexes
        try:
            index_data = self.index_extractor.extract_historical_data(INDEX_SYMBOLS, start_date, end_date)
            if index_data:
                all_data['indexes'] = index_data
                self.logger.info(f"[SUCCESS] Extracted historical data for {len(index_data)} indexes")
        except Exception as e:
            self.logger.error(f"[ERROR] Historical index extraction failed: {e}")
        
        # Extract commodities
        try:
            commodity_data = self.commodity_extractor.extract_historical_data(COMMODITY_SYMBOLS, start_date, end_date)
            if commodity_data:
                all_data['commodities'] = commodity_data
                self.logger.info(f"[SUCCESS] Extracted historical data for {len(commodity_data)} commodities")
        except Exception as e:
            self.logger.error(f"[ERROR] Historical commodity extraction failed: {e}")
        
        # Extract bonds
        try:
            bond_data = self.bond_extractor.extract_historical_data(None, start_date, end_date)
            if bond_data:
                all_data['bonds'] = bond_data
                self.logger.info(f"[SUCCESS] Extracted historical bond data")
        except Exception as e:
            self.logger.error(f"[ERROR] Historical bond extraction failed: {e}")
        
        self.logger.info(f"Historical extraction complete. Total data types: {len(all_data)}")
        return all_data
    
    def save_all_to_csv(self, all_data, timestamp=None):
        """Save all extracted data to CSV files"""
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        csv_paths = {}
        
        for data_type, data in all_data.items():
            try:
                if data_type == 'stocks':
                    path = self.stock_extractor.save_to_csv(data, f'stocks_{timestamp}.csv')
                elif data_type == 'indexes':
                    path = self.index_extractor.save_to_csv(data, f'indexes_{timestamp}.csv')
                elif data_type == 'commodities':
                    path = self.commodity_extractor.save_to_csv(data, f'commodities_{timestamp}.csv')
                elif data_type == 'bonds':
                    path = self.bond_extractor.save_to_csv(data, f'bonds_{timestamp}.csv')
                else:
                    continue
                
                if path:
                    csv_paths[data_type] = path
                    self.logger.info(f"[SAVED] Saved {data_type} data to {path}")
            
            except Exception as e:
                self.logger.error(f"[ERROR] Failed to save {data_type} data: {e}")
        
        return csv_paths
