"""
Bond Data Extractor
Simple extractor for treasury bond rates from FMP API
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

from config import API_KEY
from utils import setup_logging

class BondExtractor:
    def __init__(self):
        self.logger = setup_logging('bond_extractor')
        self.csv_dir = 'data_extracts/bonds'
        os.makedirs(self.csv_dir, exist_ok=True)
        
        self.api_url = 'https://financialmodelingprep.com/stable/treasury?from={}&to={}&apikey={}'
    
    def extract_current_data(self, symbols=None):
        """Extract current treasury rates"""
        bond_data = {}
        
        self.logger.info("Extracting current treasury rates")
        
        try:
            # Get last 7 days of data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            url = self.api_url.format(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                API_KEY
            )
            
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    bond_data['treasury_rates'] = data
                    self.logger.info(f"[SUCCESS] Treasury rates: {len(data)} records")
            else:
                self.logger.warning(f"[ERROR] Treasury rates: API error {response.status_code}")
            
        except Exception as e:
            self.logger.error(f"[ERROR] Error fetching treasury rates: {e}")
        
        return bond_data
    
    def extract_historical_data(self, symbols, start_date, end_date):
        """Extract historical treasury rates"""
        bond_data = {}
        
        self.logger.info(f"Extracting historical treasury rates from {start_date.date()} to {end_date.date()}")
        
        try:
            url = self.api_url.format(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                API_KEY
            )
            
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    bond_data['treasury_rates'] = data
                    self.logger.info(f"[SUCCESS] Historical treasury rates: {len(data)} records")
        
        except Exception as e:
            self.logger.error(f"[ERROR] Error fetching historical treasury rates: {e}")
        
        return bond_data
    
    def save_to_csv(self, bond_data, filename=None):
        """Save bond data to CSV"""
        if not bond_data:
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'bonds_{timestamp}.csv'
        
        csv_path = os.path.join(self.csv_dir, filename)
        
        # Flatten data
        all_records = []
        for key, records in bond_data.items():
            for record in records:
                all_records.append(record)
        
        df = pd.DataFrame(all_records)
        df.to_csv(csv_path, index=False)
        
        self.logger.info(f"[SAVED] Saved {len(all_records)} records to {csv_path}")
        return csv_path
