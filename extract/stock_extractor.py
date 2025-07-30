

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

from config import API_KEY, STOCK_SYMBOLS
from utils import setup_logging

class StockExtractor:
    def __init__(self):
        self.logger = setup_logging('stock_extractor')
        self.csv_dir = 'data_extracts/stocks'
        os.makedirs(self.csv_dir, exist_ok=True)
        
        self.api_url = 'https://financialmodelingprep.com/stable/historical-chart/15min?symbol={}&from={}&to={}&apikey={}'
    
    def extract_current_data(self, symbols=None):
        """Extract current 15-minute stock data"""
        if symbols is None:
            symbols = STOCK_SYMBOLS
        
        stock_data = {}
        
        self.logger.info(f"Extracting data for {len(symbols)} stocks")
        
        for symbol in symbols:
            try:
                # Get last 2 hours of data
                end_date = datetime.now()
                start_date = end_date - timedelta(hours=2)
                
                url = self.api_url.format(
                    symbol, 
                    start_date.strftime('%Y-%m-%d %H:%M:%S'), 
                    end_date.strftime('%Y-%m-%d %H:%M:%S'), 
                    API_KEY
                )
                
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        stock_data[symbol] = data
                        self.logger.info(f"[SUCCESS] {symbol}: {len(data)} records")
                else:
                    self.logger.warning(f"[ERROR] {symbol}: API error {response.status_code}")
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching {symbol}: {e}")
        
        return stock_data
    
    def extract_historical_data(self, symbols, start_date, end_date):
        """Extract historical stock data"""
        stock_data = {}
        
        self.logger.info(f"Extracting historical data for {len(symbols)} stocks")
        
        for symbol in symbols:
            try:
                current_start = start_date
                symbol_data = []
                
                while current_start < end_date:
                    current_end = current_start + timedelta(days=10)
                    
                    url = self.api_url.format(
                        symbol,
                        current_start.strftime('%Y-%m-%d'),
                        current_end.strftime('%Y-%m-%d'),
                        API_KEY
                    )
                    
                    response = requests.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            symbol_data.extend(data)
                    
                    time.sleep(1)  # Be nice to API
                    current_start = current_end
                
                if symbol_data:
                    stock_data[symbol] = symbol_data
                    self.logger.info(f"[SUCCESS] {symbol}: {len(symbol_data)} historical records")
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching historical {symbol}: {e}")
        
        return stock_data
    
    def save_to_csv(self, stock_data, filename=None):
        """Save stock data to CSV"""
        if not stock_data:
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'stocks_{timestamp}.csv'
        
        csv_path = os.path.join(self.csv_dir, filename)
        
        # Flatten data
        all_records = []
        for symbol, records in stock_data.items():
            for record in records:
                record['symbol'] = symbol
                all_records.append(record)
        
        df = pd.DataFrame(all_records)
        df.to_csv(csv_path, index=False)
        
        self.logger.info(f"[SAVED] Saved {len(all_records)} records to {csv_path}")
        return csv_path
