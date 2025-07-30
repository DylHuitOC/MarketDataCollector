"""
Commodity Data Extractor
Simple extractor for commodity data from FMP API with market hours filtering
"""

import requests
import pandas as pd
from datetime import datetime, timedelta, time as dtime
import time
import os

from config import API_KEY, COMMODITY_SYMBOLS
from utils import setup_logging

class CommodityExtractor:
    def __init__(self):
        self.logger = setup_logging('commodity_extractor')
        self.csv_dir = 'data_extracts/commodities'
        os.makedirs(self.csv_dir, exist_ok=True)
        
        self.api_url = 'https://financialmodelingprep.com/stable/historical-chart/15min?symbol={}&from={}&to={}&apikey={}'
        
        # Commodity market hours (typically 9:30 AM - 3:45 PM ET)
        self.market_open = dtime(9, 30)
        self.market_close = dtime(15, 45)
    
    def extract_current_data(self, symbols=None):
        """Extract current commodity data with market hours filtering"""
        if symbols is None:
            symbols = COMMODITY_SYMBOLS
        
        commodity_data = {}
        
        self.logger.info(f"Extracting data for {len(symbols)} commodities")
        
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
                        # Filter by market hours
                        filtered_data = self._filter_market_hours(data)
                        if filtered_data:
                            commodity_data[symbol] = filtered_data
                            self.logger.info(f"[SUCCESS] {symbol}: {len(filtered_data)} records (market hours)")
                        else:
                            self.logger.info(f"⏰ {symbol}: No data during market hours")
                else:
                    self.logger.warning(f"[ERROR] {symbol}: API error {response.status_code}")
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching {symbol}: {e}")
        
        return commodity_data
    
    def extract_historical_data(self, symbols, start_date, end_date):
        """Extract historical commodity data with market hours filtering"""
        commodity_data = {}
        
        self.logger.info(f"Extracting historical data for {len(symbols)} commodities")
        
        for symbol in symbols:
            try:
                current_start = start_date
                all_data = []
                
                while current_start < end_date:
                    current_end = current_start + timedelta(days=10)
                    
                    url = self.api_url.format(
                        symbol,
                        current_start.strftime('%Y-%m-%d'),
                        current_end.strftime('%Y-%m-%d'),
                        API_KEY
                    )
                    
                    self.logger.info(f"📊 {symbol}: {current_start.date()} → {current_end.date()}")
                    
                    response = requests.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            # Filter by market hours
                            filtered_data = self._filter_market_hours(data)
                            all_data.extend(filtered_data)
                            self.logger.info(f"[SUCCESS] {symbol}: {len(filtered_data)} records in market hours")
                    
                    time.sleep(1)  # Be nice to API
                    current_start = current_end
                
                if all_data:
                    commodity_data[symbol] = all_data
                    self.logger.info(f"[SUCCESS] {symbol}: {len(all_data)} total historical records")
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching historical {symbol}: {e}")
        
        return commodity_data
    
    def _filter_market_hours(self, data):
        """Filter data to only include records during commodity market hours"""
        filtered_data = []
        
        for entry in data:
            try:
                dt = datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S')
                
                # Check if time is within market hours
                if self.market_open <= dt.time() <= self.market_close:
                    filtered_data.append(entry)
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Error parsing date {entry.get('date', 'unknown')}: {e}")
        
        return filtered_data
    
    def save_to_csv(self, commodity_data, filename=None):
        """Save commodity data to CSV"""
        if not commodity_data:
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'commodities_{timestamp}.csv'
        
        csv_path = os.path.join(self.csv_dir, filename)
        
        # Flatten data
        all_records = []
        for symbol, records in commodity_data.items():
            for record in records:
                record['symbol'] = symbol
                all_records.append(record)
        
        df = pd.DataFrame(all_records)
        df.to_csv(csv_path, index=False)
        
        self.logger.info(f"[SAVED] Saved {len(all_records)} records to {csv_path}")
        return csv_path
