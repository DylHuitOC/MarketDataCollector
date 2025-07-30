"""
Index Data Extractor
Simple extractor for market index data from FMP API
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

from config import API_KEY, INDEX_SYMBOLS
from utils import setup_logging

class IndexExtractor:
    def __init__(self):
        self.logger = setup_logging('index_extractor')
        self.csv_dir = 'data_extracts/indexes'
        os.makedirs(self.csv_dir, exist_ok=True)
        
        # Use 5min data and aggregate to 15min
        self.api_url = 'https://financialmodelingprep.com/stable/historical-chart/5min?symbol={}&from={}&to={}&apikey={}'
    
    def extract_current_data(self, symbols=None):
        """Extract current index data"""
        if symbols is None:
            symbols = INDEX_SYMBOLS
        
        index_data = {}
        
        self.logger.info(f"Extracting data for {len(symbols)} indexes")
        
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
                        # Aggregate 5min to 15min
                        aggregated_data = self._aggregate_5min_to_15min(data)
                        index_data[symbol] = aggregated_data
                        self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated_data)} 15min records")
                else:
                    self.logger.warning(f"[ERROR] {symbol}: API error {response.status_code}")
                
                time.sleep(0.1)  # Rate limiting
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching {symbol}: {e}")
        
        return index_data
    
    def extract_historical_data(self, symbols, start_date, end_date):
        """Extract historical index data"""
        index_data = {}
        
        self.logger.info(f"Extracting historical data for {len(symbols)} indexes")
        
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
                    
                    response = requests.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            all_data.extend(data)
                    
                    time.sleep(1)  # Be nice to API
                    current_start = current_end
                
                if all_data:
                    # Aggregate all data
                    aggregated_data = self._aggregate_5min_to_15min(all_data)
                    index_data[symbol] = aggregated_data
                    self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated_data)} historical records")
                
            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching historical {symbol}: {e}")
        
        return index_data
    
    def _aggregate_5min_to_15min(self, data_5min):
        """Aggregate 5-minute data to 15-minute intervals"""
        if not data_5min:
            return []
        
        df = pd.DataFrame(data_5min)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        
        # Group by 15-minute intervals
        df.set_index('date', inplace=True)
        grouped = df.groupby(pd.Grouper(freq='15T'))
        
        aggregated = []
        for name, group in grouped:
            if not group.empty:
                record = {
                    'date': name.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': group.iloc[0]['open'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'close': group.iloc[-1]['close'],
                    'volume': group['volume'].sum()
                }
                aggregated.append(record)
        
        return aggregated
    
    def save_to_csv(self, index_data, filename=None):
        """Save index data to CSV"""
        if not index_data:
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'indexes_{timestamp}.csv'
        
        csv_path = os.path.join(self.csv_dir, filename)
        
        # Flatten data
        all_records = []
        for symbol, records in index_data.items():
            for record in records:
                record['symbol'] = symbol
                all_records.append(record)
        
        df = pd.DataFrame(all_records)
        df.to_csv(csv_path, index=False)
        
        self.logger.info(f"[SAVED] Saved {len(all_records)} records to {csv_path}")
        return csv_path
