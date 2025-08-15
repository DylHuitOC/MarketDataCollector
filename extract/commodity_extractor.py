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
        # Use 5min data and aggregate manually to custom 15min intervals
        self.api_url = 'https://financialmodelingprep.com/stable/historical-chart/5min?symbol={}&from={}&to={}&apikey={}'

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
                        filtered_5min = self._filter_market_hours(data)
                        aggregated = self._aggregate_custom_15min(filtered_5min)
                        if aggregated:
                            commodity_data[symbol] = aggregated
                            self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated)} aggregated 15min records")
                        else:
                            self.logger.info(f"{symbol}: No aggregatable 15min records in market hours")
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
                    
                    # Use ASCII arrow to avoid Windows console Unicode issues
                    self.logger.info(f" {symbol}: {current_start.date()} -> {current_end.date()}")
                    
                    response = requests.get(url)
                    if response.status_code == 200:
                        data = response.json()
                        if data:
                            filtered_5min = self._filter_market_hours(data)
                            all_data.extend(filtered_5min)
                            self.logger.info(f"[SUCCESS] {symbol}: {len(filtered_5min)} 5min records in market hours")
                    
                    time.sleep(1)
                    current_start = current_end
                
                if all_data:
                    aggregated = self._aggregate_custom_15min(all_data)
                    if aggregated:
                        commodity_data[symbol] = aggregated
                        self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated)} total aggregated 15min records")
                
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
                self.logger.warning(f"Error parsing date {entry.get('date', 'unknown')}: {e}")
        
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

    def _aggregate_custom_15min(self, data_5min):
        """Aggregate 5-min commodity bars to custom 15-min intervals:
        - Keep single 9:30 bar (if present) as its own interval
        - Then groups of 3 consecutive 5-min bars (e.g. 9:35,9:40,9:45) where last minute %15 == 0
          open = first bar open, close = last bar close, high = max highs, low = min lows, volume=sum, datetime=last bar time
        Adds last_5min_datetime field matching the last 5-min bar timestamp.
        """
        if not data_5min:
            return []
        # Filter out malformed records
        cleaned = [r for r in data_5min if isinstance(r, dict) and r.get('date')]
        if not cleaned:
            return []
        df = pd.DataFrame(cleaned).copy()
        try:
            df['date'] = pd.to_datetime(df['date'])
        except Exception as e:
            self.logger.error(f"Failed to parse dates for aggregation: {e}")
            return []
        df.sort_values('date', inplace=True)
        df.set_index('date', inplace=True)
        aggregated = []
        for session_date, day_df in df.groupby(df.index.date):
            day_df = day_df.sort_index()
            times = list(day_df.index)
            i = 0
            # Opening 9:30 bar
            if any(ts.hour == 9 and ts.minute == 30 for ts in times):
                ts_930 = [ts for ts in times if ts.hour == 9 and ts.minute == 30][0]
                first_row = day_df.loc[ts_930]
                if isinstance(first_row, pd.DataFrame):
                    first_row = first_row.iloc[0]
                aggregated.append({
                    'date': ts_930.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': first_row['open'],
                    'high': first_row['high'],
                    'low': first_row['low'],
                    'close': first_row['close'],
                    'volume': first_row['volume'],
                    'last_5min_datetime': ts_930.strftime('%Y-%m-%d %H:%M:%S')
                })
                while i < len(times) and times[i] != ts_930:
                    i += 1
                i += 1
            # Triples thereafter
            from datetime import timedelta as _td
            while i + 2 < len(times):
                g = times[i:i+3]
                last_ts = g[-1]
                if (g[1] - g[0] == _td(minutes=5) and
                    g[2] - g[1] == _td(minutes=5) and
                    last_ts.minute % 15 == 0):
                    slice_df = day_df.loc[g]
                    aggregated.append({
                        'date': last_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        'open': slice_df.iloc[0]['open'],
                        'high': slice_df['high'].max(),
                        'low': slice_df['low'].min(),
                        'close': slice_df.iloc[-1]['close'],
                        'volume': slice_df['volume'].sum(),
                        'last_5min_datetime': last_ts.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    i += 3
                else:
                    i += 1
        aggregated.sort(key=lambda r: r['date'])
        return aggregated
