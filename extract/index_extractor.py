"""
Index Data Extractor
Simple extractor for market index data from FMP API
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    # Use provided endpoint format for 5min data
        self.api_url = 'https://financialmodelingprep.com/stable/historical-chart/5min?symbol={}&from={}&to={}&apikey={}'
    # Example usage: symbol='^GSPC', apikey='7iSiCJecOuzJYx5xQr61Xd0f8NgNOnsU'
    
    def extract_current_data(self, symbols=None, interval_minutes=15):
        """Extract current index data, aggregated to interval_minutes (default 15min)"""
        if symbols is None:
            symbols = INDEX_SYMBOLS

        index_data = {}

        self.logger.info(f"Extracting data for {len(symbols)} indexes (interval: {interval_minutes}min)")

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
                        # Aggregate 5min to interval_minutes
                        aggregated_data = self._aggregate_5min_to_nmin(data, interval_minutes)
                        index_data[symbol] = aggregated_data
                        self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated_data)} {interval_minutes}min records")
                else:
                    self.logger.warning(f"[ERROR] {symbol}: API error {response.status_code}")

                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching {symbol}: {e}")

        return index_data
    
    def extract_historical_data(self, symbols, start_date, end_date, interval_minutes=15):
        """Extract historical index data, aggregated to interval_minutes (default 15min)"""
        index_data = {}

        self.logger.info(f"Extracting historical data for {len(symbols)} indexes (interval: {interval_minutes}min)")

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
                        try:
                            data = response.json()
                        except ValueError:
                            self.logger.error(f"[ERROR] {symbol}: Non-JSON response for window {current_start} - {current_end}")
                            data = []
                        if isinstance(data, list):
                            if data:
                                # Quick validation of keys
                                bad = [r for r in data if not isinstance(r, dict) or 'date' not in r]
                                if bad:
                                    self.logger.warning(f"[WARN] {symbol}: {len(bad)}/{len(data)} records missing 'date' in window {current_start.date()} -> showing first bad: {bad[0]}")
                                all_data.extend([r for r in data if isinstance(r, dict)])
                            else:
                                self.logger.debug(f"[INFO] {symbol}: Empty list for window {current_start.date()}")
                        else:
                            self.logger.warning(f"[WARN] {symbol}: Unexpected payload type {type(data)} for window {current_start} - {current_end}")

                    time.sleep(1)  # Be nice to API
                    current_start = current_end

                if all_data:
                    # Aggregate all data
                    aggregated_data = self._aggregate_5min_to_nmin(all_data, interval_minutes)
                    index_data[symbol] = aggregated_data
                    self.logger.info(f"[SUCCESS] {symbol}: {len(aggregated_data)} historical records")
                else:
                    self.logger.warning(f"[WARN] {symbol}: No data collected between {start_date} and {end_date}")

            except Exception as e:
                self.logger.error(f"[ERROR] Error fetching historical {symbol}: {e}")

        return index_data
    
    def _aggregate_5min_to_nmin(self, data_5min, interval_minutes=15):
        """Aggregate 5-minute data to n-minute intervals (default 15min)
        Adds 'last_5min_datetime' field for each aggregated record, set to the 'date' of the last 5-min record in the group.
                Filters out records missing 'date'. Uses custom business rule for 15-min: 
                    - Single opening 9:30 bar kept as its own interval
                    - Then groups of 3 consecutive 5-min bars (e.g. 9:35,9:40,9:45) where the last bar minute % 15 == 0
                        open = first bar open, close = last bar close, high = max highs, low = min lows, volume = sum, datetime = last bar time."""
        if not data_5min:
            return []

        # Filter out records missing 'date'
        missing_date = 0
        filtered_data = []
        for rec in data_5min:
            if isinstance(rec, dict) and 'date' in rec and rec['date']:
                filtered_data.append(rec)
            else:
                missing_date += 1

        if missing_date > 0:
            self.logger.debug(f"Filtered out {missing_date} records without 'date' field")

        if not filtered_data:
            return []

        df = pd.DataFrame(filtered_data).copy()
        # Keep original datetime as separate index
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values('date', inplace=True)
        df.set_index('date', inplace=True)

        # Custom 15-min logic only applies when interval_minutes == 15
        if interval_minutes != 15:
            grouped = df.groupby(pd.Grouper(freq=f'{interval_minutes}min'))
            aggregated = []
            for name, group in grouped:
                if group.empty:
                    continue
                last_5min_dt = group.index[-1].strftime('%Y-%m-%d %H:%M:%S')
                aggregated.append({
                    'date': name.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': group.iloc[0]['open'],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'close': group.iloc[-1]['close'],
                    'volume': group['volume'].sum(),
                    'last_5min_datetime': last_5min_dt
                })
            aggregated.sort(key=lambda r: r['date'])
            return aggregated

        aggregated = []
        # Process day by day
        for session_date, day_df in df.groupby(df.index.date):
            day_df = day_df.sort_index()
            times = list(day_df.index)
            i = 0
            # Handle single 9:30 bar if present
            if any(ts.hour == 9 and ts.minute == 30 for ts in times):
                first_bar = day_df.loc[[ts for ts in times if ts.hour == 9 and ts.minute == 30][0]]
                if isinstance(first_bar, pd.DataFrame):
                    # In rare case of duplicate timestamps; take first row
                    first_row = first_bar.iloc[0]
                else:
                    first_row = first_bar
                aggregated.append({
                    'date': first_row.name.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': first_row['open'],
                    'high': first_row['high'],
                    'low': first_row['low'],
                    'close': first_row['close'],
                    'volume': first_row['volume'],
                    'last_5min_datetime': first_row.name.strftime('%Y-%m-%d %H:%M:%S')
                })
                # Advance index past 9:30 bar
                while i < len(times) and not (times[i].hour == 9 and times[i].minute == 30):
                    i += 1
                i += 1  # move past 9:30
            else:
                i = 0
            # Remaining groups of 3 bars ending on quarter-hour marks (:00,:15,:30,:45)
            while i + 2 < len(times):
                group_times = times[i:i+3]
                last_ts = group_times[-1]
                # Condition: bars consecutive 5-min intervals and last minute %15 == 0
                if (group_times[1] - group_times[0] == timedelta(minutes=5) and
                    group_times[2] - group_times[1] == timedelta(minutes=5) and
                    last_ts.minute % 15 == 0):
                    slice_df = day_df.loc[group_times]
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
                    # If pattern not satisfied, advance one to realign
                    i += 1
        # Ensure chronological order
        aggregated.sort(key=lambda r: r['date'])
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
    
    def save_raw_to_csv(self, raw_index_data, filename=None):
        """Save raw 5-min index data to CSV"""
        if not raw_index_data:
            return None
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'indexes_raw_{timestamp}.csv'
        csv_path = os.path.join(self.csv_dir, filename)
        all_records = []
        for symbol, records in raw_index_data.items():
            for record in records:
                record['symbol'] = symbol
                all_records.append(record)
        df = pd.DataFrame(all_records)
        df.to_csv(csv_path, index=False)
        self.logger.info(f"[SAVED] Saved {len(all_records)} raw records to {csv_path}")
        return csv_path
