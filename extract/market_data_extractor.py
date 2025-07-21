"""
Market Data Extractor
Handles extraction of real-time and historical market data from FMP API
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import aiohttp
import time
from typing import Dict, List, Optional, Any
import json

from config import (API_KEY, STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS, 
                   API_ENDPOINTS, ELT_CONFIG, FROM_DATE, TO_DATE)
from utils import setup_logging, safe_float, safe_int, batch_process, validate_symbol

class MarketDataExtractor:
    def __init__(self):
        self.logger = setup_logging('market_data_extractor')
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MarketDataCollector/1.0'})
        
    def extract_all_current_data(self) -> Dict[str, Any]:
        """Extract current market data for all symbols"""
        extracted_data = {
            'stocks': {},
            'indexes': {},
            'commodities': {},
            'bonds': {},
            'extraction_time': datetime.now(),
            'metadata': {}
        }
        
        try:
            # Extract stock data
            self.logger.info(f"Extracting data for {len(STOCK_SYMBOLS)} stocks")
            for symbol in STOCK_SYMBOLS:
                if validate_symbol(symbol):
                    data = self._extract_symbol_data(symbol, '15min')
                    if data:
                        extracted_data['stocks'][symbol] = data
                    time.sleep(0.1)  # Rate limiting
                        
            # Extract index data (use 5min and aggregate to 15min)
            self.logger.info(f"Extracting data for {len(INDEX_SYMBOLS)} indexes")
            for symbol in INDEX_SYMBOLS:
                if validate_symbol(symbol):
                    data = self._extract_symbol_data(symbol, '5min')
                    if data:
                        # Aggregate 5min to 15min
                        aggregated_data = self._aggregate_5min_to_15min(data)
                        extracted_data['indexes'][symbol] = aggregated_data
                    time.sleep(0.1)
                        
            # Extract commodity data
            self.logger.info(f"Extracting data for {len(COMMODITY_SYMBOLS)} commodities")
            for symbol in COMMODITY_SYMBOLS:
                if validate_symbol(symbol):
                    data = self._extract_symbol_data(symbol, '15min')
                    if data:
                        extracted_data['commodities'][symbol] = data
                    time.sleep(0.1)
                        
            # Extract bond data
            bond_data = self._extract_treasury_rates()
            if bond_data:
                extracted_data['bonds'] = bond_data
                
            # Add metadata
            extracted_data['metadata'] = self._generate_extraction_metadata(extracted_data)
            
        except Exception as e:
            self.logger.error(f"Error in extract_all_current_data: {str(e)}")
            
        return extracted_data
    
    def extract_historical_data(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Extract historical data for backfill operations"""
        self.logger.info(f"Extracting historical data from {start_date} to {end_date}")
        
        extracted_data = {
            'stocks': {},
            'indexes': {},
            'commodities': {},
            'extraction_time': datetime.now(),
            'is_historical': True,
            'date_range': {'start': start_date, 'end': end_date}
        }
        
        # Process in batches to avoid API limits
        all_symbols = STOCK_SYMBOLS + INDEX_SYMBOLS + COMMODITY_SYMBOLS
        
        for symbol_batch in batch_process(all_symbols, 10):
            for symbol in symbol_batch:
                try:
                    if symbol in STOCK_SYMBOLS:
                        data = self._extract_historical_symbol_data(symbol, start_date, end_date, '15min')
                        if data:
                            extracted_data['stocks'][symbol] = data
                            
                    elif symbol in INDEX_SYMBOLS:
                        data = self._extract_historical_symbol_data(symbol, start_date, end_date, '5min')
                        if data:
                            aggregated_data = self._aggregate_5min_to_15min(data)
                            extracted_data['indexes'][symbol] = aggregated_data
                            
                    elif symbol in COMMODITY_SYMBOLS:
                        data = self._extract_historical_symbol_data(symbol, start_date, end_date, '15min')
                        if data:
                            extracted_data['commodities'][symbol] = data
                            
                    time.sleep(0.2)  # More conservative rate limiting for historical data
                    
                except Exception as e:
                    self.logger.error(f"Error extracting historical data for {symbol}: {str(e)}")
                    
            # Pause between batches
            time.sleep(1)
            
        return extracted_data
    
    def _extract_symbol_data(self, symbol: str, interval: str) -> Optional[List[Dict]]:
        """Extract data for a single symbol"""
        try:
            # Get recent data (last 2 hours to ensure we have latest 15min candle)
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=2)
            
            url = f"{API_ENDPOINTS[f'historical_{interval}']}?symbol={symbol}&from={start_date.strftime('%Y-%m-%d %H:%M:%S')}&to={end_date.strftime('%Y-%m-%d %H:%M:%S')}&apikey={API_KEY}"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or not isinstance(data, list):
                self.logger.warning(f"No data returned for {symbol}")
                return None
                
            # Validate and clean data
            cleaned_data = []
            for record in data:
                if self._validate_record(record):
                    cleaned_record = self._clean_record(record)
                    cleaned_data.append(cleaned_record)
                    
            return cleaned_data if cleaned_data else None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed for {symbol}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error extracting {symbol}: {str(e)}")
            return None
    
    def _extract_historical_symbol_data(self, symbol: str, start_date: datetime, 
                                      end_date: datetime, interval: str) -> Optional[List[Dict]]:
        """Extract historical data for a symbol over a date range"""
        try:
            url = f"{API_ENDPOINTS[f'historical_{interval}']}?symbol={symbol}&from={start_date.strftime('%Y-%m-%d')}&to={end_date.strftime('%Y-%m-%d')}&apikey={API_KEY}"
            
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or not isinstance(data, list):
                return None
                
            # Validate and clean data
            cleaned_data = []
            for record in data:
                if self._validate_record(record):
                    cleaned_record = self._clean_record(record)
                    cleaned_data.append(cleaned_record)
                    
            return cleaned_data if cleaned_data else None
            
        except Exception as e:
            self.logger.error(f"Error extracting historical data for {symbol}: {str(e)}")
            return None
    
    def _extract_treasury_rates(self) -> Optional[Dict]:
        """Extract current treasury rates"""
        try:
            url = f"{API_ENDPOINTS['treasury_rates']}?apikey={API_KEY}"
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data and isinstance(data, list) and len(data) > 0:
                # Get the most recent record
                latest_record = data[0]
                return {
                    'date': latest_record.get('date'),
                    'month1': safe_float(latest_record.get('month1')),
                    'month2': safe_float(latest_record.get('month2')),
                    'month3': safe_float(latest_record.get('month3')),
                    'month6': safe_float(latest_record.get('month6')),
                    'year1': safe_float(latest_record.get('year1')),
                    'year2': safe_float(latest_record.get('year2')),
                    'year3': safe_float(latest_record.get('year3')),
                    'year5': safe_float(latest_record.get('year5')),
                    'year7': safe_float(latest_record.get('year7')),
                    'year10': safe_float(latest_record.get('year10')),
                    'year20': safe_float(latest_record.get('year20')),
                    'year30': safe_float(latest_record.get('year30'))
                }
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting treasury rates: {str(e)}")
            return None
    
    def _aggregate_5min_to_15min(self, data: List[Dict]) -> List[Dict]:
        """Aggregate 5-minute data to 15-minute intervals"""
        if not data:
            return []
            
        # Sort by date
        sorted_data = sorted(data, key=lambda x: x['date'])
        
        aggregated = []
        i = 0
        
        while i < len(sorted_data):
            # Take 3 consecutive 5-min candles to make one 15-min candle
            group = sorted_data[i:i+3]
            
            if len(group) == 3:  # Only aggregate if we have complete 15-min period
                agg_record = {
                    'date': group[0]['date'],  # Start time of the 15-min period
                    'open': group[0]['open'],
                    'close': group[-1]['close'],
                    'high': max(record['high'] for record in group),
                    'low': min(record['low'] for record in group),
                    'volume': sum(record['volume'] for record in group)
                }
                aggregated.append(agg_record)
                
            i += 3
            
        return aggregated
    
    def _validate_record(self, record: Dict) -> bool:
        """Validate a data record"""
        required_fields = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        for field in required_fields:
            if field not in record:
                return False
            if record[field] is None:
                return False
                
        # Validate price data
        try:
            open_price = float(record['open'])
            high_price = float(record['high'])
            low_price = float(record['low'])
            close_price = float(record['close'])
            volume = int(record['volume'])
            
            # Basic sanity checks
            if high_price < max(open_price, close_price, low_price):
                return False
            if low_price > min(open_price, close_price, high_price):
                return False
            if volume < 0:
                return False
                
        except (ValueError, TypeError):
            return False
            
        return True
    
    def _clean_record(self, record: Dict) -> Dict:
        """Clean and normalize a data record"""
        return {
            'date': record['date'],
            'open': round(safe_float(record['open']), 4),
            'high': round(safe_float(record['high']), 4),
            'low': round(safe_float(record['low']), 4),
            'close': round(safe_float(record['close']), 4),
            'volume': safe_int(record['volume'])
        }
    
    def _generate_extraction_metadata(self, extracted_data: Dict) -> Dict:
        """Generate metadata about the extraction"""
        metadata = {
            'total_symbols_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'total_records': 0,
            'data_types': {}
        }
        
        for data_type, symbols_data in extracted_data.items():
            if data_type in ['stocks', 'indexes', 'commodities']:
                symbols_count = len(symbols_data)
                metadata['total_symbols_processed'] += symbols_count
                metadata['successful_extractions'] += symbols_count
                
                # Count total records
                total_records = sum(len(data) if isinstance(data, list) else 1 
                                  for data in symbols_data.values())
                metadata['total_records'] += total_records
                metadata['data_types'][data_type] = {
                    'symbols_count': symbols_count,
                    'records_count': total_records
                }
                
        return metadata
    
    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, 'session'):
            self.session.close()
