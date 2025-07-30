"""
CSV Data Warehouse Loader
Loads CSV files extracted from FMP API into the raw data warehouse
"""

import pandas as pd
import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os
import glob
from contextlib import contextmanager

from config import ELT_CONFIG, STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import get_db_connection, setup_logging, batch_process, safe_float, safe_int

class CSVDataWarehouseLoader:
    def __init__(self):
        self.logger = setup_logging('csv_data_warehouse_loader')
        
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = get_db_connection()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def load_csv_files(self, csv_files: Dict[str, str]) -> Dict[str, Any]:
        """Load data from CSV files into raw data warehouse"""
        load_results = {
            'stocks': {'records_loaded': 0, 'errors': []},
            'indexes': {'records_loaded': 0, 'errors': []},
            'commodities': {'records_loaded': 0, 'errors': []},
            'bonds': {'records_loaded': 0, 'errors': []},
            'load_time': datetime.now(),
            'source': 'csv_files'
        }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Load stock data from CSV
                if 'stocks' in csv_files and os.path.exists(csv_files['stocks']):
                    self.logger.info(f"Loading stock data from {csv_files['stocks']}")
                    stock_result = self._load_csv_to_table(
                        cursor, csv_files['stocks'], 'stock_data_raw'
                    )
                    load_results['stocks'] = stock_result
                    
                # Load index data from CSV
                if 'indexes' in csv_files and os.path.exists(csv_files['indexes']):
                    self.logger.info(f"Loading index data from {csv_files['indexes']}")
                    index_result = self._load_csv_to_table(
                        cursor, csv_files['indexes'], 'index_data_raw'
                    )
                    load_results['indexes'] = index_result
                    
                # Load commodity data from CSV
                if 'commodities' in csv_files and os.path.exists(csv_files['commodities']):
                    self.logger.info(f"Loading commodity data from {csv_files['commodities']}")
                    commodity_result = self._load_csv_to_table(
                        cursor, csv_files['commodities'], 'commodity_data_raw'
                    )
                    load_results['commodities'] = commodity_result
                    
                # Load bond data from CSV
                if 'bonds' in csv_files and os.path.exists(csv_files['bonds']):
                    self.logger.info(f"Loading bond data from {csv_files['bonds']}")
                    bond_result = self._load_csv_to_table(
                        cursor, csv_files['bonds'], 'bond_data_raw'
                    )
                    load_results['bonds'] = bond_result
                    
                # Commit all changes
                conn.commit()
                self.logger.info("All CSV data loaded successfully into raw data warehouse")
                
        except Exception as e:
            self.logger.error(f"Error loading CSV data: {str(e)}")
            load_results['error'] = str(e)
            
        return load_results
    
    def load_all_csv_files_in_directory(self, base_directory: str = 'data_extracts') -> Dict[str, Any]:
        """Load all CSV files from the data extracts directory"""
        load_results = {
            'total_files_processed': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'details': {}
        }
        
        try:
            # Find all CSV files in subdirectories
            csv_patterns = {
                'stocks': f'{base_directory}/stocks/*.csv',
                'indexes': f'{base_directory}/indexes/*.csv',
                'commodities': f'{base_directory}/commodities/*.csv',
                'bonds': f'{base_directory}/bonds/*.csv'
            }
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for data_type, pattern in csv_patterns.items():
                    csv_files = glob.glob(pattern)
                    self.logger.info(f"Found {len(csv_files)} {data_type} CSV files")
                    
                    type_results = {'files_processed': 0, 'records_loaded': 0, 'errors': []}
                    
                    for csv_file in sorted(csv_files):  # Process in chronological order
                        try:
                            table_name = f'{data_type.rstrip("s")}_data_raw'  # Remove 's' and add '_raw'
                            if data_type == 'commodities':
                                table_name = 'commodity_data_raw'
                            
                            result = self._load_csv_to_table(cursor, csv_file, table_name)
                            type_results['files_processed'] += 1
                            type_results['records_loaded'] += result.get('records_loaded', 0)
                            load_results['total_files_processed'] += 1
                            
                            # Move processed file to archive
                            self._archive_csv_file(csv_file, base_directory)
                            
                        except Exception as e:
                            error_msg = f"Error processing {csv_file}: {str(e)}"
                            self.logger.error(error_msg)
                            type_results['errors'].append(error_msg)
                            load_results['failed_loads'] += 1
                    
                    load_results['details'][data_type] = type_results
                    if type_results['files_processed'] > 0:
                        load_results['successful_loads'] += 1
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error in bulk CSV loading: {str(e)}")
            load_results['error'] = str(e)
            
        return load_results
    
    def _load_csv_to_table(self, cursor, csv_file_path: str, table_name: str) -> Dict[str, Any]:
        """Load a single CSV file into a database table"""
        result = {'records_loaded': 0, 'errors': [], 'duplicates_skipped': 0}
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            
            if df.empty:
                self.logger.warning(f"CSV file {csv_file_path} is empty")
                return result
            
            # Prepare SQL based on table type
            if 'bond' in table_name:
                sql = self._get_bond_insert_sql(table_name)
            else:
                sql = self._get_ohlcv_insert_sql(table_name)
            
            # Insert records in batches
            batch_size = ELT_CONFIG.get('batch_size', 100)
            for batch_start in range(0, len(df), batch_size):
                batch_df = df.iloc[batch_start:batch_start + batch_size]
                batch_result = self._insert_batch_from_dataframe(cursor, batch_df, sql, table_name)
                result['records_loaded'] += batch_result['inserted']
                result['duplicates_skipped'] += batch_result['duplicates']
                
            self.logger.info(f"Loaded {result['records_loaded']} records from {csv_file_path} into {table_name}")
            
        except Exception as e:
            error_msg = f"Error loading CSV {csv_file_path}: {str(e)}"
            self.logger.error(error_msg)
            result['errors'].append(error_msg)
            
        return result
    
    def _get_ohlcv_insert_sql(self, table_name: str) -> str:
        """Get INSERT SQL for OHLCV data tables"""
        return f"""
            INSERT INTO {table_name} 
            (symbol, datetime, date, open, high, low, close, volume, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), volume=VALUES(volume), loaded_at=NOW()
        """
    
    def _get_bond_insert_sql(self, table_name: str) -> str:
        """Get INSERT SQL for bond data table - updated for FMP treasury API format"""
        return f"""
            INSERT INTO {table_name}
            (datetime, date, rate, yield_1m, yield_3m, yield_6m, yield_1y, yield_2y, 
             yield_5y, yield_10y, yield_20y, yield_30y, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                rate=VALUES(rate), yield_1m=VALUES(yield_1m), yield_3m=VALUES(yield_3m),
                yield_6m=VALUES(yield_6m), yield_1y=VALUES(yield_1y), yield_2y=VALUES(yield_2y),
                yield_5y=VALUES(yield_5y), yield_10y=VALUES(yield_10y), yield_20y=VALUES(yield_20y),
                yield_30y=VALUES(yield_30y), loaded_at=NOW()
        """
    
    def _insert_batch_from_dataframe(self, cursor, df: pd.DataFrame, sql: str, table_name: str) -> Dict[str, int]:
        """Insert a batch of records from DataFrame"""
        result = {'inserted': 0, 'duplicates': 0}
        
        for _, row in df.iterrows():
            try:
                if 'bond' in table_name:
                    # FMP treasury API format
                    record_datetime = pd.to_datetime(row['date']).to_pydatetime().replace(tzinfo=None)
                    record_date = record_datetime.date()
                    
                    values = (
                        record_datetime,
                        record_date,
                        safe_float(row.get('rate')),
                        safe_float(row.get('month1')),
                        safe_float(row.get('month3')),
                        safe_float(row.get('month6')),
                        safe_float(row.get('year1')),
                        safe_float(row.get('year2')),
                        safe_float(row.get('year5')),
                        safe_float(row.get('year10')),
                        safe_float(row.get('year20')),
                        safe_float(row.get('year30'))
                    )
                else:
                    # OHLCV data (stocks, indexes, commodities)
                    values = (
                        row['symbol'],
                        pd.to_datetime(row['date']).to_pydatetime().replace(tzinfo=None),
                        pd.to_datetime(row['date']).date(),
                        round(safe_float(row['open']), 4),
                        round(safe_float(row['high']), 4),
                        round(safe_float(row['low']), 4),
                        round(safe_float(row['close']), 4),
                        safe_int(row['volume'])
                    )
                
                cursor.execute(sql, values)
                result['inserted'] += 1
                
            except Exception as e:
                self.logger.error(f"Error inserting record: {str(e)}")
                
        return result
    
    def _archive_csv_file(self, csv_file_path: str, base_directory: str):
        """Move processed CSV file to archive directory"""
        try:
            archive_dir = f'{base_directory}/archive'
            os.makedirs(archive_dir, exist_ok=True)
            
            filename = os.path.basename(csv_file_path)
            archive_path = os.path.join(archive_dir, f"processed_{datetime.now().strftime('%Y%m%d')}_{filename}")
            
            os.rename(csv_file_path, archive_path)
            self.logger.info(f"Archived {csv_file_path} to {archive_path}")
            
        except Exception as e:
            self.logger.error(f"Error archiving CSV file {csv_file_path}: {str(e)}")
    
    def create_raw_data_tables(self):
        """Create raw data tables for CSV loading"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Raw OHLCV tables
                raw_tables = {
                    'stock_data_raw': """
                        CREATE TABLE IF NOT EXISTS stock_data_raw (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(10) NOT NULL,
                            datetime DATETIME NOT NULL,
                            date DATE NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_datetime (symbol, datetime),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'index_data_raw': """
                        CREATE TABLE IF NOT EXISTS index_data_raw (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(10) NOT NULL,
                            datetime DATETIME NOT NULL,
                            date DATE NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_datetime (symbol, datetime),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'commodity_data_raw': """
                        CREATE TABLE IF NOT EXISTS commodity_data_raw (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(50) NOT NULL,
                            datetime DATETIME NOT NULL,
                            date DATE NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_date (symbol, date),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'bond_data_raw': """
                        CREATE TABLE IF NOT EXISTS bond_data_raw (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            date DATE NOT NULL UNIQUE,
                            month1 DECIMAL(5, 2),
                            month2 DECIMAL(5, 2),
                            month3 DECIMAL(5, 2),
                            month6 DECIMAL(5, 2),
                            year1 DECIMAL(5, 2),
                            year2 DECIMAL(5, 2),
                            year3 DECIMAL(5, 2),
                            year5 DECIMAL(5, 2),
                            year7 DECIMAL(5, 2),
                            year10 DECIMAL(5, 2),
                            year20 DECIMAL(5, 2),
                            year30 DECIMAL(5, 2),
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_date (date),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """
                }
                
                for table_name, ddl in raw_tables.items():
                    cursor.execute(ddl)
                    self.logger.info(f"Raw data table {table_name} ready")
                    
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error creating raw data tables: {str(e)}")
    
    def get_load_statistics(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get loading statistics for monitoring"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                
                stats = {}
                
                tables = ['stock_data_raw', 'index_data_raw', 'commodity_data_raw', 'bond_data_raw']
                
                for table in tables:
                    cursor.execute(f"""
                        SELECT COUNT(*), MIN(loaded_at), MAX(loaded_at) 
                        FROM {table}
                        WHERE loaded_at >= %s
                    """, (cutoff_time,))
                    
                    result = cursor.fetchone()
                    data_type = table.replace('_data_raw', '')
                    stats[data_type] = {
                        'records': result[0],
                        'earliest_load': result[1],
                        'latest_load': result[2]
                    }
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting load statistics: {str(e)}")
            return {}
