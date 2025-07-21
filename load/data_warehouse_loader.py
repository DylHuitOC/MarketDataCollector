"""
Data Warehouse Loader
Handles loading extracted data into MySQL data warehouse with staging and error handling
"""

import mysql.connector
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import pandas as pd
from contextlib import contextmanager

from config import ELT_CONFIG, STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import get_db_connection, setup_logging, batch_process, safe_float, safe_int

class DataWarehouseLoader:
    def __init__(self):
        self.logger = setup_logging('data_warehouse_loader')
        
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
    
    def load_extracted_data(self, extracted_data: Dict[str, Any], is_backfill: bool = False) -> Dict[str, Any]:
        """Load extracted data into the data warehouse"""
        load_results = {
            'stocks': {'records_loaded': 0, 'errors': []},
            'indexes': {'records_loaded': 0, 'errors': []},
            'commodities': {'records_loaded': 0, 'errors': []},
            'bonds': {'records_loaded': 0, 'errors': []},
            'load_time': datetime.now(),
            'is_backfill': is_backfill
        }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Load stock data
                if 'stocks' in extracted_data:
                    self.logger.info("Loading stock data...")
                    stock_result = self._load_ohlcv_data(
                        cursor, extracted_data['stocks'], 'stock_data', is_backfill
                    )
                    load_results['stocks'] = stock_result
                    
                # Load index data
                if 'indexes' in extracted_data:
                    self.logger.info("Loading index data...")
                    index_result = self._load_ohlcv_data(
                        cursor, extracted_data['indexes'], 'index_data', is_backfill
                    )
                    load_results['indexes'] = index_result
                    
                # Load commodity data
                if 'commodities' in extracted_data:
                    self.logger.info("Loading commodity data...")
                    commodity_result = self._load_ohlcv_data(
                        cursor, extracted_data['commodities'], 'commodity_data', is_backfill
                    )
                    load_results['commodities'] = commodity_result
                    
                # Load bond data
                if 'bonds' in extracted_data:
                    self.logger.info("Loading bond data...")
                    bond_result = self._load_bond_data(cursor, extracted_data['bonds'], is_backfill)
                    load_results['bonds'] = bond_result
                    
                # Commit all changes
                conn.commit()
                self.logger.info("All data loaded successfully")
                
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            load_results['error'] = str(e)
            
        return load_results
    
    def _load_ohlcv_data(self, cursor, symbols_data: Dict[str, List[Dict]], 
                        table_name: str, is_backfill: bool) -> Dict[str, Any]:
        """Load OHLCV data for stocks, indexes, or commodities"""
        result = {'records_loaded': 0, 'errors': [], 'duplicates_skipped': 0}
        
        for symbol, data_records in symbols_data.items():
            try:
                # Process in batches for better performance
                for batch in batch_process(data_records, ELT_CONFIG.get('batch_size', 100)):
                    batch_result = self._insert_ohlcv_batch(cursor, batch, symbol, table_name, is_backfill)
                    result['records_loaded'] += batch_result['inserted']
                    result['duplicates_skipped'] += batch_result['duplicates']
                    
            except Exception as e:
                error_msg = f"Error loading {symbol} into {table_name}: {str(e)}"
                self.logger.error(error_msg)
                result['errors'].append(error_msg)
                
        return result
    
    def _insert_ohlcv_batch(self, cursor, batch: List[Dict], symbol: str, 
                           table_name: str, is_backfill: bool) -> Dict[str, int]:
        """Insert a batch of OHLCV records"""
        result = {'inserted': 0, 'duplicates': 0}
        
        # Prepare the SQL based on table type
        if table_name == 'stock_data':
            sql = """
                INSERT INTO stock_data (date, open, low, high, close, volume, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            duplicate_check_sql = "SELECT COUNT(*) FROM stock_data WHERE date = %s AND symbol = %s"
            
        elif table_name == 'index_data':
            sql = """
                INSERT INTO index_data (date, symbol, open, low, high, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open), low=VALUES(low), high=VALUES(high), 
                    close=VALUES(close), volume=VALUES(volume)
            """
            duplicate_check_sql = "SELECT COUNT(*) FROM index_data WHERE date = %s AND symbol = %s"
            
        elif table_name == 'commodity_data':
            sql = """
                INSERT INTO commodity_data (date, symbol, open, low, high, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open=VALUES(open), low=VALUES(low), high=VALUES(high), 
                    close=VALUES(close), volume=VALUES(volume)
            """
            duplicate_check_sql = "SELECT COUNT(*) FROM commodity_data WHERE date = %s AND symbol = %s"
        
        for record in batch:
            try:
                # Parse and validate the date
                record_date = pd.to_datetime(record['date']).to_pydatetime().replace(tzinfo=None)
                
                # For stock_data table, check for duplicates manually (no primary key)
                if table_name == 'stock_data':
                    cursor.execute(duplicate_check_sql, (record_date, symbol))
                    if cursor.fetchone()[0] > 0:
                        if not is_backfill:  # Only skip duplicates for real-time data
                            result['duplicates'] += 1
                            continue
                
                # Prepare values based on table structure
                if table_name == 'stock_data':
                    values = (
                        record_date,
                        round(safe_float(record['open']), 2),
                        round(safe_float(record['low']), 2),
                        round(safe_float(record['high']), 2),
                        round(safe_float(record['close']), 2),
                        safe_int(record['volume']),
                        symbol
                    )
                else:  # index_data or commodity_data
                    values = (
                        record_date,
                        symbol,
                        round(safe_float(record['open']), 4),
                        round(safe_float(record['low']), 4),
                        round(safe_float(record['high']), 4),
                        round(safe_float(record['close']), 4),
                        safe_int(record['volume'])
                    )
                
                cursor.execute(sql, values)
                result['inserted'] += 1
                
            except Exception as e:
                self.logger.error(f"Error inserting record for {symbol} at {record.get('date', 'unknown')}: {str(e)}")
                
        return result
    
    def _load_bond_data(self, cursor, bond_data: Dict, is_backfill: bool) -> Dict[str, Any]:
        """Load treasury bond data"""
        result = {'records_loaded': 0, 'errors': []}
        
        try:
            if not bond_data or 'date' not in bond_data:
                return result
                
            # Check if record already exists
            bond_date = pd.to_datetime(bond_data['date']).date()
            cursor.execute("SELECT COUNT(*) FROM bond_data WHERE date = %s", (bond_date,))
            
            if cursor.fetchone()[0] > 0:
                if not is_backfill:
                    self.logger.info(f"Bond data for {bond_date} already exists, skipping")
                    return result
                else:
                    # Update existing record for backfill
                    sql = """
                        UPDATE bond_data SET
                            month1=%s, month2=%s, month3=%s, month6=%s,
                            year1=%s, year2=%s, year3=%s, year5=%s,
                            year7=%s, year10=%s, year20=%s, year30=%s
                        WHERE date=%s
                    """
            else:
                # Insert new record
                sql = """
                    INSERT INTO bond_data 
                    (date, month1, month2, month3, month6, year1, year2, year3, 
                     year5, year7, year10, year20, year30)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            
            values = (
                bond_date if not is_backfill or cursor.fetchone()[0] == 0 else None,
                safe_float(bond_data.get('month1')),
                safe_float(bond_data.get('month2')),
                safe_float(bond_data.get('month3')),
                safe_float(bond_data.get('month6')),
                safe_float(bond_data.get('year1')),
                safe_float(bond_data.get('year2')),
                safe_float(bond_data.get('year3')),
                safe_float(bond_data.get('year5')),
                safe_float(bond_data.get('year7')),
                safe_float(bond_data.get('year10')),
                safe_float(bond_data.get('year20')),
                safe_float(bond_data.get('year30'))
            )
            
            # Adjust values for update vs insert
            if cursor.fetchone()[0] > 0 and is_backfill:
                values = values[1:] + (bond_date,)  # Move date to end for UPDATE
            
            cursor.execute(sql, values)
            result['records_loaded'] = 1
            
        except Exception as e:
            error_msg = f"Error loading bond data: {str(e)}"
            self.logger.error(error_msg)
            result['errors'].append(error_msg)
            
        return result
    
    def cleanup_staging_tables(self):
        """Clean up old staging data and temporary tables"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Clean up old data (keep last 30 days)
                cutoff_date = datetime.now() - timedelta(days=30)
                
                # Example cleanup queries - adjust based on your retention policy
                cleanup_queries = [
                    f"DELETE FROM stock_data WHERE date < '{cutoff_date}' AND volume = 0",
                    f"DELETE FROM index_data WHERE date < '{cutoff_date}' AND volume = 0",
                    f"DELETE FROM commodity_data WHERE date < '{cutoff_date}' AND volume = 0"
                ]
                
                for query in cleanup_queries:
                    try:
                        cursor.execute(query)
                        rows_deleted = cursor.rowcount
                        if rows_deleted > 0:
                            self.logger.info(f"Cleaned up {rows_deleted} old records")
                    except Exception as e:
                        self.logger.error(f"Cleanup query failed: {str(e)}")
                
                conn.commit()
                self.logger.info("Staging cleanup completed")
                
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")
    
    def get_load_statistics(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get loading statistics for monitoring"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                
                stats = {}
                
                # Stock data stats
                cursor.execute("""
                    SELECT COUNT(*), MIN(date), MAX(date) 
                    FROM stock_data 
                    WHERE date >= %s
                """, (cutoff_time,))
                
                result = cursor.fetchone()
                stats['stocks'] = {
                    'records': result[0],
                    'earliest': result[1],
                    'latest': result[2]
                }
                
                # Index data stats
                cursor.execute("""
                    SELECT COUNT(*), MIN(date), MAX(date) 
                    FROM index_data 
                    WHERE date >= %s
                """, (cutoff_time,))
                
                result = cursor.fetchone()
                stats['indexes'] = {
                    'records': result[0],
                    'earliest': result[1],
                    'latest': result[2]
                }
                
                # Commodity data stats
                cursor.execute("""
                    SELECT COUNT(*), MIN(date), MAX(date) 
                    FROM commodity_data 
                    WHERE date >= %s
                """, (cutoff_time,))
                
                result = cursor.fetchone()
                stats['commodities'] = {
                    'records': result[0],
                    'earliest': result[1],
                    'latest': result[2]
                }
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting load statistics: {str(e)}")
            return {}
    
    def create_staging_tables(self):
        """Create staging tables for bulk operations"""
        staging_ddl = {
            'stock_data_staging': """
                CREATE TABLE IF NOT EXISTS stock_data_staging (
                    date DATETIME NOT NULL,
                    open DECIMAL(10, 2) NOT NULL,
                    low DECIMAL(10, 2) NOT NULL,
                    high DECIMAL(10, 2) NOT NULL,
                    close DECIMAL(10, 2) NOT NULL,
                    volume INT NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_staging_date_symbol (date, symbol)
                )
            """,
            'index_data_staging': """
                CREATE TABLE IF NOT EXISTS index_data_staging (
                    date DATETIME NOT NULL,
                    symbol VARCHAR(10) NOT NULL,
                    open DECIMAL(12, 2) NOT NULL,
                    low DECIMAL(12, 2) NOT NULL,
                    high DECIMAL(12, 2) NOT NULL,
                    close DECIMAL(12, 2) NOT NULL,
                    volume BIGINT NOT NULL,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_staging_date_symbol (date, symbol)
                )
            """
        }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for table_name, ddl in staging_ddl.items():
                    cursor.execute(ddl)
                    self.logger.info(f"Staging table {table_name} ready")
                    
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error creating staging tables: {str(e)}")
