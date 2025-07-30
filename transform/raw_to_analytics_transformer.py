"""
Raw to Analytics Data Warehouse Transformer
Transforms data from raw data warehouse (DW1) to analytics data warehouse (DW2)
Adds technical indicators, aggregations, and market analytics
"""

import pandas as pd
import mysql.connector
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from contextlib import contextmanager

from config import ELT_CONFIG, STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import get_db_connection, setup_logging, batch_process, safe_float, safe_int

class RawToAnalyticsTransformer:
    def __init__(self):
        self.logger = setup_logging('raw_to_analytics_transformer')
        
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
    
    def transform_all_data(self, lookback_days: int = 1) -> Dict[str, Any]:
        """Transform raw data from last N days into analytics warehouse"""
        transform_results = {
            'stocks_transformed': 0,
            'indexes_transformed': 0,
            'commodities_transformed': 0,
            'bonds_transformed': 0,
            'analytics_generated': 0,
            'transform_time': datetime.now(),
            'errors': []
        }
        
        try:
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            
            with self.get_connection() as conn:
                # Transform OHLCV data (stocks, indexes, commodities)
                stock_count = self._transform_ohlcv_data(
                    conn, 'stock_data_raw', 'stock_data', cutoff_date
                )
                transform_results['stocks_transformed'] = stock_count
                
                index_count = self._transform_ohlcv_data(
                    conn, 'index_data_raw', 'index_data', cutoff_date
                )
                transform_results['indexes_transformed'] = index_count
                
                commodity_count = self._transform_ohlcv_data(
                    conn, 'commodity_data_raw', 'commodity_data', cutoff_date
                )
                transform_results['commodities_transformed'] = commodity_count
                
                # Transform bond data
                bond_count = self._transform_bond_data(conn, cutoff_date)
                transform_results['bonds_transformed'] = bond_count
                
                # Generate technical indicators
                indicators_count = self._generate_technical_indicators(conn, cutoff_date)
                transform_results['analytics_generated'] = indicators_count
                
                # Generate daily aggregates
                self._generate_daily_aggregates(conn, cutoff_date)
                
                # Generate market summary
                self._generate_market_summary(conn, cutoff_date)
                
                conn.commit()
                
        except Exception as e:
            error_msg = f"Error in data transformation: {str(e)}"
            self.logger.error(error_msg)
            transform_results['errors'].append(error_msg)
            
        return transform_results
    
    def _transform_ohlcv_data(self, conn, source_table: str, target_table: str, cutoff_date: datetime) -> int:
        """Transform OHLCV data from raw to analytics with data quality checks"""
        cursor = conn.cursor()
        
        # Get raw data with basic validation - updated for new schema
        cursor.execute(f"""
            SELECT symbol, datetime, date, open, high, low, close, volume, loaded_at
            FROM {source_table}
            WHERE loaded_at >= %s
            AND high >= GREATEST(open, close, low)
            AND low <= LEAST(open, close, high)
            AND open > 0 AND high > 0 AND low > 0 AND close > 0
            ORDER BY symbol, datetime
        """, (cutoff_date,))
        
        raw_data = cursor.fetchall()
        
        if not raw_data:
            self.logger.info(f"No new data to transform from {source_table}")
            return 0
        
        # Process data by symbol
        symbols_data = {}
        for row in raw_data:
            symbol = row[0]
            if symbol not in symbols_data:
                symbols_data[symbol] = []
            symbols_data[symbol].append(row)
        
        insert_sql = f"""
            INSERT INTO {target_table} 
            (symbol, datetime, date, open, high, low, close, volume, 
             price_change, price_change_pct, avg_price, volatility, 
             relative_volume, data_quality_score, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), volume=VALUES(volume),
                price_change=VALUES(price_change), price_change_pct=VALUES(price_change_pct),
                avg_price=VALUES(avg_price), volatility=VALUES(volatility),
                relative_volume=VALUES(relative_volume), 
                data_quality_score=VALUES(data_quality_score),
                loaded_at=NOW()
        """
        
        records_inserted = 0
        
        for symbol, symbol_data in symbols_data.items():
            try:
                # Calculate enhanced metrics for each record
                for i, row in enumerate(symbol_data):
                    datetime_val, date_val, open_price, high, low, close, volume = row[1:8]
                    
                    # Calculate basic metrics
                    avg_price = (high + low + close) / 3
                    price_change = None
                    price_change_pct = None
                    volatility = None
                    relative_volume = None
                    
                    # Calculate change from previous period if available
                    if i > 0:
                        prev_close = symbol_data[i-1][5]
                        price_change = round(close - prev_close, 4)
                        price_change_pct = round((price_change / prev_close) * 100, 2)
                    
                    # Calculate volatility (high-low range as % of close)
                    if close > 0:
                        volatility = round(((high - low) / close) * 100, 2)
                    
                    # Get historical volume for relative volume calculation
                    relative_volume = self._calculate_relative_volume(
                        cursor, symbol, source_table, volume, datetime_val
                    )
                    
                    # Calculate data quality score
                    quality_score = self._calculate_quality_score(
                        open_price, high, low, close, volume, volatility
                    )
                    
                    # Insert enhanced record
                    values = (
                        symbol, datetime_val, date_val, round(open_price, 4), round(high, 4), 
                        round(low, 4), round(close, 4), volume,
                        price_change, price_change_pct, round(avg_price, 4),
                        volatility, relative_volume, quality_score
                    )
                    
                    cursor.execute(insert_sql, values)
                    records_inserted += 1
                    
            except Exception as e:
                self.logger.error(f"Error transforming data for {symbol}: {str(e)}")
        
        self.logger.info(f"Transformed {records_inserted} records from {source_table} to {target_table}")
        return records_inserted
    
    def _transform_bond_data(self, conn, cutoff_date: datetime) -> int:
        """Transform bond data from raw to analytics - updated for new schema"""
        cursor = conn.cursor()
        
        # Get raw bond data - updated field names
        cursor.execute("""
            SELECT datetime, date, rate, yield_1m, yield_3m, yield_6m, yield_1y, 
                   yield_2y, yield_5y, yield_10y, yield_20y, yield_30y, loaded_at
            FROM bond_data_raw
            WHERE loaded_at >= %s
            ORDER BY datetime
        """, (cutoff_date,))
        
        raw_data = cursor.fetchall()
        
        if not raw_data:
            self.logger.info("No new bond data to transform")
            return 0
        
        insert_sql = """
            INSERT INTO bond_data
            (datetime, date, rate, yield_1m, yield_3m, yield_6m, yield_1y, 
             yield_2y, yield_5y, yield_10y, yield_20y, yield_30y, 
             yield_curve_slope, term_spread, credit_spread_proxy, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                rate=VALUES(rate), yield_1m=VALUES(yield_1m), yield_3m=VALUES(yield_3m),
                yield_6m=VALUES(yield_6m), yield_1y=VALUES(yield_1y), yield_2y=VALUES(yield_2y),
                yield_5y=VALUES(yield_5y), yield_10y=VALUES(yield_10y), yield_20y=VALUES(yield_20y),
                yield_30y=VALUES(yield_30y), yield_curve_slope=VALUES(yield_curve_slope), 
                term_spread=VALUES(term_spread), credit_spread_proxy=VALUES(credit_spread_proxy), 
                loaded_at=NOW()
        """
        
        records_inserted = 0
        
        for row in raw_data:
            try:
                datetime_val = row[0]
                date_val = row[1]
                rate = row[2]
                yields = row[3:12]  # yield_1m through yield_30y
                
                # Calculate yield curve analytics
                year2_yield = safe_float(row[6])  # year2
                year10_yield = safe_float(row[10])  # year10
                year30_yield = safe_float(row[12])  # year30
                
                # Yield curve slope (10Y - 2Y)
                yield_curve_slope = None
                if year10_yield is not None and year2_yield is not None:
                    yield_curve_slope = round(year10_yield - year2_yield, 2)
                
                # Term spread (30Y - 10Y)
                term_spread = None
                if year30_yield is not None and year10_yield is not None:
                    term_spread = round(year30_yield - year10_yield, 2)
                
                # Credit spread proxy (using corporate bond premium estimation)
                credit_spread_proxy = None
                if year10_yield is not None:
                    # Simple approximation based on historical spreads
                    credit_spread_proxy = round(year10_yield * 0.15, 2)
                
                values = [datetime_val, date_val, rate] + list(yields) + [yield_curve_slope, term_spread, credit_spread_proxy]
                
                cursor.execute(insert_sql, values)
                records_inserted += 1
                
            except Exception as e:
                self.logger.error(f"Error transforming bond data for {datetime_val}: {str(e)}")
        
        self.logger.info(f"Transformed {records_inserted} bond records")
        return records_inserted
    
    def _generate_technical_indicators(self, conn, cutoff_date: datetime) -> int:
        """Generate technical indicators for all OHLCV data"""
        cursor = conn.cursor()
        
        # Get list of symbols that need indicators calculated
        tables = ['stock_data', 'index_data', 'commodity_data']
        total_indicators = 0
        
        for table in tables:
            cursor.execute(f"""
                SELECT DISTINCT symbol FROM {table}
                WHERE date >= %s
            """, (cutoff_date,))
            
            symbols = [row[0] for row in cursor.fetchall()]
            
            for symbol in symbols:
                try:
                    indicators_count = self._calculate_symbol_indicators(cursor, table, symbol, cutoff_date)
                    total_indicators += indicators_count
                    
                except Exception as e:
                    self.logger.error(f"Error calculating indicators for {symbol}: {str(e)}")
        
        self.logger.info(f"Generated {total_indicators} technical indicators")
        return total_indicators
    
    def _calculate_symbol_indicators(self, cursor, table: str, symbol: str, cutoff_date: datetime) -> int:
        """Calculate technical indicators for a specific symbol"""
        # Get historical data (need more data for proper indicators)
        lookback_date = cutoff_date - timedelta(days=252)  # ~1 year for 200-day MA
        
        cursor.execute(f"""
            SELECT date, open, high, low, close, volume
            FROM {table}
            WHERE symbol = %s AND date >= %s
            ORDER BY date
        """, (symbol, lookback_date))
        
        data = cursor.fetchall()
        
        if len(data) < 20:  # Need minimum data for indicators
            return 0
        
        # Convert to DataFrame for easier calculations
        df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
        df['close'] = pd.to_numeric(df['close'])
        df['high'] = pd.to_numeric(df['high'])
        df['low'] = pd.to_numeric(df['low'])
        df['volume'] = pd.to_numeric(df['volume'])
        
        # Calculate technical indicators
        df = self._add_moving_averages(df)
        df = self._add_macd(df)
        df = self._add_rsi(df)
        df = self._add_bollinger_bands(df)
        
        # Insert indicators for recent data only
        insert_sql = """
            INSERT INTO technical_indicators
            (symbol, date, sma_20, sma_50, sma_200, ema_12, ema_26,
             macd_line, macd_signal, macd_histogram, rsi_14,
             bb_upper, bb_middle, bb_lower, bb_width, bb_position,
             calculated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                sma_20=VALUES(sma_20), sma_50=VALUES(sma_50), sma_200=VALUES(sma_200),
                ema_12=VALUES(ema_12), ema_26=VALUES(ema_26),
                macd_line=VALUES(macd_line), macd_signal=VALUES(macd_signal),
                macd_histogram=VALUES(macd_histogram), rsi_14=VALUES(rsi_14),
                bb_upper=VALUES(bb_upper), bb_middle=VALUES(bb_middle), bb_lower=VALUES(bb_lower),
                bb_width=VALUES(bb_width), bb_position=VALUES(bb_position),
                calculated_at=NOW()
        """
        
        records_inserted = 0
        
        # Only insert indicators for data after cutoff_date
        recent_df = df[df['date'] >= cutoff_date]
        
        for _, row in recent_df.iterrows():
            values = (
                symbol, row['date'],
                self._safe_round(row.get('sma_20')), self._safe_round(row.get('sma_50')), 
                self._safe_round(row.get('sma_200')),
                self._safe_round(row.get('ema_12')), self._safe_round(row.get('ema_26')),
                self._safe_round(row.get('macd_line')), self._safe_round(row.get('macd_signal')),
                self._safe_round(row.get('macd_histogram')), self._safe_round(row.get('rsi_14')),
                self._safe_round(row.get('bb_upper')), self._safe_round(row.get('bb_middle')),
                self._safe_round(row.get('bb_lower')), self._safe_round(row.get('bb_width')),
                self._safe_round(row.get('bb_position'))
            )
            
            cursor.execute(insert_sql, values)
            records_inserted += 1
        
        return records_inserted
    
    def _add_moving_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add moving averages to DataFrame"""
        df['sma_20'] = df['close'].rolling(window=20).mean()
        df['sma_50'] = df['close'].rolling(window=50).mean()
        df['sma_200'] = df['close'].rolling(window=200).mean()
        
        # Exponential moving averages
        df['ema_12'] = df['close'].ewm(span=12).mean()
        df['ema_26'] = df['close'].ewm(span=26).mean()
        
        return df
    
    def _add_macd(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add MACD indicators to DataFrame"""
        if 'ema_12' not in df.columns or 'ema_26' not in df.columns:
            df = self._add_moving_averages(df)
        
        df['macd_line'] = df['ema_12'] - df['ema_26']
        df['macd_signal'] = df['macd_line'].ewm(span=9).mean()
        df['macd_histogram'] = df['macd_line'] - df['macd_signal']
        
        return df
    
    def _add_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """Add RSI indicator to DataFrame"""
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        rs = avg_gain / avg_loss
        df['rsi_14'] = 100 - (100 / (1 + rs))
        
        return df
    
    def _add_bollinger_bands(self, df: pd.DataFrame, period: int = 20, std_dev: int = 2) -> pd.DataFrame:
        """Add Bollinger Bands to DataFrame"""
        df['bb_middle'] = df['close'].rolling(window=period).mean()
        bb_std = df['close'].rolling(window=period).std()
        
        df['bb_upper'] = df['bb_middle'] + (bb_std * std_dev)
        df['bb_lower'] = df['bb_middle'] - (bb_std * std_dev)
        
        # Band width and position
        df['bb_width'] = (df['bb_upper'] - df['bb_lower']) / df['bb_middle'] * 100
        df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower']) * 100
        
        return df
    
    def _generate_daily_aggregates(self, conn, cutoff_date: datetime):
        """Generate daily market aggregates"""
        cursor = conn.cursor()
        
        # Aggregate data from the last day
        aggregate_date = cutoff_date.date()
        
        insert_sql = """
            INSERT INTO daily_aggregates
            (date, total_stocks, advancing_stocks, declining_stocks, 
             unchanged_stocks, avg_volume, total_volume, market_breadth,
             advance_decline_ratio, up_volume, down_volume, vwap,
             calculated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE
                total_stocks=VALUES(total_stocks), advancing_stocks=VALUES(advancing_stocks),
                declining_stocks=VALUES(declining_stocks), unchanged_stocks=VALUES(unchanged_stocks),
                avg_volume=VALUES(avg_volume), total_volume=VALUES(total_volume),
                market_breadth=VALUES(market_breadth), advance_decline_ratio=VALUES(advance_decline_ratio),
                up_volume=VALUES(up_volume), down_volume=VALUES(down_volume), vwap=VALUES(vwap),
                calculated_at=NOW()
        """
        
        # Calculate aggregates from stock data
        cursor.execute("""
            SELECT 
                COUNT(*) as total_stocks,
                SUM(CASE WHEN price_change > 0 THEN 1 ELSE 0 END) as advancing,
                SUM(CASE WHEN price_change < 0 THEN 1 ELSE 0 END) as declining,
                SUM(CASE WHEN price_change = 0 THEN 1 ELSE 0 END) as unchanged,
                AVG(volume) as avg_volume,
                SUM(volume) as total_volume,
                SUM(CASE WHEN price_change > 0 THEN volume ELSE 0 END) as up_volume,
                SUM(CASE WHEN price_change < 0 THEN volume ELSE 0 END) as down_volume,
                SUM(avg_price * volume) / SUM(volume) as vwap
            FROM stock_data
            WHERE DATE(date) = %s
        """, (aggregate_date,))
        
        result = cursor.fetchone()
        
        if result and result[0] > 0:  # If we have stock data
            total_stocks, advancing, declining, unchanged = result[0:4]
            avg_volume, total_volume, up_volume, down_volume, vwap = result[4:9]
            
            # Calculate derived metrics
            market_breadth = round((advancing - declining) / total_stocks * 100, 2)
            advance_decline_ratio = round(advancing / declining, 2) if declining > 0 else None
            
            values = (
                aggregate_date, total_stocks, advancing, declining, unchanged,
                round(avg_volume, 0), total_volume, market_breadth, advance_decline_ratio,
                up_volume, down_volume, round(vwap, 4)
            )
            
            cursor.execute(insert_sql, values)
            self.logger.info(f"Generated daily aggregates for {aggregate_date}")
    
    def _generate_market_summary(self, conn, cutoff_date: datetime):
        """Generate market summary statistics"""
        cursor = conn.cursor()
        summary_date = cutoff_date.date()
        
        # Market summary for key indices
        summary_data = []
        key_indices = ['^GSPC', '^DJI', '^IXIC']  # S&P 500, Dow, Nasdaq
        
        for index_symbol in key_indices:
            cursor.execute("""
                SELECT close, price_change, price_change_pct, volume, volatility
                FROM index_data
                WHERE symbol = %s AND DATE(date) = %s
                ORDER BY date DESC
                LIMIT 1
            """, (index_symbol, summary_date))
            
            result = cursor.fetchone()
            if result:
                summary_data.append((index_symbol,) + result)
        
        # Insert market summary
        if summary_data:
            insert_sql = """
                INSERT INTO market_summary
                (date, sp500_close, sp500_change, sp500_change_pct,
                 dow_close, dow_change, dow_change_pct,
                 nasdaq_close, nasdaq_change, nasdaq_change_pct,
                 market_sentiment, volatility_index, calculated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    sp500_close=VALUES(sp500_close), sp500_change=VALUES(sp500_change),
                    sp500_change_pct=VALUES(sp500_change_pct), dow_close=VALUES(dow_close),
                    dow_change=VALUES(dow_change), dow_change_pct=VALUES(dow_change_pct),
                    nasdaq_close=VALUES(nasdaq_close), nasdaq_change=VALUES(nasdaq_change),
                    nasdaq_change_pct=VALUES(nasdaq_change_pct), 
                    market_sentiment=VALUES(market_sentiment), volatility_index=VALUES(volatility_index),
                    calculated_at=NOW()
            """
            
            # Organize data by index
            sp500_data = next((d for d in summary_data if d[0] == '^GSPC'), None)
            dow_data = next((d for d in summary_data if d[0] == '^DJI'), None)
            nasdaq_data = next((d for d in summary_data if d[0] == '^IXIC'), None)
            
            # Calculate market sentiment (simple average of major index changes)
            changes = [d[3] for d in summary_data if d[3] is not None]  # price_change_pct
            market_sentiment = round(sum(changes) / len(changes), 2) if changes else None
            
            # Calculate volatility index (average volatility of major indices)
            volatilities = [d[5] for d in summary_data if d[5] is not None]  # volatility
            volatility_index = round(sum(volatilities) / len(volatilities), 2) if volatilities else None
            
            values = (
                summary_date,
                sp500_data[1] if sp500_data else None,  # close
                sp500_data[2] if sp500_data else None,  # change
                sp500_data[3] if sp500_data else None,  # change_pct
                dow_data[1] if dow_data else None,
                dow_data[2] if dow_data else None,
                dow_data[3] if dow_data else None,
                nasdaq_data[1] if nasdaq_data else None,
                nasdaq_data[2] if nasdaq_data else None,
                nasdaq_data[3] if nasdaq_data else None,
                market_sentiment,
                volatility_index
            )
            
            cursor.execute(insert_sql, values)
            self.logger.info(f"Generated market summary for {summary_date}")
    
    def _calculate_relative_volume(self, cursor, symbol: str, source_table: str, 
                                 current_volume: int, current_date: datetime) -> Optional[float]:
        """Calculate relative volume compared to average"""
        try:
            # Get 30-day average volume
            lookback_date = current_date - timedelta(days=30)
            
            cursor.execute(f"""
                SELECT AVG(volume) FROM {source_table}
                WHERE symbol = %s AND date >= %s AND date < %s
            """, (symbol, lookback_date, current_date))
            
            result = cursor.fetchone()
            avg_volume = result[0] if result and result[0] else None
            
            if avg_volume and avg_volume > 0:
                return round(current_volume / avg_volume, 2)
            
        except Exception as e:
            self.logger.error(f"Error calculating relative volume for {symbol}: {str(e)}")
        
        return None
    
    def _calculate_quality_score(self, open_price: float, high: float, low: float, 
                               close: float, volume: int, volatility: Optional[float]) -> int:
        """Calculate data quality score (0-100)"""
        score = 100
        
        # Basic price validation
        if not (low <= min(open_price, close) <= max(open_price, close) <= high):
            score -= 30
        
        # Volume validation
        if volume <= 0:
            score -= 20
        
        # Volatility check (excessive volatility reduces score)
        if volatility and volatility > 20:  # >20% daily range
            score -= 10
        
        # Price continuity check (no gaps > 10%)
        price_range = high - low
        if close > 0 and (price_range / close) > 0.10:
            score -= 10
        
        return max(0, min(100, score))
    
    def _safe_round(self, value, decimals: int = 4) -> Optional[float]:
        """Safely round a value that might be None or NaN"""
        if value is None or pd.isna(value):
            return None
        try:
            return round(float(value), decimals)
        except (ValueError, TypeError):
            return None
    
    def create_analytics_tables(self):
        """Create analytics data warehouse tables"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Enhanced OHLCV tables with analytics
                enhanced_tables = {
                    'stock_data': """
                        CREATE TABLE IF NOT EXISTS stock_data (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(10) NOT NULL,
                            date DATETIME NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            price_change DECIMAL(12, 4),
                            price_change_pct DECIMAL(8, 2),
                            avg_price DECIMAL(12, 4),
                            volatility DECIMAL(8, 2),
                            relative_volume DECIMAL(8, 2),
                            data_quality_score INT DEFAULT 100,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_date (symbol, date),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_price_change (price_change_pct),
                            INDEX idx_volume (volume),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'index_data': """
                        CREATE TABLE IF NOT EXISTS index_data (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(10) NOT NULL,
                            date DATETIME NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            price_change DECIMAL(12, 4),
                            price_change_pct DECIMAL(8, 2),
                            avg_price DECIMAL(12, 4),
                            volatility DECIMAL(8, 2),
                            relative_volume DECIMAL(8, 2),
                            data_quality_score INT DEFAULT 100,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_date (symbol, date),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_price_change (price_change_pct),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'commodity_data': """
                        CREATE TABLE IF NOT EXISTS commodity_data (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(50) NOT NULL,
                            date DATETIME NOT NULL,
                            open DECIMAL(12, 4) NOT NULL,
                            high DECIMAL(12, 4) NOT NULL,
                            low DECIMAL(12, 4) NOT NULL,
                            close DECIMAL(12, 4) NOT NULL,
                            volume BIGINT NOT NULL,
                            price_change DECIMAL(12, 4),
                            price_change_pct DECIMAL(8, 2),
                            avg_price DECIMAL(12, 4),
                            volatility DECIMAL(8, 2),
                            relative_volume DECIMAL(8, 2),
                            data_quality_score INT DEFAULT 100,
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_date (symbol, date),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_price_change (price_change_pct),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'bond_data': """
                        CREATE TABLE IF NOT EXISTS bond_data (
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
                            yield_curve_slope DECIMAL(5, 2),
                            term_spread DECIMAL(5, 2),
                            credit_spread_proxy DECIMAL(5, 2),
                            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_date (date),
                            INDEX idx_yield_curve (yield_curve_slope),
                            INDEX idx_loaded_at (loaded_at)
                        )
                    """,
                    'technical_indicators': """
                        CREATE TABLE IF NOT EXISTS technical_indicators (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symbol VARCHAR(10) NOT NULL,
                            date DATETIME NOT NULL,
                            sma_20 DECIMAL(12, 4),
                            sma_50 DECIMAL(12, 4),
                            sma_200 DECIMAL(12, 4),
                            ema_12 DECIMAL(12, 4),
                            ema_26 DECIMAL(12, 4),
                            macd_line DECIMAL(12, 4),
                            macd_signal DECIMAL(12, 4),
                            macd_histogram DECIMAL(12, 4),
                            rsi_14 DECIMAL(5, 2),
                            bb_upper DECIMAL(12, 4),
                            bb_middle DECIMAL(12, 4),
                            bb_lower DECIMAL(12, 4),
                            bb_width DECIMAL(8, 2),
                            bb_position DECIMAL(8, 2),
                            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE KEY unique_symbol_date (symbol, date),
                            INDEX idx_symbol (symbol),
                            INDEX idx_date (date),
                            INDEX idx_rsi (rsi_14),
                            INDEX idx_calculated_at (calculated_at)
                        )
                    """,
                    'daily_aggregates': """
                        CREATE TABLE IF NOT EXISTS daily_aggregates (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            date DATE NOT NULL UNIQUE,
                            total_stocks INT,
                            advancing_stocks INT,
                            declining_stocks INT,
                            unchanged_stocks INT,
                            avg_volume BIGINT,
                            total_volume BIGINT,
                            market_breadth DECIMAL(5, 2),
                            advance_decline_ratio DECIMAL(5, 2),
                            up_volume BIGINT,
                            down_volume BIGINT,
                            vwap DECIMAL(12, 4),
                            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_date (date),
                            INDEX idx_breadth (market_breadth),
                            INDEX idx_calculated_at (calculated_at)
                        )
                    """,
                    'market_summary': """
                        CREATE TABLE IF NOT EXISTS market_summary (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            date DATE NOT NULL UNIQUE,
                            sp500_close DECIMAL(12, 4),
                            sp500_change DECIMAL(12, 4),
                            sp500_change_pct DECIMAL(8, 2),
                            dow_close DECIMAL(12, 4),
                            dow_change DECIMAL(12, 4),
                            dow_change_pct DECIMAL(8, 2),
                            nasdaq_close DECIMAL(12, 4),
                            nasdaq_change DECIMAL(12, 4),
                            nasdaq_change_pct DECIMAL(8, 2),
                            market_sentiment DECIMAL(8, 2),
                            volatility_index DECIMAL(8, 2),
                            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            INDEX idx_date (date),
                            INDEX idx_sentiment (market_sentiment),
                            INDEX idx_calculated_at (calculated_at)
                        )
                    """
                }
                
                for table_name, ddl in enhanced_tables.items():
                    cursor.execute(ddl)
                    self.logger.info(f"Analytics table {table_name} ready")
                    
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error creating analytics tables: {str(e)}")
