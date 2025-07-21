"""
Analytics Transformer
Handles data transformations, aggregations, and analytics calculations
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import numpy as np

from utils import get_db_connection, setup_logging
from config import STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS

class AnalyticsTransformer:
    def __init__(self):
        self.logger = setup_logging('analytics_transformer')
        
    def run_incremental_transforms(self):
        """Run incremental transformations on recent data"""
        try:
            self.logger.info("Starting incremental transformations...")
            
            # Calculate technical indicators for recent data
            self._calculate_recent_technical_indicators()
            
            # Update aggregated tables
            self._update_aggregated_tables()
            
            # Calculate performance metrics
            self._calculate_performance_metrics()
            
            self.logger.info("Incremental transformations completed")
            
        except Exception as e:
            self.logger.error(f"Error in incremental transformations: {str(e)}")
    
    def run_daily_transforms(self):
        """Run comprehensive daily transformations"""
        try:
            self.logger.info("Starting daily transformations...")
            
            # Full technical indicators recalculation
            self._calculate_all_technical_indicators()
            
            # Market summary calculations
            self._calculate_market_summaries()
            
            # Correlation analysis
            self._calculate_correlation_matrices()
            
            # Portfolio analytics
            self._calculate_portfolio_analytics()
            
            self.logger.info("Daily transformations completed")
            
        except Exception as e:
            self.logger.error(f"Error in daily transformations: {str(e)}")
    
    def _calculate_recent_technical_indicators(self):
        """Calculate technical indicators for recent data (last 200 periods)"""
        try:
            with get_db_connection() as conn:
                # Create or update technical indicators table
                self._ensure_technical_indicators_table(conn)
                
                # Process each symbol type
                self._process_symbol_indicators(conn, STOCK_SYMBOLS, 'stock_data')
                self._process_symbol_indicators(conn, INDEX_SYMBOLS, 'index_data')
                self._process_symbol_indicators(conn, COMMODITY_SYMBOLS, 'commodity_data')
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error calculating technical indicators: {str(e)}")
    
    def _process_symbol_indicators(self, conn, symbols: List[str], table_name: str):
        """Process technical indicators for a list of symbols"""
        cursor = conn.cursor()
        
        for symbol in symbols:
            try:
                # Get recent data for calculations (200 periods for proper MA/EMA calculation)
                if table_name == 'stock_data':
                    query = """
                        SELECT date, open, high, low, close, volume 
                        FROM stock_data 
                        WHERE symbol = %s 
                        ORDER BY date DESC 
                        LIMIT 200
                    """
                else:
                    query = f"""
                        SELECT date, open, high, low, close, volume 
                        FROM {table_name} 
                        WHERE symbol = %s 
                        ORDER BY date DESC 
                        LIMIT 200
                    """
                
                cursor.execute(query, (symbol,))
                data = cursor.fetchall()
                
                if len(data) < 20:  # Need minimum data for calculations
                    continue
                
                # Convert to DataFrame for easier calculations
                df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
                df = df.sort_values('date').reset_index(drop=True)
                
                # Calculate indicators
                indicators = self._calculate_indicators_for_symbol(df)
                
                # Store only the most recent indicator values
                if indicators:
                    latest_indicators = indicators.iloc[-1]
                    self._store_technical_indicators(cursor, symbol, latest_indicators, table_name)
                    
            except Exception as e:
                self.logger.error(f"Error processing indicators for {symbol}: {str(e)}")
    
    def _calculate_indicators_for_symbol(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators for a symbol's data"""
        try:
            # Simple Moving Averages
            df['sma_20'] = df['close'].rolling(window=20).mean()
            df['sma_50'] = df['close'].rolling(window=50).mean()
            df['sma_200'] = df['close'].rolling(window=200).mean()
            
            # Exponential Moving Averages
            df['ema_12'] = df['close'].ewm(span=12).mean()
            df['ema_26'] = df['close'].ewm(span=26).mean()
            
            # MACD
            df['macd'] = df['ema_12'] - df['ema_26']
            df['macd_signal'] = df['macd'].ewm(span=9).mean()
            df['macd_histogram'] = df['macd'] - df['macd_signal']
            
            # RSI
            df['rsi'] = self._calculate_rsi(df['close'])
            
            # Bollinger Bands
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            bb_std = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            
            # Volume indicators
            df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
            df['volume_ratio'] = df['volume'] / df['volume_sma_20']
            
            # Price change indicators
            df['price_change_1d'] = df['close'].pct_change()
            df['price_change_5d'] = df['close'].pct_change(periods=5)
            df['price_change_20d'] = df['close'].pct_change(periods=20)
            
            # Volatility (standard deviation of returns)
            df['volatility_20d'] = df['price_change_1d'].rolling(window=20).std()
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error calculating indicators: {str(e)}")
            return pd.DataFrame()
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        avg_gains = gains.rolling(window=period).mean()
        avg_losses = losses.rolling(window=period).mean()
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _store_technical_indicators(self, cursor, symbol: str, indicators: pd.Series, table_type: str):
        """Store technical indicators in the database"""
        try:
            # Insert or update technical indicators
            sql = """
                INSERT INTO technical_indicators 
                (symbol, table_type, date, sma_20, sma_50, sma_200, ema_12, ema_26, 
                 macd, macd_signal, macd_histogram, rsi, bb_upper, bb_middle, bb_lower,
                 volume_sma_20, volume_ratio, price_change_1d, price_change_5d, 
                 price_change_20d, volatility_20d, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE
                    sma_20=VALUES(sma_20), sma_50=VALUES(sma_50), sma_200=VALUES(sma_200),
                    ema_12=VALUES(ema_12), ema_26=VALUES(ema_26), macd=VALUES(macd),
                    macd_signal=VALUES(macd_signal), macd_histogram=VALUES(macd_histogram),
                    rsi=VALUES(rsi), bb_upper=VALUES(bb_upper), bb_middle=VALUES(bb_middle),
                    bb_lower=VALUES(bb_lower), volume_sma_20=VALUES(volume_sma_20),
                    volume_ratio=VALUES(volume_ratio), price_change_1d=VALUES(price_change_1d),
                    price_change_5d=VALUES(price_change_5d), price_change_20d=VALUES(price_change_20d),
                    volatility_20d=VALUES(volatility_20d), updated_at=NOW()
            """
            
            values = (
                symbol, table_type, indicators['date'],
                self._safe_float(indicators.get('sma_20')),
                self._safe_float(indicators.get('sma_50')),
                self._safe_float(indicators.get('sma_200')),
                self._safe_float(indicators.get('ema_12')),
                self._safe_float(indicators.get('ema_26')),
                self._safe_float(indicators.get('macd')),
                self._safe_float(indicators.get('macd_signal')),
                self._safe_float(indicators.get('macd_histogram')),
                self._safe_float(indicators.get('rsi')),
                self._safe_float(indicators.get('bb_upper')),
                self._safe_float(indicators.get('bb_middle')),
                self._safe_float(indicators.get('bb_lower')),
                self._safe_float(indicators.get('volume_sma_20')),
                self._safe_float(indicators.get('volume_ratio')),
                self._safe_float(indicators.get('price_change_1d')),
                self._safe_float(indicators.get('price_change_5d')),
                self._safe_float(indicators.get('price_change_20d')),
                self._safe_float(indicators.get('volatility_20d'))
            )
            
            cursor.execute(sql, values)
            
        except Exception as e:
            self.logger.error(f"Error storing indicators for {symbol}: {str(e)}")
    
    def _update_aggregated_tables(self):
        """Update daily, weekly, monthly aggregated tables"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Ensure aggregated tables exist
                self._ensure_aggregated_tables(conn)
                
                # Update daily aggregates
                self._update_daily_aggregates(cursor)
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error updating aggregated tables: {str(e)}")
    
    def _update_daily_aggregates(self, cursor):
        """Update daily OHLCV aggregates from 15-minute data"""
        # This would aggregate 15-minute data into daily bars
        # Implementation depends on specific requirements
        pass
    
    def _calculate_performance_metrics(self):
        """Calculate performance metrics and rankings"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Calculate daily performers
                self._calculate_daily_performers(cursor)
                
                # Update market cap rankings (if available)
                # This would require additional market cap data
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Error calculating performance metrics: {str(e)}")
    
    def _calculate_daily_performers(self, cursor):
        """Calculate top/bottom daily performers"""
        try:
            # Get latest close prices and calculate daily changes
            query = """
                SELECT symbol, close, 
                       LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_close
                FROM stock_data 
                WHERE date >= CURDATE() - INTERVAL 2 DAY
                ORDER BY symbol, date DESC
            """
            
            cursor.execute(query)
            # Process results and create rankings
            # Implementation details would depend on specific requirements
            
        except Exception as e:
            self.logger.error(f"Error calculating daily performers: {str(e)}")
    
    def _calculate_all_technical_indicators(self):
        """Full recalculation of all technical indicators"""
        # This would be a more comprehensive version of _calculate_recent_technical_indicators
        # that processes historical data as well
        pass
    
    def _calculate_market_summaries(self):
        """Calculate market-wide summary statistics"""
        # Calculate market breadth, sector performance, etc.
        pass
    
    def _calculate_correlation_matrices(self):
        """Calculate correlation matrices between symbols"""
        # Calculate price correlations for risk analysis
        pass
    
    def _calculate_portfolio_analytics(self):
        """Calculate portfolio-level analytics"""
        # Portfolio performance, risk metrics, etc.
        pass
    
    def _ensure_technical_indicators_table(self, conn):
        """Ensure technical indicators table exists"""
        cursor = conn.cursor()
        
        ddl = """
            CREATE TABLE IF NOT EXISTS technical_indicators (
                symbol VARCHAR(10) NOT NULL,
                table_type VARCHAR(20) NOT NULL,
                date DATETIME NOT NULL,
                sma_20 DECIMAL(12, 4),
                sma_50 DECIMAL(12, 4),
                sma_200 DECIMAL(12, 4),
                ema_12 DECIMAL(12, 4),
                ema_26 DECIMAL(12, 4),
                macd DECIMAL(12, 6),
                macd_signal DECIMAL(12, 6),
                macd_histogram DECIMAL(12, 6),
                rsi DECIMAL(5, 2),
                bb_upper DECIMAL(12, 4),
                bb_middle DECIMAL(12, 4),
                bb_lower DECIMAL(12, 4),
                volume_sma_20 BIGINT,
                volume_ratio DECIMAL(8, 4),
                price_change_1d DECIMAL(8, 6),
                price_change_5d DECIMAL(8, 6),
                price_change_20d DECIMAL(8, 6),
                volatility_20d DECIMAL(8, 6),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, table_type, date),
                INDEX idx_symbol_date (symbol, date),
                INDEX idx_updated_at (updated_at)
            )
        """
        
        cursor.execute(ddl)
    
    def _ensure_aggregated_tables(self, conn):
        """Ensure aggregated tables exist"""
        # Create daily, weekly, monthly aggregation tables
        pass
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float, handling NaN and None"""
        try:
            if pd.isna(value) or value is None:
                return None
            return float(value)
        except (ValueError, TypeError):
            return None
