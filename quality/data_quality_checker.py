"""
Data Quality Checker
Validates data quality and integrity for the market data warehouse
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd

from utils import get_db_connection, setup_logging
from config import STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS

class DataQualityChecker:
    def __init__(self):
        self.logger = setup_logging('data_quality_checker')
        
    def run_all_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all data quality checks"""
        results = {}
        
        try:
            # Data completeness checks
            results['completeness'] = self._check_data_completeness()
            
            # Data accuracy checks
            results['accuracy'] = self._check_data_accuracy()
            
            # Data consistency checks
            results['consistency'] = self._check_data_consistency()
            
            # Timeliness checks
            results['timeliness'] = self._check_data_timeliness()
            
            # Volume anomaly checks
            results['volume_anomalies'] = self._check_volume_anomalies()
            
            # Price anomaly checks
            results['price_anomalies'] = self._check_price_anomalies()
            
            self.logger.info("All data quality checks completed")
            
        except Exception as e:
            self.logger.error(f"Error running data quality checks: {str(e)}")
            results['error'] = {'passed': False, 'message': str(e)}
            
        return results
    
    def _check_data_completeness(self) -> Dict[str, Any]:
        """Check if we have complete data for all expected symbols"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Check last 24 hours
                cutoff_time = datetime.now() - timedelta(hours=24)
                
                results = {
                    'passed': True,
                    'message': 'Data completeness check passed',
                    'details': {}
                }
                
                # Check stock data completeness
                stock_issues = []
                for symbol in STOCK_SYMBOLS:
                    cursor.execute("""
                        SELECT COUNT(*) FROM stock_data 
                        WHERE symbol = %s AND date >= %s
                    """, (symbol, cutoff_time))
                    
                    count = cursor.fetchone()[0]
                    if count == 0:
                        stock_issues.append(symbol)
                
                if stock_issues:
                    results['passed'] = False
                    results['message'] = f"Missing stock data for: {', '.join(stock_issues)}"
                    results['details']['missing_stocks'] = stock_issues
                
                # Check index data completeness
                index_issues = []
                for symbol in INDEX_SYMBOLS:
                    cursor.execute("""
                        SELECT COUNT(*) FROM index_data 
                        WHERE symbol = %s AND date >= %s
                    """, (symbol, cutoff_time))
                    
                    count = cursor.fetchone()[0]
                    if count == 0:
                        index_issues.append(symbol)
                
                if index_issues:
                    results['passed'] = False
                    results['message'] += f" Missing index data for: {', '.join(index_issues)}"
                    results['details']['missing_indexes'] = index_issues
                
                # Check commodity data completeness
                commodity_issues = []
                for symbol in COMMODITY_SYMBOLS:
                    cursor.execute("""
                        SELECT COUNT(*) FROM commodity_data 
                        WHERE symbol = %s AND date >= %s
                    """, (symbol, cutoff_time))
                    
                    count = cursor.fetchone()[0]
                    if count == 0:
                        commodity_issues.append(symbol)
                
                if commodity_issues:
                    results['passed'] = False
                    results['message'] += f" Missing commodity data for: {', '.join(commodity_issues)}"
                    results['details']['missing_commodities'] = commodity_issues
                
                return results
                
        except Exception as e:
            return {'passed': False, 'message': f"Completeness check failed: {str(e)}"}
    
    def _check_data_accuracy(self) -> Dict[str, Any]:
        """Check data accuracy (price validation, etc.)"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                issues = []
                
                # Check for invalid prices (negative, zero, or null)
                tables = ['stock_data', 'index_data', 'commodity_data']
                
                for table in tables:
                    cursor.execute(f"""
                        SELECT symbol, date, open, high, low, close
                        FROM {table}
                        WHERE date >= CURDATE() - INTERVAL 1 DAY
                        AND (open <= 0 OR high <= 0 OR low <= 0 OR close <= 0
                             OR high < GREATEST(open, close, low)
                             OR low > LEAST(open, close, high))
                    """)
                    
                    invalid_records = cursor.fetchall()
                    if invalid_records:
                        issues.extend([f"{table}: {record}" for record in invalid_records])
                
                if issues:
                    return {
                        'passed': False,
                        'message': f"Found {len(issues)} invalid price records",
                        'details': issues[:10]  # Show first 10 issues
                    }
                else:
                    return {
                        'passed': True,
                        'message': 'Data accuracy check passed'
                    }
                    
        except Exception as e:
            return {'passed': False, 'message': f"Accuracy check failed: {str(e)}"}
    
    def _check_data_consistency(self) -> Dict[str, Any]:
        """Check data consistency between related records"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                issues = []
                
                # Check for duplicate records
                cursor.execute("""
                    SELECT symbol, date, COUNT(*)
                    FROM stock_data
                    WHERE date >= CURDATE() - INTERVAL 1 DAY
                    GROUP BY symbol, date
                    HAVING COUNT(*) > 1
                """)
                
                duplicates = cursor.fetchall()
                if duplicates:
                    issues.extend([f"Duplicate stock data: {dup}" for dup in duplicates])
                
                # Check for gaps in time series
                # This is a simplified check - in production you'd want more sophisticated gap detection
                
                if issues:
                    return {
                        'passed': False,
                        'message': f"Found {len(issues)} consistency issues",
                        'details': issues
                    }
                else:
                    return {
                        'passed': True,
                        'message': 'Data consistency check passed'
                    }
                    
        except Exception as e:
            return {'passed': False, 'message': f"Consistency check failed: {str(e)}"}
    
    def _check_data_timeliness(self) -> Dict[str, Any]:
        """Check if data is being updated in a timely manner"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Check when we last received data
                cursor.execute("""
                    SELECT MAX(date) as latest_stock FROM stock_data
                """)
                latest_stock = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT MAX(date) as latest_index FROM index_data
                """)
                latest_index = cursor.fetchone()[0]
                
                now = datetime.now()
                delay_threshold = timedelta(hours=2)  # Allow 2 hours delay
                
                issues = []
                
                if latest_stock and (now - latest_stock) > delay_threshold:
                    issues.append(f"Stock data delay: {now - latest_stock}")
                
                if latest_index and (now - latest_index) > delay_threshold:
                    issues.append(f"Index data delay: {now - latest_index}")
                
                if issues:
                    return {
                        'passed': False,
                        'message': f"Data timeliness issues: {', '.join(issues)}",
                        'details': issues
                    }
                else:
                    return {
                        'passed': True,
                        'message': 'Data timeliness check passed'
                    }
                    
        except Exception as e:
            return {'passed': False, 'message': f"Timeliness check failed: {str(e)}"}
    
    def _check_volume_anomalies(self) -> Dict[str, Any]:
        """Check for unusual volume patterns"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Find records with extremely high or low volume compared to recent average
                cursor.execute("""
                    WITH recent_avg AS (
                        SELECT symbol, AVG(volume) as avg_volume, STDDEV(volume) as std_volume
                        FROM stock_data
                        WHERE date >= CURDATE() - INTERVAL 30 DAY
                        GROUP BY symbol
                    )
                    SELECT s.symbol, s.date, s.volume, r.avg_volume
                    FROM stock_data s
                    JOIN recent_avg r ON s.symbol = r.symbol
                    WHERE s.date >= CURDATE() - INTERVAL 1 DAY
                    AND (s.volume > r.avg_volume + (3 * r.std_volume) 
                         OR s.volume < r.avg_volume / 10)
                """)
                
                anomalies = cursor.fetchall()
                
                if anomalies:
                    return {
                        'passed': False,
                        'message': f"Found {len(anomalies)} volume anomalies",
                        'details': [f"{symbol} at {date}: volume {volume}" 
                                  for symbol, date, volume, avg_vol in anomalies[:5]]
                    }
                else:
                    return {
                        'passed': True,
                        'message': 'Volume anomaly check passed'
                    }
                    
        except Exception as e:
            return {'passed': False, 'message': f"Volume anomaly check failed: {str(e)}"}
    
    def _check_price_anomalies(self) -> Dict[str, Any]:
        """Check for unusual price movements"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Find records with extreme price changes (>20% in 15 minutes)
                cursor.execute("""
                    SELECT symbol, date, open, close, 
                           ((close - open) / open * 100) as change_pct
                    FROM stock_data
                    WHERE date >= CURDATE() - INTERVAL 1 DAY
                    AND ABS((close - open) / open * 100) > 20
                """)
                
                anomalies = cursor.fetchall()
                
                if anomalies:
                    return {
                        'passed': False,
                        'message': f"Found {len(anomalies)} price anomalies",
                        'details': [f"{symbol} at {date}: {change_pct:.2f}% change" 
                                  for symbol, date, open_p, close_p, change_pct in anomalies]
                    }
                else:
                    return {
                        'passed': True,
                        'message': 'Price anomaly check passed'
                    }
                    
        except Exception as e:
            return {'passed': False, 'message': f"Price anomaly check failed: {str(e)}"}
    
    def generate_quality_report(self) -> str:
        """Generate a formatted data quality report"""
        results = self.run_all_checks()
        
        report = ["Data Quality Report", "=" * 50, ""]
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        overall_status = "PASS" if all(result.get('passed', False) for result in results.values() if 'passed' in result) else "FAIL"
        report.append(f"Overall Status: {overall_status}")
        report.append("")
        
        for check_name, result in results.items():
            if 'passed' in result:
                status = "PASS" if result['passed'] else "FAIL"
                report.append(f"{check_name.title()}: {status}")
                report.append(f"  Message: {result['message']}")
                
                if 'details' in result and result['details']:
                    report.append(f"  Details: {result['details']}")
                report.append("")
        
        return "\n".join(report)
