"""
Weekly Reporter
Generates weekly summary reports of market data and system performance
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
import pandas as pd

from utils import get_db_connection, setup_logging
from config import STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from quality.data_quality_checker import DataQualityChecker

class WeeklyReporter:
    def __init__(self):
        self.logger = setup_logging('weekly_reporter')
        
    def generate_report(self):
        """Generate comprehensive weekly report"""
        try:
            self.logger.info("Generating weekly report...")
            
            report_data = {
                'report_date': datetime.now(),
                'week_start': datetime.now() - timedelta(days=7),
                'week_end': datetime.now(),
                'data_summary': self._generate_data_summary(),
                'market_performance': self._generate_market_performance(),
                'data_quality': self._generate_quality_summary(),
                'system_metrics': self._generate_system_metrics()
            }
            
            # Format and save report
            formatted_report = self._format_report(report_data)
            self._save_report(formatted_report)
            
            self.logger.info("Weekly report generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating weekly report: {str(e)}")
    
    def _generate_data_summary(self) -> Dict[str, Any]:
        """Generate data ingestion summary"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                week_ago = datetime.now() - timedelta(days=7)
                
                summary = {}
                
                # Stock data summary
                cursor.execute("""
                    SELECT COUNT(*) as records, COUNT(DISTINCT symbol) as symbols,
                           MIN(date) as earliest, MAX(date) as latest
                    FROM stock_data
                    WHERE date >= %s
                """, (week_ago,))
                
                result = cursor.fetchone()
                summary['stocks'] = {
                    'records': result[0],
                    'symbols': result[1],
                    'earliest': result[2],
                    'latest': result[3]
                }
                
                # Index data summary
                cursor.execute("""
                    SELECT COUNT(*) as records, COUNT(DISTINCT symbol) as symbols,
                           MIN(date) as earliest, MAX(date) as latest
                    FROM index_data
                    WHERE date >= %s
                """, (week_ago,))
                
                result = cursor.fetchone()
                summary['indexes'] = {
                    'records': result[0],
                    'symbols': result[1],
                    'earliest': result[2],
                    'latest': result[3]
                }
                
                # Commodity data summary
                cursor.execute("""
                    SELECT COUNT(*) as records, COUNT(DISTINCT symbol) as symbols,
                           MIN(date) as earliest, MAX(date) as latest
                    FROM commodity_data
                    WHERE date >= %s
                """, (week_ago,))
                
                result = cursor.fetchone()
                summary['commodities'] = {
                    'records': result[0],
                    'symbols': result[1],
                    'earliest': result[2],
                    'latest': result[3]
                }
                
                return summary
                
        except Exception as e:
            self.logger.error(f"Error generating data summary: {str(e)}")
            return {}
    
    def _generate_market_performance(self) -> Dict[str, Any]:
        """Generate market performance summary"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                week_ago = datetime.now() - timedelta(days=7)
                
                performance = {}
                
                # Top stock performers
                cursor.execute("""
                    WITH week_data AS (
                        SELECT symbol, 
                               FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date) as week_start_price,
                               LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as week_end_price
                        FROM stock_data
                        WHERE date >= %s
                    ),
                    weekly_returns AS (
                        SELECT symbol,
                               ((week_end_price - week_start_price) / week_start_price * 100) as return_pct
                        FROM week_data
                        GROUP BY symbol, week_start_price, week_end_price
                    )
                    SELECT symbol, return_pct
                    FROM weekly_returns
                    ORDER BY return_pct DESC
                    LIMIT 5
                """, (week_ago,))
                
                top_performers = cursor.fetchall()
                performance['top_stocks'] = [{'symbol': symbol, 'return': return_pct} 
                                           for symbol, return_pct in top_performers]
                
                # Bottom stock performers
                cursor.execute("""
                    WITH week_data AS (
                        SELECT symbol, 
                               FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date) as week_start_price,
                               LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as week_end_price
                        FROM stock_data
                        WHERE date >= %s
                    ),
                    weekly_returns AS (
                        SELECT symbol,
                               ((week_end_price - week_start_price) / week_start_price * 100) as return_pct
                        FROM week_data
                        GROUP BY symbol, week_start_price, week_end_price
                    )
                    SELECT symbol, return_pct
                    FROM weekly_returns
                    ORDER BY return_pct ASC
                    LIMIT 5
                """, (week_ago,))
                
                bottom_performers = cursor.fetchall()
                performance['bottom_stocks'] = [{'symbol': symbol, 'return': return_pct} 
                                              for symbol, return_pct in bottom_performers]
                
                # Index performance
                cursor.execute("""
                    WITH week_data AS (
                        SELECT symbol, 
                               FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date) as week_start_price,
                               LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date 
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as week_end_price
                        FROM index_data
                        WHERE date >= %s
                    )
                    SELECT symbol,
                           ((week_end_price - week_start_price) / week_start_price * 100) as return_pct
                    FROM week_data
                    GROUP BY symbol, week_start_price, week_end_price
                """, (week_ago,))
                
                index_performance = cursor.fetchall()
                performance['indexes'] = [{'symbol': symbol, 'return': return_pct} 
                                        for symbol, return_pct in index_performance]
                
                return performance
                
        except Exception as e:
            self.logger.error(f"Error generating market performance: {str(e)}")
            return {}
    
    def _generate_quality_summary(self) -> Dict[str, Any]:
        """Generate data quality summary"""
        try:
            quality_checker = DataQualityChecker()
            quality_results = quality_checker.run_all_checks()
            
            summary = {
                'overall_status': 'PASS' if all(result.get('passed', False) 
                                               for result in quality_results.values() 
                                               if 'passed' in result) else 'FAIL',
                'checks_performed': len([r for r in quality_results.values() if 'passed' in r]),
                'checks_passed': len([r for r in quality_results.values() if r.get('passed', False)]),
                'issues': []
            }
            
            for check_name, result in quality_results.items():
                if 'passed' in result and not result['passed']:
                    summary['issues'].append({
                        'check': check_name,
                        'message': result['message']
                    })
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating quality summary: {str(e)}")
            return {}
    
    def _generate_system_metrics(self) -> Dict[str, Any]:
        """Generate system performance metrics"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                metrics = {}
                
                # Database size metrics
                cursor.execute("SELECT table_name, table_rows FROM information_schema.tables WHERE table_schema = DATABASE()")
                table_info = cursor.fetchall()
                
                metrics['table_sizes'] = {table: rows for table, rows in table_info}
                
                # Data freshness
                cursor.execute("SELECT MAX(date) FROM stock_data")
                latest_stock_data = cursor.fetchone()[0]
                
                if latest_stock_data:
                    data_age = datetime.now() - latest_stock_data
                    metrics['data_freshness_hours'] = data_age.total_seconds() / 3600
                
                return metrics
                
        except Exception as e:
            self.logger.error(f"Error generating system metrics: {str(e)}")
            return {}
    
    def _format_report(self, report_data: Dict[str, Any]) -> str:
        """Format report data into readable text"""
        report = []
        
        # Header
        report.append("="*60)
        report.append("MARKET DATA COLLECTOR - WEEKLY REPORT")
        report.append("="*60)
        report.append(f"Report Generated: {report_data['report_date'].strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Week Period: {report_data['week_start'].strftime('%Y-%m-%d')} to {report_data['week_end'].strftime('%Y-%m-%d')}")
        report.append("")
        
        # Data Summary
        report.append("DATA INGESTION SUMMARY")
        report.append("-"*30)
        data_summary = report_data.get('data_summary', {})
        
        for data_type, summary in data_summary.items():
            report.append(f"{data_type.title()}:")
            report.append(f"  Records: {summary.get('records', 0):,}")
            report.append(f"  Symbols: {summary.get('symbols', 0)}")
            report.append(f"  Date Range: {summary.get('earliest', 'N/A')} to {summary.get('latest', 'N/A')}")
            report.append("")
        
        # Market Performance
        report.append("MARKET PERFORMANCE")
        report.append("-"*20)
        performance = report_data.get('market_performance', {})
        
        if 'top_stocks' in performance:
            report.append("Top Stock Performers:")
            for stock in performance['top_stocks'][:3]:
                report.append(f"  {stock['symbol']}: {stock['return']:.2f}%")
        
        if 'bottom_stocks' in performance:
            report.append("Bottom Stock Performers:")
            for stock in performance['bottom_stocks'][:3]:
                report.append(f"  {stock['symbol']}: {stock['return']:.2f}%")
        
        if 'indexes' in performance:
            report.append("Index Performance:")
            for index in performance['indexes']:
                report.append(f"  {index['symbol']}: {index['return']:.2f}%")
        
        report.append("")
        
        # Data Quality
        report.append("DATA QUALITY")
        report.append("-"*15)
        quality = report_data.get('data_quality', {})
        
        report.append(f"Overall Status: {quality.get('overall_status', 'UNKNOWN')}")
        report.append(f"Checks Performed: {quality.get('checks_performed', 0)}")
        report.append(f"Checks Passed: {quality.get('checks_passed', 0)}")
        
        if quality.get('issues'):
            report.append("Issues Found:")
            for issue in quality['issues']:
                report.append(f"  - {issue['check']}: {issue['message']}")
        
        report.append("")
        
        # System Metrics
        report.append("SYSTEM METRICS")
        report.append("-"*15)
        metrics = report_data.get('system_metrics', {})
        
        if 'data_freshness_hours' in metrics:
            freshness = metrics['data_freshness_hours']
            report.append(f"Data Freshness: {freshness:.1f} hours")
        
        if 'table_sizes' in metrics:
            report.append("Table Sizes:")
            for table, rows in metrics['table_sizes'].items():
                if rows and rows > 0:
                    report.append(f"  {table}: {rows:,} rows")
        
        report.append("")
        report.append("="*60)
        
        return "\n".join(report)
    
    def _save_report(self, report_content: str):
        """Save report to file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"reports/weekly_report_{timestamp}.txt"
            
            # Create reports directory if it doesn't exist
            import os
            os.makedirs('reports', exist_ok=True)
            
            with open(filename, 'w') as f:
                f.write(report_content)
            
            self.logger.info(f"Report saved to {filename}")
            
        except Exception as e:
            self.logger.error(f"Error saving report: {str(e)}")
