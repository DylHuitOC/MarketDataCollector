"""
ELT Orchestrator for Market Data Collection
Handles scheduled extraction, loading, and transformation of market data from FMP API
"""

import schedule
import time
import logging
import signal
import sys
from datetime import datetime, timedelta
import pytz
from threading import Thread, Event
import traceback

from config import ELT_CONFIG, MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE, MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE
from extract.market_data_extractor import MarketDataExtractor
from load.data_warehouse_loader import DataWarehouseLoader
from transform.analytics_transformer import AnalyticsTransformer
from utils import setup_logging, is_market_open, get_next_extraction_time

class ELTOrchestrator:
    def __init__(self):
        self.logger = setup_logging('elt_orchestrator')
        self.stop_event = Event()
        self.extractor = MarketDataExtractor()
        self.loader = DataWarehouseLoader()
        self.transformer = AnalyticsTransformer()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop_event.set()
        
    def extract_load_transform(self):
        """Main ELT process - runs every 15 minutes during market hours"""
        if not is_market_open():
            self.logger.info("Market is closed. Skipping extraction.")
            return
            
        try:
            self.logger.info("Starting ELT process...")
            start_time = datetime.now()
            
            # Extract phase
            self.logger.info("Phase 1: Extracting market data...")
            extracted_data = self.extractor.extract_all_current_data()
            
            if not extracted_data:
                self.logger.warning("No data extracted. Skipping load and transform phases.")
                return
                
            # Load phase
            self.logger.info("Phase 2: Loading data to warehouse...")
            load_results = self.loader.load_extracted_data(extracted_data)
            
            # Transform phase (run analytics on recent data)
            self.logger.info("Phase 3: Running transformations...")
            self.transformer.run_incremental_transforms()
            
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.info(f"ELT process completed in {duration:.2f} seconds")
            
            # Log summary statistics
            self._log_summary_stats(load_results)
            
        except Exception as e:
            self.logger.error(f"ELT process failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            
    def run_end_of_day_processing(self):
        """Comprehensive processing at market close"""
        try:
            self.logger.info("Starting end-of-day processing...")
            
            # Run full transformations
            self.transformer.run_daily_transforms()
            
            # Data quality checks
            self._run_data_quality_checks()
            
            # Cleanup old staging data
            self.loader.cleanup_staging_tables()
            
            self.logger.info("End-of-day processing completed")
            
        except Exception as e:
            self.logger.error(f"End-of-day processing failed: {str(e)}")
            
    def backfill_missing_data(self, start_date=None, end_date=None):
        """Backfill missing historical data"""
        try:
            if not start_date:
                start_date = datetime.now() - timedelta(days=ELT_CONFIG['lookback_days'])
            if not end_date:
                end_date = datetime.now()
                
            self.logger.info(f"Starting backfill from {start_date} to {end_date}")
            
            backfill_data = self.extractor.extract_historical_data(start_date, end_date)
            if backfill_data:
                self.loader.load_extracted_data(backfill_data, is_backfill=True)
                
        except Exception as e:
            self.logger.error(f"Backfill process failed: {str(e)}")
            
    def _run_data_quality_checks(self):
        """Run data quality validation"""
        from quality.data_quality_checker import DataQualityChecker
        
        checker = DataQualityChecker()
        quality_results = checker.run_all_checks()
        
        for check_name, result in quality_results.items():
            if not result['passed']:
                self.logger.warning(f"Data quality check failed: {check_name} - {result['message']}")
            
    def _log_summary_stats(self, load_results):
        """Log summary statistics from load process"""
        total_records = sum(result.get('records_loaded', 0) for result in load_results.values())
        self.logger.info(f"Total records loaded: {total_records}")
        
        for table, result in load_results.items():
            if result.get('records_loaded', 0) > 0:
                self.logger.info(f"  {table}: {result['records_loaded']} records")
                
    def schedule_jobs(self):
        """Setup scheduled jobs"""
        # Main ELT process every 15 minutes during market hours
        schedule.every(ELT_CONFIG['extract_interval_minutes']).minutes.do(self.extract_load_transform)
        
        # End of day processing
        schedule.every().day.at("16:30").do(self.run_end_of_day_processing)
        
        # Weekly data quality report
        schedule.every().monday.at("07:00").do(self._generate_weekly_report)
        
        # Daily backfill check
        schedule.every().day.at("06:00").do(self.backfill_missing_data)
        
        self.logger.info("Scheduled jobs configured:")
        self.logger.info(f"  - ELT process: Every {ELT_CONFIG['extract_interval_minutes']} minutes")
        self.logger.info("  - End-of-day processing: Daily at 4:30 PM")
        self.logger.info("  - Backfill check: Daily at 6:00 AM")
        self.logger.info("  - Weekly report: Mondays at 7:00 AM")
        
    def _generate_weekly_report(self):
        """Generate weekly data summary report"""
        try:
            from reporting.weekly_reporter import WeeklyReporter
            reporter = WeeklyReporter()
            reporter.generate_report()
        except Exception as e:
            self.logger.error(f"Weekly report generation failed: {str(e)}")
    
    def run(self):
        """Main execution loop"""
        self.logger.info("Starting ELT Orchestrator...")
        self.schedule_jobs()
        
        # Run initial backfill on startup
        self.logger.info("Running initial backfill check...")
        self.backfill_missing_data()
        
        self.logger.info("ELT Orchestrator is running. Press Ctrl+C to stop.")
        
        while not self.stop_event.is_set():
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error(f"Scheduler error: {str(e)}")
                time.sleep(60)
                
        self.logger.info("ELT Orchestrator shutting down...")

def main():
    """Entry point for the ELT orchestrator"""
    orchestrator = ELTOrchestrator()
    
    # Handle command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'backfill':
            start_date = sys.argv[2] if len(sys.argv) > 2 else None
            end_date = sys.argv[3] if len(sys.argv) > 3 else None
            if start_date:
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if end_date:
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            orchestrator.backfill_missing_data(start_date, end_date)
            
        elif command == 'run-once':
            orchestrator.extract_load_transform()
            
        elif command == 'eod':
            orchestrator.run_end_of_day_processing()
            
        else:
            print("Usage: python elt_orchestrator.py [backfill|run-once|eod] [start_date] [end_date]")
            sys.exit(1)
    else:
        # Run the main scheduler
        orchestrator.run()

if __name__ == "__main__":
    main()
