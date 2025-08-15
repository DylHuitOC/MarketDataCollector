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
from load.csv_data_warehouse_loader import CSVDataWarehouseLoader
from transform.raw_to_analytics_transformer import RawToAnalyticsTransformer
from quality.data_quality_checker import DataQualityChecker
from utils import setup_logging, is_market_open, get_next_extraction_time

class ELTOrchestrator:
    def __init__(self):
        self.logger = setup_logging('elt_orchestrator')
        self.stop_event = Event()
        self.extractor = MarketDataExtractor()
        self.csv_loader = CSVDataWarehouseLoader()
        self.transformer = RawToAnalyticsTransformer()
        self.quality_checker = DataQualityChecker()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self.logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.stop_event.set()
        
    def extract_load_transform(self):
        """Main ELT process with CSV → DW1 → DW2 workflow - runs every 15 minutes during market hours"""
        if not is_market_open():
            self.logger.info("Market is closed. Skipping extraction.")
            return
            
        try:
            self.logger.info("Starting ELT process with CSV workflow...")
            start_time = datetime.now()
            
            # Phase 1: Extract data from FMP API and save as CSV
            self.logger.info("Phase 1: Extracting market data to CSV files...")
            extracted_data = self.extractor.extract_all_current_data()
            
            if not extracted_data or extracted_data.get('error'):
                self.logger.warning("CSV extraction failed or no data extracted. Skipping subsequent phases.")
                return
            
            # Verify CSV files were created
            csv_files = extracted_data.get('csv_files', {})
            if not csv_files:
                self.logger.warning("No CSV files were generated. Skipping load and transform phases.")
                return
                
            # Phase 2: Load CSV data into raw data warehouse (DW1)
            self.logger.info("Phase 2: Loading CSV data to raw data warehouse (DW1)...")
            csv_load_results = self.csv_loader.load_csv_files(csv_files)
            
            if csv_load_results.get('error'):
                self.logger.error(f"CSV loading failed: {csv_load_results['error']}")
                return
            
            # Phase 3: Transform data from DW1 to analytics warehouse (DW2)
            self.logger.info("Phase 3: Transforming data from DW1 to analytics warehouse (DW2)...")
            transform_results = self.transformer.transform_all_data(lookback_days=1)
            
            if transform_results.get('error'):
                self.logger.error(f"Transformation failed: {transform_results['error']}")
                return
            
            # Phase 4: Run data quality checks
            self.logger.info("Phase 4: Running data quality checks...")
            quality_results = self.quality_checker.run_all_checks()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            # Log summary statistics
            total_csv_records = sum(
                result.get('records_loaded', 0) 
                for result in csv_load_results.values() 
                if isinstance(result, dict)
            )
            total_analytics_records = (
                transform_results.get('stocks_transformed', 0) +
                transform_results.get('indexes_transformed', 0) +
                transform_results.get('commodities_transformed', 0)
            )
            
            self.logger.info(
                f"ELT process completed in {duration:.2f} seconds. "
                f"Processed {total_csv_records} CSV records → {total_analytics_records} analytics records"
            )
            
            # Archive processed CSV files
            self._archive_processed_csvs(csv_files)
            
        except Exception as e:
            self.logger.error(f"ELT process failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            
    def _archive_processed_csvs(self, csv_files):
        """Archive processed CSV files to avoid reprocessing"""
        try:
            import os
            import shutil
            from datetime import datetime
            
            archive_base = "data_extracts/archive"
            os.makedirs(archive_base, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for data_type, csv_path in csv_files.items():
                if os.path.exists(csv_path):
                    filename = os.path.basename(csv_path)
                    archive_path = os.path.join(archive_base, f"{timestamp}_{filename}")
                    shutil.move(csv_path, archive_path)
                    self.logger.info(f"Archived {csv_path} to {archive_path}")
                    
        except Exception as e:
            self.logger.error(f"Error archiving CSV files: {str(e)}")
            
    def run_end_of_day_processing(self):
        """Comprehensive processing at market close"""
        try:
            self.logger.info("Starting end-of-day processing...")
            
            # Process any remaining CSV files
            bulk_results = self.csv_loader.load_all_csv_files_in_directory()
            self.logger.info(f"Processed {bulk_results.get('total_files_processed', 0)} remaining CSV files")
            
            # Run comprehensive transformations for the day
            transform_results = self.transformer.transform_all_data(lookback_days=1)
            self.logger.info(f"Transformed {sum(v for k, v in transform_results.items() if k.endswith('_transformed'))} records")
            
            # Data quality checks
            self._run_data_quality_checks()
            
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
        try:
            quality_results = self.quality_checker.run_all_checks()
            
            for check_name, result in quality_results.items():
                if not result.get('passed', True):
                    self.logger.warning(f"Data quality check '{check_name}' failed: {result.get('message', 'Unknown issue')}")
                else:
                    self.logger.info(f"Data quality check '{check_name}' passed")
                    
        except Exception as e:
            self.logger.error(f"Data quality checks failed: {str(e)}")
            
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
