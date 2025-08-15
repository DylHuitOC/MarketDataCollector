"""
ELT Process Runner
Simple script to run different ELT operations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from datetime import datetime, timedelta

from elt_orchestrator import ELTOrchestrator
from extract.market_data_extractor import MarketDataExtractor
from load.csv_data_warehouse_loader import CSVDataWarehouseLoader
from transform.raw_to_analytics_transformer import RawToAnalyticsTransformer
from quality.data_quality_checker import DataQualityChecker
from reporting.weekly_reporter import WeeklyReporter
from utils import setup_logging

def run_extraction():
    """Run data extraction to CSV only"""
    logger = setup_logging('elt_runner')
    logger.info("Running CSV extraction process...")
    
    extractor = MarketDataExtractor()
    extracted_data = extractor.extract_all_current_data()
    
    if extracted_data and not extracted_data.get('error'):
        csv_files = extracted_data.get('csv_files', {})
        logger.info(f"Extraction completed. CSV files created: {list(csv_files.keys())}")
        return csv_files
    else:
        logger.warning("No data extracted or extraction failed")
        return None

def run_csv_loading(csv_files=None):
    """Load CSV files into raw data warehouse"""
    logger = setup_logging('elt_runner')
    logger.info("Running CSV to raw data warehouse loading...")
    
    csv_loader = CSVDataWarehouseLoader()
    
    if csv_files:
        # Load specific CSV files
        result = csv_loader.load_csv_files(csv_files)
    else:
        # Load all CSV files in directory
        result = csv_loader.load_all_csv_files_in_directory()
    
    logger.info(f"CSV loading completed. Results: {result}")
    return result

def run_transformation():
    """Transform data from raw DW to analytics DW"""
    logger = setup_logging('elt_runner')
    logger.info("Running raw to analytics transformation...")
    
    transformer = RawToAnalyticsTransformer()
    result = transformer.transform_all_data(lookback_days=1)
    
    logger.info(f"Transformation completed. Results: {result}")
    return result

def run_full_elt():
    """Run complete ELT process: CSV extraction -> DW1 loading -> DW2 transformation"""
    logger = setup_logging('elt_runner')
    logger.info("Running full CSV -> DW1 -> DW2 ELT process...")
    
    # Step 1: Extract to CSV
    csv_files = run_extraction()
    if not csv_files:
        logger.error("CSV extraction failed, stopping ELT process")
        return
    
    # Step 2: Load CSV to raw DW
    load_result = run_csv_loading(csv_files)
    if load_result.get('error'):
        logger.error(f"CSV loading failed: {load_result['error']}")
        return
    
    # Step 3: Transform to analytics DW
    transform_result = run_transformation()
    if transform_result.get('error'):
        logger.error(f"Transformation failed: {transform_result['error']}")
        return
    
    # Step 4: Quality checks
    run_quality_check()
    
    logger.info("Full ELT process completed successfully")

def run_backfill(start_date_str, end_date_str):
    """Run historical data backfill"""
    logger = setup_logging('elt_runner')
    logger.info(f"Running backfill from {start_date_str} to {end_date_str}")
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    # Note: Backfill would need to be implemented in the extractor for CSV workflow
    logger.warning("Backfill not yet implemented for CSV workflow")

def run_transforms():
    """Run analytics transformations (legacy method name)"""
    return run_transformation()
    logger.info("Transformations completed")

def run_quality_check():
    """Run data quality checks"""
    logger = setup_logging('elt_runner')
    logger.info("Running data quality checks...")
    
    checker = DataQualityChecker()
    results = checker.run_all_checks()
    
    # Print summary
    total_checks = len([r for r in results.values() if 'passed' in r])
    passed_checks = len([r for r in results.values() if r.get('passed', False)])
    
    print(f"\nData Quality Check Results:")
    print(f"Checks Run: {total_checks}")
    print(f"Checks Passed: {passed_checks}")
    print(f"Overall Status: {'PASS' if passed_checks == total_checks else 'FAIL'}")
    
    for check_name, result in results.items():
        if 'passed' in result:
            status = 'PASS' if result['passed'] else 'FAIL'
            print(f"  {check_name}: {status} - {result['message']}")

def run_weekly_report():
    """Generate weekly report"""
    logger = setup_logging('elt_runner')
    logger.info("Generating weekly report...")
    
    reporter = WeeklyReporter()
    reporter.generate_report()
    logger.info("Weekly report generated")

def run_scheduler():
    """Start the ELT scheduler"""
    logger = setup_logging('elt_runner')
    logger.info("Starting ELT scheduler...")
    
    orchestrator = ELTOrchestrator()
    orchestrator.run()

def main():
    parser = argparse.ArgumentParser(description='ELT Process Runner')
    parser.add_argument('command', choices=[
        'extract', 'csv-load', 'transform', 'quality', 'full', 'report', 'schedule'
    ], help='Command to run')
    parser.add_argument('--start-date', help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for backfill (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'extract':
            run_extraction()
        elif args.command == 'csv-load':
            run_csv_loading()
        elif args.command == 'transform':
            run_transformation()
        elif args.command == 'quality':
            run_quality_check()
        elif args.command == 'full':
            run_full_elt()
        elif args.command == 'report':
            run_weekly_report()
        elif args.command == 'schedule':
            run_scheduler()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        logger = setup_logging('elt_runner')
        logger.error(f"Error running {args.command}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
