"""
ELT Process Runner
Simple script to run different ELT operations
"""

import sys
import argparse
from datetime import datetime, timedelta

from elt_orchestrator import ELTOrchestrator
from extract.market_data_extractor import MarketDataExtractor
from load.data_warehouse_loader import DataWarehouseLoader
from transform.analytics_transformer import AnalyticsTransformer
from quality.data_quality_checker import DataQualityChecker
from reporting.weekly_reporter import WeeklyReporter
from utils import setup_logging

def run_extraction():
    """Run data extraction only"""
    logger = setup_logging('elt_runner')
    logger.info("Running extraction process...")
    
    extractor = MarketDataExtractor()
    extracted_data = extractor.extract_all_current_data()
    
    if extracted_data:
        loader = DataWarehouseLoader()
        result = loader.load_extracted_data(extracted_data)
        logger.info(f"Extraction completed. Results: {result}")
    else:
        logger.warning("No data extracted")

def run_backfill(start_date_str, end_date_str):
    """Run historical data backfill"""
    logger = setup_logging('elt_runner')
    logger.info(f"Running backfill from {start_date_str} to {end_date_str}")
    
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    
    orchestrator = ELTOrchestrator()
    orchestrator.backfill_missing_data(start_date, end_date)

def run_transforms():
    """Run analytics transformations"""
    logger = setup_logging('elt_runner')
    logger.info("Running transformations...")
    
    transformer = AnalyticsTransformer()
    transformer.run_incremental_transforms()
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

def run_full_elt():
    """Run complete ELT process once"""
    logger = setup_logging('elt_runner')
    logger.info("Running full ELT process...")
    
    orchestrator = ELTOrchestrator()
    orchestrator.extract_load_transform()
    logger.info("Full ELT process completed")

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
        'extract', 'backfill', 'transform', 'quality', 'full', 'report', 'schedule'
    ], help='Command to run')
    parser.add_argument('--start-date', help='Start date for backfill (YYYY-MM-DD)')
    parser.add_argument('--end-date', help='End date for backfill (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        if args.command == 'extract':
            run_extraction()
        elif args.command == 'backfill':
            if not args.start_date or not args.end_date:
                print("Backfill requires --start-date and --end-date")
                sys.exit(1)
            run_backfill(args.start_date, args.end_date)
        elif args.command == 'transform':
            run_transforms()
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
