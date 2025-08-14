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

def main():
    loader = CSVDataWarehouseLoader()
    # Path to your CSV file
    csv_file = "data_extracts/stocks/stocks_2years.csv"
    # Load into stock_data_raw table
    result = loader._load_csv_to_table(loader.get_connection().__enter__().cursor(), csv_file, "stock_data_raw")
    print(result)

if __name__ == "__main__":
    main()