import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from datetime import datetime, timedelta

import time
from extract import StockExtractor
from config import STOCK_SYMBOLS

def run_stock_extraction():
    extractor = StockExtractor()
    while True:
        print("Starting stock data extraction...")
        now = datetime.now()
        start_date = now - timedelta(minutes=15)
        # Pass start_date and now to the extractor
        extractor.extract_current_data(STOCK_SYMBOLS, start_date, now)
        print("Extraction complete. Waiting 15 minutes...")
        time.sleep(15 * 60)  # Wait 15 minutes

if __name__ == "__main__":
    run_stock_extraction()