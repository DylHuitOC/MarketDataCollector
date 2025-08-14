import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from extract import StockExtractor
from config import STOCK_SYMBOLS

# Pull all historical data for 29 stock symbols for the last 2 years

def run_historical_stock_extraction():
    extractor = StockExtractor()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)  # Last 2 years
    print(f"Extracting historical data for {len(STOCK_SYMBOLS)} symbols from {start_date.date()} to {end_date.date()}...")
    data = extractor.extract_historical_data(STOCK_SYMBOLS, start_date, end_date)
    extractor.save_to_csv(data, filename="stocks_2years.csv")
    print("Extraction complete. Data saved to stocks_2years.csv.")

if __name__ == "__main__":
    run_historical_stock_extraction()
