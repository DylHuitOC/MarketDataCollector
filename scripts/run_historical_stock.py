import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from extract.stock_extractor import StockExtractor
from config import STOCK_SYMBOLS
from load.data_warehouse_loader import DataWarehouseLoader

def run_historical_stock_extraction(days_back: int = 5):
    """Extract recent historical 15-min stock data and load to DB (default last 5 days)."""
    extractor = StockExtractor()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    print(f"Extracting historical data for {len(STOCK_SYMBOLS)} symbols from {start_date.date()} to {end_date.date()}...")
    data = extractor.extract_historical_data(STOCK_SYMBOLS, start_date, end_date)
    csv_path = extractor.save_to_csv(data, filename=f"stocks_last{days_back}d.csv")
    # Prepare for loading
    loader = DataWarehouseLoader()
    # Convert to loader format (symbol -> list of dicts already)
    load_payload = {'stocks': data}
    load_result = loader.load_extracted_data(load_payload, stock_table='stock_data')
    print(f"Loaded stocks: {load_result['stocks']['records_loaded']} records (duplicates skipped: {load_result['stocks'].get('duplicates_skipped',0)})")
    if csv_path:
        print(f"CSV saved: {csv_path}")

if __name__ == "__main__":
    run_historical_stock_extraction()
