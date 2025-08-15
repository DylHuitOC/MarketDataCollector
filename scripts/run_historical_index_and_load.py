import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
from datetime import datetime, timedelta
from extract.index_extractor import IndexExtractor
from config import INDEX_SYMBOLS
from load.data_warehouse_loader import DataWarehouseLoader

if __name__ == "__main__":
    # Set time window for last day
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    extractor = IndexExtractor()
    # Aggregate to 15min intervals (default)
    index_data = extractor.extract_historical_data(INDEX_SYMBOLS, start_date, end_date, interval_minutes=15)

    # Save to CSV (optional, for logging)
    extractor.save_to_csv(index_data)

    # Load to database
    loader = DataWarehouseLoader()
    # Build dict mapping symbol to list of records
    index_records = {}
    for symbol, records in index_data.items():
        for record in records:
            record['symbol'] = symbol
        index_records[symbol] = records
    # Load to production table
    loader.load_extracted_data({'indexes': index_records}, index_table='index_data')
    print(f"Extracted, saved, and loaded index data for {start_date} to {end_date} into index_data.")
