import sys
from datetime import datetime, timedelta
from extract.index_extractor import IndexExtractor
from config import INDEX_SYMBOLS

if __name__ == "__main__":
    # Set time window for last day
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    extractor = IndexExtractor()
    # Aggregate to 15min intervals (default)
    index_data = extractor.extract_historical_data(INDEX_SYMBOLS, start_date, end_date, interval_minutes=15)
    # Save to CSV
    extractor.save_to_csv(index_data)
    print(f"Extracted and saved index data for {start_date} to {end_date}.")
