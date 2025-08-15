import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime, timedelta
from extract.commodity_extractor import CommodityExtractor
from config import COMMODITY_SYMBOLS
from load.data_warehouse_loader import DataWarehouseLoader


def run_historical_commodity_extraction(days_back: int = 5):
    """Extract recent historical 15-min commodity data (filtered to market hours) and load to DB."""
    extractor = CommodityExtractor()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    print(f"Extracting commodity data for {len(COMMODITY_SYMBOLS)} symbols from {start_date.date()} to {end_date.date()}...")
    data = extractor.extract_historical_data(COMMODITY_SYMBOLS, start_date, end_date)
    csv_path = extractor.save_to_csv(data, filename=f"commodities_last{days_back}d.csv")
    loader = DataWarehouseLoader()
    load_payload = {'commodities': data}
    load_result = loader.load_extracted_data(load_payload, commodity_table='commodity_data')
    print(f"Loaded commodities: {load_result['commodities']['records_loaded']} records (duplicates skipped: {load_result['commodities'].get('duplicates_skipped',0)})")
    if csv_path:
        print(f"CSV saved: {csv_path}")

if __name__ == "__main__":
    run_historical_commodity_extraction()
