## Market Data Collector (Simplified Overview)

This project pulls intraday OHLCV data from the Financial Modeling Prep (FMP) API and stores it in MySQL.
It supports stocks, indexes, and commodities. Indexes and commodities use 5‑minute source data that we aggregate into custom 15‑minute bars (special handling: a standalone 9:30 bar, then 3×5‑min groups ending on :45 / :00 / :15 / :30). Stocks are fetched directly at 15‑minute intervals. Data is written to tables: `stock_data`, `index_data`, `commodity_data` (datetime‑based, no separate date column).

### Core Flow
```
FMP API → Extractors → (optional CSV) → Loader → MySQL
```

### Key Extractors
- `extract/stock_extractor.py` (15min endpoint)  
- `extract/index_extractor.py` (5min → custom 15min aggregation)  
- `extract/commodity_extractor.py` (5min → custom 15min aggregation + market hours filter)  

### What Gets Stored
Each bar: symbol, datetime (bar close timestamp), open, high, low, close, volume.  
For aggregated series (indexes, commodities) the datetime corresponds to the last 5‑min bar in the 15‑min set (e.g. 09:45, 10:00...).

### Setup (Quick)
1. Install dependencies: `pip install -r requirements.txt`  
2. Create `.env` with at least:  
    `API_KEY=your_fmp_key`  
    DB connection vars (`DB_HOST`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`).  
3. Initialize database (run your existing init script / container).  

### Running Individual Extractions
Stocks (recent historical example):
```
python scripts/run_historical_stock.py
```

Indexes (recent historical 5→15 aggregation):
```
python scripts/run_historical_index_and_load.py
```

Commodities (recent historical 5→15 aggregation):
```
python scripts/run_historical_commodity.py
```

Current (single cycle) multi‑asset extraction + CSV + load (if implemented):
```
python scripts/run_elt.py extract
```

Scheduled loop (orchestrator):
```
python scripts/run_elt.py schedule
```

### Output Files
CSV files land under `data_extracts/<asset_type>/` with timestamped filenames. They include a `symbol` column. Aggregated sets may add a helper field `last_5min_datetime` for traceability.

### Adjusting Symbols
Edit `config.py` lists: `STOCK_SYMBOLS`, `INDEX_SYMBOLS`, `COMMODITY_SYMBOLS`. Restart any running scheduler for changes to take effect.

### Notes / Conventions
- Index & commodity bars: open = first 5‑min open in set; close = last 5‑min close; high/low/volume aggregated across the 3 bars (post‑9:30).  
- Data quality / analytics transformers exist but are optional for basic extraction.  
- Duplicate protection uses (symbol, datetime) unique key.  

### Minimal Troubleshooting
- Empty results: confirm market hours and API key.  
- NULL datetimes: schema mismatch—reinitialize tables.  
- Duplicate skips: expected when re‑running overlapping windows.  

### Next Steps (Optional)
- Add technical indicators (transform step).  
- Add weekly reporting.  
- Add Windows Task Scheduler entry to run the schedule command at startup.  

---
This simplified README focuses on the essentials: what it does, how aggregation works, and how to run the main extraction scripts.
