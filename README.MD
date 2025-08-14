# Market Data ELT Pipeline

A production-ready ELT (Extract, Load, Transform) pipeline for real-time market data collection from Financial Modeling Prep API. Extracts 15-minute OHLCV data for stocks, indexes, and commodities, with automated technical analysis and data quality monitoring.

## Features

- **Real-time Data Collection**: 15-minute intervals during market hours
- **Comprehensive Coverage**: Stocks, indexes, commodities, and treasury bonds
- **Technical Analysis**: Automated calculation of indicators (SMA, EMA, MACD, RSI, Bollinger Bands)
- **Data Quality Monitoring**: Automated validation and anomaly detection
- **Robust Error Handling**: Retry logic, rate limiting, and graceful degradation
- **Production Monitoring**: Logging, job tracking, and weekly reports

## Quick Start

### Prerequisites
- Python 3.8+
- MySQL 5.7+
- Financial Modeling Prep API key

### Setup
1. Clone and navigate to the project:
```bash
cd MarketDataCollector
```

2. Run the setup script:
```powershell
.\setup_elt.ps1
```

3. Configure your API key and database in `.env`:
```env
API_KEY=your_fmp_api_key_here
DB_HOST=localhost
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=market_data
REAL_TIME_ENABLED=true
```

4. Start the ELT scheduler:
```powershell
.\start_elt.ps1
```

## Usage

### Manual Operations
```powershell
# Extract current market data
.\run_elt_ops.ps1 -Command extract

# Backfill historical data
.\run_elt_ops.ps1 -Command backfill -StartDate 2024-01-01 -EndDate 2024-01-31

# Run analytics transformations
.\run_elt_ops.ps1 -Command transform

# Check data quality
.\run_elt_ops.ps1 -Command quality

# Generate weekly report
.\run_elt_ops.ps1 -Command report

# Run complete ELT process once
.\run_elt_ops.ps1 -Command full
```

### Python CLI
```bash
# Start scheduler (runs every 15 minutes during market hours)
python run_elt.py schedule

# Manual operations
python run_elt.py extract
python run_elt.py backfill --start-date 2024-01-01 --end-date 2024-01-31
python run_elt.py transform
python run_elt.py quality
python run_elt.py full
python run_elt.py report
```

## Architecture

### Data Flow
```
FMP API → Extractor → Staging Tables → Data Warehouse → Analytics → Reports
```

## Configuration

### Symbols
Edit `config.py` to modify tracked symbols:
```python
STOCK_SYMBOLS = ['AAPL', 'MSFT', 'GOOGL', ...]
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC']
COMMODITY_SYMBOLS = ['GCUSD', 'CLUSD']
```

### ELT Settings
```python
ELT_CONFIG = {
    'extract_interval_minutes': 15,
    'batch_size': 100,
    'max_retries': 3,
    'retry_delay_seconds': 30,
    'lookback_days': 7,
    'market_timezone': 'US/Eastern'
}
```
