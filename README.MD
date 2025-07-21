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

### Key Components

**ELT Orchestrator** (`elt_orchestrator.py`)
- Schedules 15-minute extractions during market hours
- Coordinates Extract → Load → Transform operations
- Handles end-of-day processing and data quality checks

**Market Data Extractor** (`extract/market_data_extractor.py`)
- Fetches real-time and historical data from FMP API
- Handles rate limiting and API error recovery
- Validates and cleans incoming data

**Data Warehouse Loader** (`load/data_warehouse_loader.py`)
- Batch processing with staging tables
- Duplicate detection and handling
- Transaction management and rollback recovery

**Analytics Transformer** (`transform/analytics_transformer.py`)
- Technical indicators (SMA, EMA, MACD, RSI, Bollinger Bands)
- Market aggregations and performance metrics
- Daily/weekly/monthly rollups

**Data Quality Checker** (`quality/data_quality_checker.py`)
- Completeness, accuracy, and consistency validation
- Price and volume anomaly detection
- Timeliness monitoring

### Database Schema

**Market Data Tables**
- `stock_data` - 15-minute OHLCV for individual stocks
- `index_data` - 15-minute OHLCV for market indexes
- `commodity_data` - 15-minute OHLCV for commodities
- `bond_data` - Daily treasury rates

**Analytics Tables**
- `technical_indicators` - Technical analysis results
- `daily_aggregates` - Daily OHLCV rollups
- `market_summary` - Market-wide statistics

**Operations Tables**
- `elt_job_log` - Job execution tracking
- `data_quality_log` - Quality check results

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

## Monitoring & Troubleshooting

### Log Files
- `logs/elt_orchestrator.log` - Main scheduler and coordination
- `logs/market_data_extractor.log` - API extraction and errors
- `logs/data_warehouse_loader.log` - Database loading operations
- `logs/analytics_transformer.log` - Technical analysis calculations
- `logs/data_quality_checker.log` - Quality validation results

### Data Quality Checks
Run quality checks to validate system health:
```bash
python run_elt.py quality
```

Checks include:
- Data completeness (all symbols have recent data)
- Price validation (high/low/open/close relationships)
- Volume anomaly detection
- Data freshness and timeliness

### Common Issues

**No data being extracted**
- Verify API key in `.env` file
- Check if market is open (weekdays 9:30 AM - 4:00 PM ET)
- Review API rate limiting logs

**Database connection errors**
- Verify database credentials in `.env`
- Check if MySQL service is running
- Validate database exists and tables are created

**Missing technical indicators**
- Ensure minimum 200 periods of historical data
- Check for data gaps in source tables
- Review transformation logs for calculation errors

## Performance

- **Extraction Rate**: ~100 symbols in 15-20 seconds with rate limiting
- **Database Load**: ~10,000 records per minute with batch processing
- **Analytics**: Technical indicators for all symbols in under 1 minute
- **Storage**: ~50MB per day for 100+ symbols at 15-minute intervals

## Legacy Scripts (for reference)
The original collection scripts are still available:
- `collect_stocks.py`, `collect_indexes.py`, `collect_commodities.py`, `collect_bond.py`
- `run_collect.sh`, `run_createDB.sh`, `run_export.sh`

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review log files in the `logs/` directory
3. Run data quality checks to identify issues
4. See `.github/copilot-instructions.md` for AI agent guidance