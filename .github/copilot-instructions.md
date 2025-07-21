# AI Coding Agent Instructions for Market Data ELT Pipeline

## Project Overview
This is a real-time market data ELT (Extract, Load, Transform) pipeline that:
- Extracts 15-minute OHLCV data from Financial Modeling Prep (FMP) API every 15 minutes during market hours
- Loads data into MySQL data warehouse with staging and error handling
- Transforms data with technical indicators, aggregations, and analytics
- Monitors data quality and generates reports

## Architecture & Key Components

### Core ELT Flow
```
FMP API → Extractor → Loader → MySQL → Transformer → Analytics Tables
```

**Main Orchestrator**: `elt_orchestrator.py` - Schedules and coordinates all ELT operations
**Entry Point**: `run_elt.py` - CLI interface for manual operations

### Module Structure
- `extract/` - Data extraction from FMP API with rate limiting and error handling
- `load/` - Data warehouse loading with staging tables and batch processing  
- `transform/` - Technical indicators, aggregations, and analytics calculations
- `quality/` - Data validation, anomaly detection, and quality reporting
- `reporting/` - Weekly reports and performance summaries

### Database Schema
**Primary Tables**: `stock_data`, `index_data`, `commodity_data`, `bond_data`
**Analytics**: `technical_indicators`, `daily_aggregates`, `market_summary`
**Operations**: `elt_job_log`, `data_quality_log`
**Staging**: `*_staging` tables for bulk operations

## Development Patterns

### Configuration Management
All settings centralized in `config.py`:
- API endpoints and symbols lists
- ELT timing and batch sizes (15min intervals, 100 record batches)
- Market hours (9:30 AM - 4:00 PM ET)
- Database connection and retry logic

### Error Handling & Resilience
- Automatic retries with exponential backoff for API calls
- Database transactions with rollback on errors
- Rate limiting (0.1s between API calls, 1s between batches)
- Graceful degradation when data sources unavailable

### Data Processing Patterns
- **Index Aggregation**: 5-minute data aggregated to 15-minute (3 candles → 1 candle)
- **Technical Indicators**: 200-period lookback for proper MA/EMA calculations
- **Staging Pattern**: Load to staging tables first, then merge to production
- **Incremental Processing**: Only process new/changed data to minimize load

### Logging & Monitoring
- Module-specific loggers with file and console output
- Structured logging in `logs/` directory
- Data quality checks logged to `data_quality_log` table
- Job execution tracking in `elt_job_log` table

## Key Workflows

### Starting the System
```powershell
# Setup (first time)
.\setup_elt.ps1

# Start 15-minute scheduler
.\start_elt.ps1

# Manual operations
.\run_elt_ops.ps1 -Command extract
.\run_elt_ops.ps1 -Command backfill -StartDate 2024-01-01 -EndDate 2024-01-31
```

### Data Flow Debugging
1. Check `elt_job_log` table for execution history
2. Review logs in `logs/` directory by module
3. Run quality checks: `python run_elt.py quality`
4. Validate extraction: Query staging tables for recent data

### Adding New Symbols
1. Update symbol lists in `config.py` (STOCK_SYMBOLS, INDEX_SYMBOLS, etc.)
2. Restart scheduler to pick up new configuration
3. Run backfill for historical data if needed

## Critical Implementation Details

### Market Hours Logic
Uses `pytz` for Eastern Time calculations. Market open check in `utils.is_market_open()` accounts for weekends and market hours (9:30 AM - 4:00 PM ET).

### API Rate Limiting
FMP API limits enforced with `time.sleep()` calls:
- 0.1s between individual symbol requests
- 1s between symbol batches
- More conservative 0.2s for historical data requests

### Data Validation
Multi-layer validation in `quality/data_quality_checker.py`:
- Price validation (high >= max(open,close,low), etc.)
- Volume anomaly detection (3 standard deviations from 30-day average)
- Completeness checks (all expected symbols have recent data)
- Timeliness checks (data freshness within 2 hours)

### Technical Indicators
Calculated in `transform/analytics_transformer.py` using pandas:
- Moving averages (SMA 20/50/200, EMA 12/26)
- MACD with signal line and histogram
- RSI (14-period)
- Bollinger Bands (20-period, 2 std dev)

## Common Debugging Scenarios

**No data being extracted**: Check API key in `.env`, verify market hours, check rate limiting
**Database errors**: Verify connection in `config.py`, check table schemas match DDL
**Missing indicators**: Ensure minimum 200 periods of data for calculations
**Scheduler not running**: Check timezone settings, verify cron-like schedule logic
**Quality check failures**: Review specific check results, may indicate data source issues

## Testing Strategy
- Run `python run_elt.py quality` before major changes
- Use `python run_elt.py extract` to test API connectivity
- Backfill small date ranges to test historical data processing
- Monitor `elt_job_log` table for execution patterns and errors
