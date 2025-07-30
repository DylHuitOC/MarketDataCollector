# Demo Script for Market Data ELT Pipeline

## Pre-Demo Setup (Do this beforehand)
```bash
# Ensure database is running
./start_database.sh

# Initialize tables if not done
./setup_database.sh

# Verify API key is set in .env
cat .env | grep API_KEY
```

## Demo Flow (15-20 minutes)

### 1. Introduction (2 minutes)
**"I've transformed our basic market data collection into a production-ready ELT pipeline."**

Show the project structure:
```bash
ls -la
echo "Key improvements: Automated scheduling, dual data warehouses, technical indicators, and data quality monitoring."
```

### 2. Architecture Overview (3 minutes)
**"Let me show you the data flow architecture."**

Open `TECHNICAL_REVIEW_GUIDE.md` and explain:
- CSV-first extraction for data integrity
- Raw warehouse (DW1) for historical data
- Analytics warehouse (DW2) for enhanced data
- Automated 15-minute scheduling

### 3. Live Data Extraction Demo (4 minutes)
**"Let's extract live market data right now."**

```bash
# Extract current market data
echo "Extracting real-time market data..."
./run_elt_ops.sh -c extract

# Show generated CSV files
echo "Data extracted to CSV files:"
ls -la data_extracts/*/

# Show sample data
echo "Sample stock data:"
head -5 data_extracts/stocks/*.csv
```

### 4. Database Loading Demo (3 minutes)
**"Now let's load this into our data warehouses."**

```bash
# Load CSV to raw warehouse
echo "Loading to raw data warehouse (DW1)..."
./run_elt_ops.sh -c csv-load

# Transform to analytics warehouse
echo "Transforming to analytics warehouse (DW2)..."
./run_elt_ops.sh -c transform
```

### 5. Analytics Showcase (4 minutes)
**"Here's where the real value is - enhanced analytics."**

```bash
# Connect to database and show results
docker exec -it market_data_mysql mysql -u market_user -pmarket_pass market_data_warehouse
```

```sql
-- Show raw data
SELECT symbol, date, close, volume FROM stock_data_raw ORDER BY date DESC LIMIT 5;

-- Show enhanced analytics
SELECT symbol, date, close, price_change_pct, volatility, data_quality_score 
FROM stock_data ORDER BY date DESC LIMIT 5;

-- Show technical indicators
SELECT symbol, date, close, sma_20, rsi_14, macd_line 
FROM technical_indicators 
WHERE symbol = 'AAPL' ORDER BY date DESC LIMIT 3;

-- Show market summary
SELECT date, sp500_close, sp500_change_pct, market_sentiment 
FROM market_summary ORDER BY date DESC LIMIT 3;

-- Exit database
exit
```

### 6. Data Quality & Monitoring (2 minutes)
**"The system includes comprehensive monitoring."**

```bash
# Run quality checks
echo "Running data quality checks..."
./run_elt_ops.sh -c quality

# Show logs
echo "System logs:"
ls -la logs/
tail -10 logs/elt_*.log
```

### 7. Full Pipeline Demo (2 minutes)
**"Here's the complete pipeline in one command."**

```bash
# Run complete ELT pipeline
echo "Running full ELT pipeline..."
./run_elt_ops.sh -c full
```

### 8. Production Features (1 minute)
**"Ready for production deployment."**

Show:
```bash
# Cross-platform scripts
ls *.sh *.ps1

# Docker setup
cat docker-compose.yml

# Configuration management
head -20 config.py
```

## Key Demo Points to Emphasize

### 🎯 **Business Value**
- "Reduces manual work from 100% to less than 5%"
- "Real-time data every 15 minutes during market hours"
- "Automatic error recovery and data validation"
- "Technical indicators calculated automatically"

### 💻 **Technical Excellence**
- "Modular design - easy to add new data sources"
- "Dual warehouse architecture for data integrity"
- "Production-ready monitoring and logging"
- "Cross-platform - works on Windows and Linux"

### 🚀 **Production Ready**
- "Three commands to deploy: start database, setup tables, start scheduler"
- "Built-in rate limiting respects API constraints"
- "Comprehensive error handling and recovery"
- "Docker containerization for consistent deployment"

## Questions to Ask

1. **"What additional data sources would be valuable?"**
2. **"How does this fit with your existing infrastructure?"**
3. **"What's your preferred deployment timeline?"**
4. **"Who would be the primary users of this analytics data?"**
5. **"Any specific technical concerns or requirements?"**

## Backup Demo (If Live Demo Fails)

Show pre-captured outputs:
```bash
# Show sample CSV files (prepared beforehand)
cat sample_outputs/stocks_sample.csv

# Show sample database queries (screenshots)
# Show sample logs and monitoring outputs
```

## Closing

**"This pipeline is ready for production deployment. The next steps would be:**
1. **Choose deployment environment**
2. **Set up monitoring dashboards**  
3. **Configure any additional data sources**
4. **Train the team on operations**

**Any questions about the technical implementation or business capabilities?"**
