from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pytz

# Load environment variables
load_dotenv()

# API Key
API_KEY = os.getenv('API_KEY')

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': int(os.getenv('DB_PORT', 3307))
}

# ELT Process Configuration
ELT_CONFIG = {
    'extract_interval_minutes': 15,
    'batch_size': 100,
    'max_retries': 3,
    'retry_delay_seconds': 30,
    'lookback_days': 7,  # How many days to look back for data updates
    'market_timezone': 'US/Eastern',
    'log_level': 'INFO'
}

# Market Hours (Eastern Time)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MINUTE = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MINUTE = 0

# Date Range Configuration (for historical backfill)
FROM_DATE = '2024-06-06'
TO_DATE = '2025-06-06'

# Real-time data configuration
REAL_TIME_ENABLED = os.getenv('REAL_TIME_ENABLED', 'true').lower() == 'true'
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')

# Symbols
STOCK_SYMBOLS = ['AAPL']#, 'AMD', 'AMZN', 'BA', 'BABA', 'BAC', 'C', 'CSCO', 'CVX', 'DIS', 'F', 'GE',
#          'GOOGL', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD', 'META', 'MSFT', 'NFLX', 'NVDA',
#          'PFE', 'T', 'TSLA', 'VZ', 'WMT', 'XOM']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC']  # S&P 500, Dow Jones, Nasdaq
COMMODITY_SYMBOLS = ['GCUSD', 'CLUSD']  # Gold, Oil

# API Configuration
FMP_BASE_URL = 'https://financialmodelingprep.com/stable'
API_ENDPOINTS = {
    'historical_15min': f'{FMP_BASE_URL}/historical-chart/15min',
    'historical_5min': f'{FMP_BASE_URL}/historical-chart/5min',
    'historical_1min': f'{FMP_BASE_URL}/historical-chart/1min',
    'treasury_rates': f'{FMP_BASE_URL}/treasury',
    'real_time_quote': f'{FMP_BASE_URL}/quote'
}
