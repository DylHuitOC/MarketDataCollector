from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# API Key
API_KEY = os.getenv('API_KEY')

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Date Range Configuration
FROM_DATE = '2024-06-06'
TO_DATE = '2025-06-06'

# Symbols
STOCK_SYMBOLS = ['AAPL', 'AMD', 'AMZN', 'BA', 'BABA', 'BAC', 'C', 'CSCO', 'CVX', 'DIS', 'F', 'GE',
          'GOOGL', 'IBM', 'INTC', 'JNJ', 'JPM', 'KO', 'MCD', 'META', 'MSFT', 'NFLX', 'NVDA',
          'PFE', 'T', 'TSLA', 'VZ', 'WMT', 'XOM']
INDEX_SYMBOLS = ['^GSPC', '^DJI', '^IXIC']  # S&P 500, Dow Jones, Nasdaq
COMMODITY_SYMBOLS = ['GCUSD', 'CLUSD']  # Gold, Oil
