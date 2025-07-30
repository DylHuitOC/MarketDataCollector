import mysql.connector
import logging
import pytz
import time
from datetime import datetime, timedelta, time as dt_time
from config import DB_CONFIG, ELT_CONFIG, MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE, MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE
import os

def get_db_connection():
    """Get database connection with automatic retries"""
    max_retries = ELT_CONFIG.get('max_retries', 3)
    retry_delay = ELT_CONFIG.get('retry_delay_seconds', 30)
    
    for attempt in range(max_retries):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except mysql.connector.Error as e:
            if attempt < max_retries - 1:
                logging.warning(f"Database connection attempt {attempt + 1} failed. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"Failed to connect to database after {max_retries} attempts: {e}")
                raise

def setup_logging(module_name, log_level=None):
    """Setup logging configuration"""
    if not log_level:
        log_level = ELT_CONFIG.get('log_level', 'INFO')
    
    # Create logs directory if it doesn't exist
    logs_dir = 'logs'
    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)
    
    # Configure logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # File handler
    file_handler = logging.FileHandler(f'{logs_dir}/{module_name}.log')
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    
    # Setup logger
    logger = logging.getLogger(module_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def is_market_open(check_time=None):
    """Check if the market is currently open (Eastern Time)"""
    if check_time is None:
        check_time = datetime.now()
    
    # Convert to Eastern Time
    eastern_tz = pytz.timezone(ELT_CONFIG['market_timezone'])
    if check_time.tzinfo is None:
        check_time = pytz.utc.localize(check_time)
    
    eastern_time = check_time.astimezone(eastern_tz)
    
    # Check if it's a weekday (0 = Monday, 6 = Sunday)
    if eastern_time.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Check if it's within market hours
    market_open = dt_time(MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE)
    market_close = dt_time(MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE)
    
    current_time = eastern_time.time()
    return market_open <= current_time <= market_close

def get_next_extraction_time():
    """Get the next scheduled extraction time"""
    now = datetime.now()
    eastern_tz = pytz.timezone(ELT_CONFIG['market_timezone'])
    
    if now.tzinfo is None:
        now = pytz.utc.localize(now)
    
    eastern_now = now.astimezone(eastern_tz)
    
    # Calculate next 15-minute interval
    interval_minutes = ELT_CONFIG['extract_interval_minutes']
    current_minute = eastern_now.minute
    next_interval = ((current_minute // interval_minutes) + 1) * interval_minutes
    
    if next_interval >= 60:
        next_time = eastern_now.replace(hour=eastern_now.hour + 1, minute=next_interval - 60, second=0, microsecond=0)
    else:
        next_time = eastern_now.replace(minute=next_interval, second=0, microsecond=0)
    
    return next_time

def get_market_calendar(start_date, end_date):
    """Get list of market trading days between dates (excludes weekends and major holidays)"""
    # Simplified version - excludes weekends only
    # In production, you'd want to integrate with a proper market calendar
    trading_days = []
    current_date = start_date
    
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            trading_days.append(current_date)
        current_date += timedelta(days=1)
    
    return trading_days

def batch_process(items, batch_size=None):
    """Generator that yields batches of items"""
    if batch_size is None:
        batch_size = ELT_CONFIG.get('batch_size', 100)
    
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]

def validate_symbol(symbol):
    """Validate if a symbol is properly formatted"""
    if not symbol:
        return False
    
    # Basic validation - adjust as needed
    return len(symbol) <= 10 and symbol.replace('^', '').replace('.', '').isalnum()

def get_lookback_date():
    """Get the date for looking back to check for missing data"""
    return datetime.now() - timedelta(days=ELT_CONFIG['lookback_days'])

def format_duration(seconds):
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"

def safe_float(value, default=0.0):
    """Safely convert value to float"""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    """Safely convert value to int"""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def get_table_name_for_symbol_type(symbol):
    """Determine which table a symbol belongs to"""
    from config import STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
    
    if symbol in STOCK_SYMBOLS:
        return 'stock_data'
    elif symbol in INDEX_SYMBOLS:
        return 'index_data'
    elif symbol in COMMODITY_SYMBOLS:
        return 'commodity_data'
    else:
        return 'unknown_data'
