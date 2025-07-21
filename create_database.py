import mysql.connector
from config import DB_CONFIG

# Extract DB name because it's inside DB_CONFIG
DB_NAME = DB_CONFIG['database']

# Connect to MySQL server (without selecting database yet)
conn = mysql.connector.connect(
    host=DB_CONFIG['host'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password']
)
cursor = conn.cursor()

# Create database if it doesn't exist
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
print(f"Database '{DB_NAME}' is ready!")

# Connect to the newly created database
conn.database = DB_NAME

# Table creation queries
TABLES = {}

# Original market data tables
TABLES['stock_data'] = (
    """
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATETIME NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        volume INT NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_stock_symbol_date (symbol, date),
        INDEX idx_stock_date (date),
        INDEX idx_stock_symbol (symbol)
    )
    """
)

TABLES['bond_data'] = (
    """
    CREATE TABLE IF NOT EXISTS bond_data (
        date DATE NOT NULL PRIMARY KEY,
        month1 DECIMAL(5, 2) NOT NULL,
        month2 DECIMAL(5, 2) NOT NULL,
        month3 DECIMAL(5, 2) NOT NULL,
        month6 DECIMAL(5, 2) NOT NULL,
        year1 DECIMAL(5, 2) NOT NULL,
        year2 DECIMAL(5, 2) NOT NULL,
        year3 DECIMAL(5, 2) NOT NULL,
        year5 DECIMAL(5, 2) NOT NULL,
        year7 DECIMAL(5, 2) NOT NULL,
        year10 DECIMAL(5, 2) NOT NULL,
        year20 DECIMAL(5, 2) NOT NULL,
        year30 DECIMAL(5, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
)

TABLES['index_data'] = (
    """
    CREATE TABLE IF NOT EXISTS index_data (
        date DATETIME NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        open DECIMAL(12, 2) NOT NULL,
        low DECIMAL(12, 2) NOT NULL,
        high DECIMAL(12, 2) NOT NULL,
        close DECIMAL(12, 2) NOT NULL,
        volume BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, symbol),
        INDEX idx_index_symbol_date (symbol, date),
        INDEX idx_index_date (date)
    )
    """
)

TABLES['commodity_data'] = (
    """
    CREATE TABLE IF NOT EXISTS commodity_data (
        date DATETIME NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        open DECIMAL(12, 4) NOT NULL,
        low DECIMAL(12, 4) NOT NULL,
        high DECIMAL(12, 4) NOT NULL,
        close DECIMAL(12, 4) NOT NULL,
        volume BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, symbol),
        INDEX idx_commodity_symbol_date (symbol, date),
        INDEX idx_commodity_date (date)
    )
    """
)

# New ELT process tables

TABLES['technical_indicators'] = (
    """
    CREATE TABLE IF NOT EXISTS technical_indicators (
        symbol VARCHAR(10) NOT NULL,
        table_type VARCHAR(20) NOT NULL,
        date DATETIME NOT NULL,
        sma_20 DECIMAL(12, 4),
        sma_50 DECIMAL(12, 4),
        sma_200 DECIMAL(12, 4),
        ema_12 DECIMAL(12, 4),
        ema_26 DECIMAL(12, 4),
        macd DECIMAL(12, 6),
        macd_signal DECIMAL(12, 6),
        macd_histogram DECIMAL(12, 6),
        rsi DECIMAL(5, 2),
        bb_upper DECIMAL(12, 4),
        bb_middle DECIMAL(12, 4),
        bb_lower DECIMAL(12, 4),
        volume_sma_20 BIGINT,
        volume_ratio DECIMAL(8, 4),
        price_change_1d DECIMAL(8, 6),
        price_change_5d DECIMAL(8, 6),
        price_change_20d DECIMAL(8, 6),
        volatility_20d DECIMAL(8, 6),
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, table_type, date),
        INDEX idx_symbol_date (symbol, date),
        INDEX idx_updated_at (updated_at),
        INDEX idx_table_type (table_type)
    )
    """
)

TABLES['daily_aggregates'] = (
    """
    CREATE TABLE IF NOT EXISTS daily_aggregates (
        date DATE NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        table_type VARCHAR(20) NOT NULL,
        open DECIMAL(12, 4) NOT NULL,
        high DECIMAL(12, 4) NOT NULL,
        low DECIMAL(12, 4) NOT NULL,
        close DECIMAL(12, 4) NOT NULL,
        volume BIGINT NOT NULL,
        vwap DECIMAL(12, 4),
        trade_count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (date, symbol, table_type),
        INDEX idx_daily_symbol_date (symbol, date),
        INDEX idx_daily_date (date),
        INDEX idx_daily_table_type (table_type)
    )
    """
)

TABLES['market_summary'] = (
    """
    CREATE TABLE IF NOT EXISTS market_summary (
        date DATE NOT NULL PRIMARY KEY,
        total_symbols INT,
        advancing_stocks INT,
        declining_stocks INT,
        unchanged_stocks INT,
        new_highs INT,
        new_lows INT,
        total_volume BIGINT,
        market_cap DECIMAL(20, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    """
)

TABLES['elt_job_log'] = (
    """
    CREATE TABLE IF NOT EXISTS elt_job_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        job_type VARCHAR(50) NOT NULL,
        start_time DATETIME NOT NULL,
        end_time DATETIME,
        status VARCHAR(20) NOT NULL,
        records_processed INT DEFAULT 0,
        error_message TEXT,
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_job_type_start_time (job_type, start_time),
        INDEX idx_status (status),
        INDEX idx_start_time (start_time)
    )
    """
)

TABLES['data_quality_log'] = (
    """
    CREATE TABLE IF NOT EXISTS data_quality_log (
        id INT AUTO_INCREMENT PRIMARY KEY,
        check_date DATETIME NOT NULL,
        check_type VARCHAR(50) NOT NULL,
        status VARCHAR(20) NOT NULL,
        message TEXT,
        details JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_check_date (check_date),
        INDEX idx_check_type (check_type),
        INDEX idx_status (status)
    )
    """
)

# Staging tables for bulk operations
TABLES['stock_data_staging'] = (
    """
    CREATE TABLE IF NOT EXISTS stock_data_staging (
        date DATETIME NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        volume INT NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_staging_date_symbol (date, symbol),
        INDEX idx_staging_load_timestamp (load_timestamp)
    )
    """
)

TABLES['index_data_staging'] = (
    """
    CREATE TABLE IF NOT EXISTS index_data_staging (
        date DATETIME NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        open DECIMAL(12, 2) NOT NULL,
        low DECIMAL(12, 2) NOT NULL,
        high DECIMAL(12, 2) NOT NULL,
        close DECIMAL(12, 2) NOT NULL,
        volume BIGINT NOT NULL,
        load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_staging_date_symbol (date, symbol),
        INDEX idx_staging_load_timestamp (load_timestamp)
    )
    """
)

# Execute each table creation
for table_name, ddl in TABLES.items():
    try:
        cursor.execute(ddl)
        print(f"Table '{table_name}' is ready!")
    except mysql.connector.Error as err:
        print(f"Error creating table {table_name}: {err}")

# Create views for common queries
VIEWS = {}

VIEWS['latest_stock_prices'] = """
    CREATE OR REPLACE VIEW latest_stock_prices AS
    SELECT DISTINCT 
        symbol,
        FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY date DESC) as latest_price,
        FIRST_VALUE(date) OVER (PARTITION BY symbol ORDER BY date DESC) as latest_date,
        FIRST_VALUE(volume) OVER (PARTITION BY symbol ORDER BY date DESC) as latest_volume
    FROM stock_data
    WHERE date >= CURDATE() - INTERVAL 1 DAY
"""

VIEWS['market_movers'] = """
    CREATE OR REPLACE VIEW market_movers AS
    WITH daily_change AS (
        SELECT 
            symbol,
            close as current_price,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) as prev_price,
            date
        FROM stock_data
        WHERE date >= CURDATE() - INTERVAL 2 DAY
    )
    SELECT 
        symbol,
        current_price,
        prev_price,
        ((current_price - prev_price) / prev_price * 100) as change_percent,
        date
    FROM daily_change
    WHERE prev_price IS NOT NULL
    AND date >= CURDATE()
    ORDER BY ABS(change_percent) DESC
"""

# Create views
for view_name, view_sql in VIEWS.items():
    try:
        cursor.execute(view_sql)
        print(f"View '{view_name}' is ready!")
    except mysql.connector.Error as err:
        print(f"Error creating view {view_name}: {err}")

# Clean up
cursor.close()
conn.close()
print("✅ All tables and views created successfully.")
print("")
print("ELT Database Schema Summary:")
print("- Market Data Tables: stock_data, index_data, commodity_data, bond_data")
print("- Analytics Tables: technical_indicators, daily_aggregates, market_summary")
print("- Operations Tables: elt_job_log, data_quality_log")
print("- Staging Tables: stock_data_staging, index_data_staging")
print("- Views: latest_stock_prices, market_movers")
print("")
print("Ready for ELT operations!")
