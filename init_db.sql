# MySQL Database Setup Script
# This script will be run automatically when the MySQL container starts
# 
# Schema Design:
# - Supports simplified extractor architecture with specialized extractors
# - Raw tables for CSV import staging
# - Production tables with proper constraints and indexes  
# - Analytics tables for technical indicators and aggregations
# - Logging tables for ELT job tracking and data quality monitoring
#
# Updated: July 2025 to support simplified extractor architecture

USE market_data_warehouse;

-- Grant necessary privileges
GRANT ALL PRIVILEGES ON market_data_warehouse.* TO 'market_user'@'%';
FLUSH PRIVILEGES;

-- Create initial logging table
CREATE TABLE IF NOT EXISTS elt_job_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL,
    job_type VARCHAR(50) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    status ENUM('running', 'completed', 'failed') DEFAULT 'running',
    records_processed INT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_job_type (job_type),
    INDEX idx_status (status),
    INDEX idx_start_time (start_time)
);

-- Create data quality log table
CREATE TABLE IF NOT EXISTS data_quality_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(100) NOT NULL,
    check_date DATE NOT NULL,
    passed BOOLEAN NOT NULL,
    message TEXT,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_check_name (check_name),
    INDEX idx_check_date (check_date),
    INDEX idx_passed (passed)
);

SELECT 'Database initialized successfully' as result;

-- Raw data tables
CREATE TABLE IF NOT EXISTS stock_data_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS index_data_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS commodity_data_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bond_data_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging tables
CREATE TABLE IF NOT EXISTS stock_data_staging LIKE stock_data_raw;
CREATE TABLE IF NOT EXISTS index_data_staging LIKE index_data_raw;
CREATE TABLE IF NOT EXISTS commodity_data_staging LIKE commodity_data_raw;
CREATE TABLE IF NOT EXISTS bond_data_staging LIKE bond_data_raw;

-- Production tables with proper indexes
CREATE TABLE IF NOT EXISTS stock_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_date (symbol, date),
    INDEX idx_datetime (datetime)
);

CREATE TABLE IF NOT EXISTS index_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_date (symbol, date),
    INDEX idx_datetime (datetime)
);

CREATE TABLE IF NOT EXISTS commodity_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_date (symbol, date),
    INDEX idx_datetime (datetime)
);

CREATE TABLE IF NOT EXISTS bond_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    date DATE,
    rate DECIMAL(8,4),
    yield_1m DECIMAL(8,4),
    yield_3m DECIMAL(8,4),
    yield_6m DECIMAL(8,4),
    yield_1y DECIMAL(8,4),
    yield_2y DECIMAL(8,4),
    yield_5y DECIMAL(8,4),
    yield_10y DECIMAL(8,4),
    yield_20y DECIMAL(8,4),
    yield_30y DECIMAL(8,4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_datetime (datetime),
    INDEX idx_date (date)
);

-- Analytics tables
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
    INDEX idx_updated_at (updated_at)
);

CREATE TABLE IF NOT EXISTS daily_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS market_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    market VARCHAR(32),
    total_symbols INT,
    avg_volume BIGINT,
    avg_price_change DECIMAL(8, 6),
    top_gainers JSON,
    top_losers JSON,
    market_breadth JSON,
    summary JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_date_market (date, market),
    INDEX idx_date (date)
);
