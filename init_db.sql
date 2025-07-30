# MySQL Database Setup Script
# This script will be run automatically when the MySQL container starts

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

-- Production tables
CREATE TABLE IF NOT EXISTS stock_data LIKE stock_data_raw;
CREATE TABLE IF NOT EXISTS index_data LIKE index_data_raw;
CREATE TABLE IF NOT EXISTS commodity_data LIKE commodity_data_raw;
CREATE TABLE IF NOT EXISTS bond_data LIKE bond_data_raw;

-- Analytics tables
CREATE TABLE IF NOT EXISTS technical_indicators (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    datetime DATETIME,
    indicator_name VARCHAR(32),
    value DECIMAL(18,6)
);

CREATE TABLE IF NOT EXISTS daily_aggregates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATE,
    open DECIMAL(18,6),
    high DECIMAL(18,6),
    low DECIMAL(18,6),
    close DECIMAL(18,6),
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS market_summary (
    id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    market VARCHAR(32),
    summary JSON
);
