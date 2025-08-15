-- Staging tables for bulk/incremental loads (with future key fields)
CREATE TABLE IF NOT EXISTS stock_data_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    open DECIMAL(12,4),
    low DECIMAL(12,4),
    high DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS index_data_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    open DECIMAL(12,4),
    low DECIMAL(12,4),
    high DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS commodity_data_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    open DECIMAL(12,4),
    low DECIMAL(12,4),
    high DECIMAL(12,4),
    close DECIMAL(12,4),
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

-- Metrics tables for transformed/aggregated data
CREATE TABLE IF NOT EXISTS stock_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    sma_20 DECIMAL(12,4),
    sma_50 DECIMAL(12,4),
    sma_200 DECIMAL(12,4),
    ema_12 DECIMAL(12,4),
    ema_26 DECIMAL(12,4),
    macd DECIMAL(12,4),
    macd_signal DECIMAL(12,4),
    macd_hist DECIMAL(12,4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS index_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    sma_20 DECIMAL(12,4),
    sma_50 DECIMAL(12,4),
    sma_200 DECIMAL(12,4),
    ema_12 DECIMAL(12,4),
    ema_26 DECIMAL(12,4),
    macd DECIMAL(12,4),
    macd_signal DECIMAL(12,4),
    macd_hist DECIMAL(12,4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

CREATE TABLE IF NOT EXISTS commodity_metrics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16),
    date DATETIME,
    sma_20 DECIMAL(12,4),
    sma_50 DECIMAL(12,4),
    sma_200 DECIMAL(12,4),
    ema_12 DECIMAL(12,4),
    ema_26 DECIMAL(12,4),
    macd DECIMAL(12,4),
    macd_signal DECIMAL(12,4),
    macd_hist DECIMAL(12,4),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol_date (symbol, date)
);

USE market_data_warehouse;

CREATE TABLE IF NOT EXISTS stock_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    datetime DATETIME NOT NULL,
    open DECIMAL(10,4) NOT NULL,
    high DECIMAL(10,4) NOT NULL,
    low DECIMAL(10,4) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_datetime (symbol, datetime),
    INDEX idx_datetime (datetime)
);

CREATE TABLE IF NOT EXISTS index_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    datetime DATETIME NOT NULL,
    open DECIMAL(10,4) NOT NULL,
    high DECIMAL(10,4) NOT NULL,
    low DECIMAL(10,4) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_datetime (symbol, datetime),
    INDEX idx_datetime (datetime)
);

CREATE TABLE IF NOT EXISTS commodity_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    datetime DATETIME NOT NULL,
    open DECIMAL(10,4) NOT NULL,
    high DECIMAL(10,4) NOT NULL,
    low DECIMAL(10,4) NOT NULL,
    close DECIMAL(10,4) NOT NULL,
    volume BIGINT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_datetime (symbol, datetime),
    INDEX idx_symbol_datetime (symbol, datetime),
    INDEX idx_datetime (datetime)
);
