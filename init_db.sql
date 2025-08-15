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

-- =============================================================
-- DATA WAREHOUSE STAR SCHEMA EXTENSION (NEW LAYER)
-- Keeps legacy operational tables above intact.
-- Add dimensions + unified fact tables for analytics & BI.
-- =============================================================

-- Dimension: Symbol (one row per tradable instrument)
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL UNIQUE,
    asset_type ENUM('STOCK','INDEX','COMMODITY') NOT NULL,
    name VARCHAR(128) NULL,
    currency VARCHAR(8) NULL,
    first_trade_date DATE NULL,
    active_flag TINYINT(1) DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_asset_type (asset_type)
);

-- Dimension: Calendar (supports date-based slicing)
CREATE TABLE IF NOT EXISTS dim_calendar (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    month_name VARCHAR(12) NOT NULL,
    day TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL, -- 1=Monday .. 7=Sunday
    day_name VARCHAR(12) NOT NULL,
    is_weekend TINYINT(1) NOT NULL DEFAULT 0,
    is_market_holiday TINYINT(1) NOT NULL DEFAULT 0,
    trading_day_index INT NULL, -- sequential index of trading days
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_year_month (year, month),
    INDEX idx_trading_day (trading_day_index)
);

-- Dimension: Time Interval (optional granular intraday slot metadata)
CREATE TABLE IF NOT EXISTS dim_time_interval (
    interval_id INT AUTO_INCREMENT PRIMARY KEY,
    minutes INT NOT NULL UNIQUE,
    label VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Unified intraday OHLCV (all asset types)
CREATE TABLE IF NOT EXISTS fact_intraday_price (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    datetime DATETIME NOT NULL,
    date_id INT NOT NULL,
    interval_minutes INT NOT NULL DEFAULT 15,
    open DECIMAL(12,4) NOT NULL,
    high DECIMAL(12,4) NOT NULL,
    low DECIMAL(12,4) NOT NULL,
    close DECIMAL(12,4) NOT NULL,
    volume BIGINT NULL,
    source VARCHAR(32) DEFAULT 'FMP',
    load_batch_id BIGINT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_symbol_datetime_interval (symbol_id, datetime, interval_minutes),
    KEY idx_symbol_datetime (symbol_id, datetime),
    KEY idx_date (date_id),
    CONSTRAINT fk_price_symbol FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    CONSTRAINT fk_price_date FOREIGN KEY (date_id) REFERENCES dim_calendar(date_id)
);

-- Fact: Daily aggregates (derived from intraday or direct daily feed)
CREATE TABLE IF NOT EXISTS fact_daily_price (
    daily_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    date_id INT NOT NULL,
    open DECIMAL(12,4) NOT NULL,
    high DECIMAL(12,4) NOT NULL,
    low DECIMAL(12,4) NOT NULL,
    close DECIMAL(12,4) NOT NULL,
    volume BIGINT NULL,
    return_close DECIMAL(12,6) NULL,
    range_pct DECIMAL(12,6) NULL, -- (high-low)/open
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_symbol_date (symbol_id, date_id),
    KEY idx_symbol_date (symbol_id, date_id),
    CONSTRAINT fk_daily_symbol FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    CONSTRAINT fk_daily_date FOREIGN KEY (date_id) REFERENCES dim_calendar(date_id)
);

-- Fact: Technical indicators (intraday anchor)
CREATE TABLE IF NOT EXISTS fact_indicator (
    indicator_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol_id INT NOT NULL,
    datetime DATETIME NOT NULL,
    date_id INT NOT NULL,
    interval_minutes INT NOT NULL DEFAULT 15,
    sma_20 DECIMAL(14,6) NULL,
    sma_50 DECIMAL(14,6) NULL,
    sma_200 DECIMAL(14,6) NULL,
    ema_12 DECIMAL(14,6) NULL,
    ema_26 DECIMAL(14,6) NULL,
    macd DECIMAL(14,6) NULL,
    macd_signal DECIMAL(14,6) NULL,
    macd_hist DECIMAL(14,6) NULL,
    rsi_14 DECIMAL(14,6) NULL,
    bb_mid DECIMAL(14,6) NULL,
    bb_upper DECIMAL(14,6) NULL,
    bb_lower DECIMAL(14,6) NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_symbol_datetime_interval_indicator (symbol_id, datetime, interval_minutes),
    KEY idx_symbol_datetime_indicator (symbol_id, datetime),
    CONSTRAINT fk_indicator_symbol FOREIGN KEY (symbol_id) REFERENCES dim_symbol(symbol_id),
    CONSTRAINT fk_indicator_date FOREIGN KEY (date_id) REFERENCES dim_calendar(date_id)
);

-- Operational: ELT job log
CREATE TABLE IF NOT EXISTS elt_job_log (
    job_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_type VARCHAR(32) NOT NULL,
    started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at DATETIME NULL,
    status ENUM('SUCCESS','FAILED','PARTIAL','RUNNING') NOT NULL DEFAULT 'RUNNING',
    records_processed INT DEFAULT 0,
    error_message TEXT NULL,
    details JSON NULL,
    INDEX idx_job_type_started (job_type, started_at),
    INDEX idx_status (status)
);

-- Operational: Data quality log
CREATE TABLE IF NOT EXISTS data_quality_log (
    dq_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(64) NOT NULL,
    run_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status ENUM('PASS','FAIL','WARN') NOT NULL,
    affected_rows INT NULL,
    message TEXT NULL,
    details JSON NULL,
    INDEX idx_check_run (check_name, run_at),
    INDEX idx_status (status)
);

-- Analytical Views (non-breaking, rely on dim_symbol + fact_intraday_price)
CREATE OR REPLACE VIEW vw_intraday_all AS
SELECT f.fact_id, s.symbol, s.asset_type, f.datetime, f.interval_minutes,
       f.open, f.high, f.low, f.close, f.volume
FROM fact_intraday_price f
JOIN dim_symbol s ON f.symbol_id = s.symbol_id;

CREATE OR REPLACE VIEW vw_stock_intraday AS
SELECT * FROM vw_intraday_all WHERE asset_type='STOCK';

CREATE OR REPLACE VIEW vw_index_intraday AS
SELECT * FROM vw_intraday_all WHERE asset_type='INDEX';

CREATE OR REPLACE VIEW vw_commodity_intraday AS
SELECT * FROM vw_intraday_all WHERE asset_type='COMMODITY';

-- Helper view combining price + indicators
CREATE OR REPLACE VIEW vw_intraday_with_indicators AS
SELECT p.symbol, p.asset_type, p.datetime, p.interval_minutes,
       p.open, p.high, p.low, p.close, p.volume,
       i.sma_20, i.sma_50, i.sma_200, i.ema_12, i.ema_26,
       i.macd, i.macd_signal, i.macd_hist, i.rsi_14,
       i.bb_mid, i.bb_upper, i.bb_lower
FROM vw_intraday_all p
LEFT JOIN fact_indicator i
  ON i.symbol_id = (SELECT symbol_id FROM dim_symbol ds WHERE ds.symbol = p.symbol LIMIT 1)
 AND i.datetime = p.datetime
 AND i.interval_minutes = p.interval_minutes;

-- Seed minimal dim_time_interval values
INSERT IGNORE INTO dim_time_interval (minutes, label) VALUES
 (1,'1 Minute'),(5,'5 Minute'),(15,'15 Minute'),(30,'30 Minute'),(60,'1 Hour'),(1440,'1 Day');

-- NOTE: Populate dim_symbol & dim_calendar before loading new fact tables.
-- Example (symbol seed): INSERT IGNORE INTO dim_symbol(symbol, asset_type) SELECT DISTINCT symbol,'STOCK' FROM stock_data;
-- Example (calendar seed): generate rows for needed date span via a numbers table or recursive CTE.

