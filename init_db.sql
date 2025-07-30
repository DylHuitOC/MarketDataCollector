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
