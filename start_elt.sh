#!/bin/bash
# Start ELT Pipeline Scheduler for Market Data Collection
# Runs the 15-minute ELT scheduler during market hours

echo -e "\033[32mStarting Market Data ELT Pipeline Scheduler...\033[0m"

# Check if database is running
if ! docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass &> /dev/null; then
    echo -e "\033[31m✗ MySQL is not running. Please run ./start_database.sh first.\033[0m"
    exit 1
fi

# Check if Python dependencies are installed
if ! python3 -c "import mysql.connector, pandas, schedule, pytz" &> /dev/null; then
    echo -e "\033[33mInstalling Python dependencies...\033[0m"
    pip3 install -r requirements.txt
fi

echo -e "\033[32m✓ All dependencies ready\033[0m"
echo -e "\033[33mStarting ELT scheduler (runs every 15 minutes during market hours)...\033[0m"
echo -e "\033[33mPress Ctrl+C to stop\033[0m"
echo ""

# Start the scheduler
python3 run_elt.py schedule
