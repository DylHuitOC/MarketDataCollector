#!/bin/bash
# Initialize Database Tables for Market Data ELT Pipeline
# This script creates all required tables for the CSV → DW1 → DW2 workflow

echo -e "\033[32mInitializing Market Data ELT Database Tables...\033[0m"

# Check if database is running
if ! docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass &> /dev/null; then
    echo -e "\033[31m✗ MySQL is not running. Please run ./start_database.sh first.\033[0m"
    exit 1
fi
echo -e "\033[32m✓ MySQL is running\033[0m"

# Create raw data warehouse tables (DW1)
echo -e "\033[33mCreating raw data warehouse tables (DW1)...\033[0m"
python3 -c "
from load.csv_data_warehouse_loader import CSVDataWarehouseLoader
print('Creating raw data warehouse tables...')
csv_loader = CSVDataWarehouseLoader()
csv_loader.create_raw_data_tables()
print('✓ Raw data warehouse tables created')
"

if [ $? -ne 0 ]; then
    echo -e "\033[31m✗ Failed to create raw data warehouse tables\033[0m"
    exit 1
fi

# Create analytics data warehouse tables (DW2)
echo -e "\033[33mCreating analytics data warehouse tables (DW2)...\033[0m"
python3 -c "
from transform.raw_to_analytics_transformer import RawToAnalyticsTransformer
print('Creating analytics data warehouse tables...')
transformer = RawToAnalyticsTransformer()
transformer.create_analytics_tables()
print('✓ Analytics data warehouse tables created')
"

if [ $? -ne 0 ]; then
    echo -e "\033[31m✗ Failed to create analytics data warehouse tables\033[0m"
    exit 1
fi

echo ""
echo -e "\033[32m✓ Database initialization completed successfully!\033[0m"
echo ""
echo -e "\033[36mYour ELT pipeline is now ready to use:\033[0m"
echo "  • Raw Data Warehouse (DW1): For CSV-loaded data"
echo "  • Analytics Data Warehouse (DW2): For transformed data with indicators"
echo ""
echo -e "\033[33mNext steps:\033[0m"
echo "  1. Add your FMP API key to .env file"
echo "  2. Test extraction: python3 run_elt.py extract"
echo "  3. Run full ELT: python3 run_elt.py full"
echo "  4. Start scheduler: python3 run_elt.py schedule"
