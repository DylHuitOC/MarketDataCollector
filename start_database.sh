#!/bin/bash
# Start MySQL Database for Market Data ELT Pipeline
# This script starts a MySQL database using Docker

echo -e "\e[32mStarting MySQL Database for Market Data Pipeline...\e[0m"

# Check if Docker is running
if ! docker version &> /dev/null; then
    echo -e "\e[31m✗ Docker is not running. Please start Docker first.\e[0m"
    exit 1
fi
echo -e "\e[32m✓ Docker is running\e[0m"

# Start the MySQL container
echo -e "\e[33mStarting MySQL container...\e[0m"
docker-compose up -d

# Wait for MySQL to be ready
echo -e "\e[33mWaiting for MySQL to be ready...\e[0m"
max_wait=30
waited=0

while [ $waited -lt $max_wait ]; do
    sleep 2
    waited=$((waited + 2))
    
    # Test connection
    if docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass &> /dev/null; then
        echo -e "\e[32m✓ MySQL is ready!\e[0m"
        break
    fi
    
    if [ $waited -ge $max_wait ]; then
        echo -e "\e[31m✗ MySQL failed to start within $max_wait seconds\e[0m"
        echo -e "\e[33mCheck Docker logs: docker logs market_data_mysql\e[0m"
        exit 1
    fi
    
    echo -n "."
done

echo ""
echo -e "\e[36mDatabase Connection Details:\e[0m"
echo "  Host: localhost:3306"
echo "  Database: market_data_warehouse"
echo "  Username: market_user"
echo "  Password: market_pass"
echo ""
echo -e "\e[33mTo stop the database: docker-compose down\e[0m"
echo -e "\e[33mTo view logs: docker logs market_data_mysql\e[0m"
