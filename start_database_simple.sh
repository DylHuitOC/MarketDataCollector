#!/bin/bash
# Start MySQL Database for Market Data ELT Pipeline
# This script starts a MySQL database using Docker

echo "Starting MySQL Database for Market Data Pipeline..."

# Check if Docker is running
if ! docker version &> /dev/null; then
    echo "ERROR: Docker is not running. Please start Docker first."
    exit 1
fi
echo "SUCCESS: Docker is running"

# Start the MySQL container
echo "Starting MySQL container..."
docker-compose up -d

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to start MySQL container"
    echo "Check if docker-compose.yml exists and Docker is properly configured"
    exit 1
fi

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
max_wait=60
waited=0

while [ $waited -lt $max_wait ]; do
    sleep 3
    waited=$((waited + 3))
    
    # Test connection
    if docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass &> /dev/null; then
        echo "SUCCESS: MySQL is ready!"
        break
    fi
    
    if [ $waited -ge $max_wait ]; then
        echo "ERROR: MySQL failed to start within $max_wait seconds"
        echo "Check Docker logs with: docker logs market_data_mysql"
        exit 1
    fi
    
    echo -n "."
done

echo ""
echo "=== Database Connection Details ==="
echo "Host: localhost:3306"
echo "Database: market_data_warehouse"
echo "Username: market_user"
echo "Password: market_pass"
echo ""
echo "=== Management Commands ==="
echo "Stop database: docker-compose down"
echo "View logs: docker logs market_data_mysql"
echo "Connect to DB: docker exec -it market_data_mysql mysql -u market_user -pmarket_pass market_data_warehouse"
