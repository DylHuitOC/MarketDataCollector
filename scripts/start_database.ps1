# Start MySQL Database for Market Data ELT Pipeline
# This script starts a MySQL database using Docker

Write-Host "Starting MySQL Database for Market Data Pipeline..." -ForegroundColor Green

# Check if Docker is running
try {
    docker version | Out-Null
    Write-Host "✓ Docker is running" -ForegroundColor Green
} 
catch {
    Write-Host "✗ Docker is not running. Please start Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Start the MySQL container
Write-Host "Starting MySQL container..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to start MySQL container" -ForegroundColor Red
    Write-Host "Check if docker-compose.yml exists and Docker is properly configured" -ForegroundColor Yellow
    exit 1
}

# Wait for MySQL to be ready
Write-Host "Waiting for MySQL to be ready..." -ForegroundColor Yellow
$maxWait = 60
$waited = 0

while ($waited -lt $maxWait) {
    Start-Sleep -Seconds 3
    $waited += 3
    
    # Test connection
    $testResult = docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass 2>$null
    
    if ($testResult -like "*mysqld is alive*") {
        Write-Host "MySQL is ready!" -ForegroundColor Green
        break
    }
    
    if ($waited -ge $maxWait) {
        Write-Host "MySQL failed to start within $maxWait seconds" -ForegroundColor Red
        Write-Host "Check Docker logs: docker logs market_data_mysql" -ForegroundColor Yellow
        exit 1
    }
    
    Write-Host "." -NoNewline -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Database Connection Details ===" -ForegroundColor Cyan
Write-Host "Host: localhost:3306"
Write-Host "Database: market_data_warehouse"
Write-Host "Username: market_user"
Write-Host "Password: market_pass"
Write-Host ""
Write-Host "=== Management Commands ===" -ForegroundColor Yellow
Write-Host "Stop database: docker-compose down"
Write-Host "View logs: docker logs market_data_mysql"
Write-Host "Connect to DB: docker exec -it market_data_mysql mysql -u market_user -pmarket_pass market_data_warehouse"
