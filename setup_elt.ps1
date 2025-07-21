# PowerShell script to set up the ELT environment and run initial setup
Write-Host "Setting up Market Data ELT Process..." -ForegroundColor Green

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    Write-Host "Found Python: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "Error: Python not found. Please install Python 3.8+" -ForegroundColor Red
    exit 1
}

# Install/upgrade pip packages
Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error: Failed to install dependencies" -ForegroundColor Red
    exit 1
}

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env template file..." -ForegroundColor Yellow
    @"
# FMP API Configuration
API_KEY=your_fmp_api_key_here

# Database Configuration
DB_HOST=localhost
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=market_data

# ELT Configuration
REAL_TIME_ENABLED=true
"@ | Out-File -FilePath ".env" -Encoding UTF8
    
    Write-Host "Please edit .env file with your actual configuration" -ForegroundColor Red
    Write-Host "You need to set your FMP API key and database credentials" -ForegroundColor Red
}

# Create database schema
Write-Host "Setting up database schema..." -ForegroundColor Yellow
python create_database.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Warning: Database setup failed. Please check your database connection." -ForegroundColor Yellow
}

# Create logs and reports directories
Write-Host "Creating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "reports" | Out-Null

Write-Host "Setup completed!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Edit .env file with your FMP API key and database credentials"
Write-Host "2. Run: python run_elt.py quality  # to test data quality checks"
Write-Host "3. Run: python run_elt.py extract  # to test data extraction"
Write-Host "4. Run: python run_elt.py schedule # to start the scheduler"
Write-Host ""
Write-Host "Available commands:" -ForegroundColor Cyan
Write-Host "  python run_elt.py extract    # Extract current data"
Write-Host "  python run_elt.py backfill --start-date 2024-01-01 --end-date 2024-01-31"
Write-Host "  python run_elt.py transform  # Run analytics transformations"
Write-Host "  python run_elt.py quality   # Run data quality checks"
Write-Host "  python run_elt.py full      # Run complete ELT process once"
Write-Host "  python run_elt.py report    # Generate weekly report"
Write-Host "  python run_elt.py schedule  # Start 15-minute scheduler"
