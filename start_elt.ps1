# PowerShell script to start the ELT scheduler
Write-Host "Starting Market Data ELT Scheduler..." -ForegroundColor Green
Write-Host "This will run data extraction every 15 minutes during market hours" -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

python run_elt.py schedule
