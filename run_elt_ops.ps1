# PowerShell script for quick ELT operations
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("extract", "backfill", "transform", "quality", "full", "report")]
    [string]$Command,
    
    [string]$StartDate,
    [string]$EndDate
)

Write-Host "Running ELT operation: $Command" -ForegroundColor Green

switch ($Command) {
    "extract" {
        Write-Host "Extracting current market data..." -ForegroundColor Yellow
        python run_elt.py extract
    }
    "backfill" {
        if (-not $StartDate -or -not $EndDate) {
            Write-Host "Backfill requires -StartDate and -EndDate parameters (YYYY-MM-DD format)" -ForegroundColor Red
            exit 1
        }
        Write-Host "Running backfill from $StartDate to $EndDate..." -ForegroundColor Yellow
        python run_elt.py backfill --start-date $StartDate --end-date $EndDate
    }
    "transform" {
        Write-Host "Running analytics transformations..." -ForegroundColor Yellow
        python run_elt.py transform
    }
    "quality" {
        Write-Host "Running data quality checks..." -ForegroundColor Yellow
        python run_elt.py quality
    }
    "full" {
        Write-Host "Running full ELT process..." -ForegroundColor Yellow
        python run_elt.py full
    }
    "report" {
        Write-Host "Generating weekly report..." -ForegroundColor Yellow
        python run_elt.py report
    }
}

if ($LASTEXITCODE -eq 0) {
    Write-Host "Operation completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Operation failed with exit code $LASTEXITCODE" -ForegroundColor Red
}
