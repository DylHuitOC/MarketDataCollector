#!/bin/bash
# ELT Operations Runner for Linux
# Equivalent to run_elt_ops.ps1 but for bash

COMMAND=""
START_DATE=""
END_DATE=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--command)
            COMMAND="$2"
            shift 2
            ;;
        -s|--start-date)
            START_DATE="$2"
            shift 2
            ;;
        -e|--end-date)
            END_DATE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 -c <command> [-s start-date] [-e end-date]"
            echo ""
            echo "Commands:"
            echo "  extract     - Extract data to CSV files only"
            echo "  csv-load    - Load CSV files to raw data warehouse"
            echo "  transform   - Transform raw data to analytics warehouse"
            echo "  quality     - Run data quality checks"
            echo "  full        - Run complete ELT pipeline"
            echo "  report      - Generate weekly report"
            echo ""
            echo "Examples:"
            echo "  $0 -c extract"
            echo "  $0 -c full"
            echo "  $0 -c quality"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

if [ -z "$COMMAND" ]; then
    echo -e "\033[31mError: Command is required\033[0m"
    echo "Use -h or --help for usage information"
    exit 1
fi

echo -e "\033[32mRunning ELT operation: $COMMAND\033[0m"

# Check if database is running
if ! docker exec market_data_mysql mysqladmin ping -h localhost -u market_user -pmarket_pass &> /dev/null; then
    echo -e "\033[31m✗ MySQL is not running. Please run ./start_database.sh first.\033[0m"
    exit 1
fi

# Execute the command
case $COMMAND in
    extract)
        echo -e "\033[33mExtracting market data to CSV files...\033[0m"
        python3 run_elt.py extract
        ;;
    csv-load)
        echo -e "\033[33mLoading CSV files to raw data warehouse...\033[0m"
        python3 run_elt.py csv-load
        ;;
    transform)
        echo -e "\033[33mTransforming data to analytics warehouse...\033[0m"
        python3 run_elt.py transform
        ;;
    quality)
        echo -e "\033[33mRunning data quality checks...\033[0m"
        python3 run_elt.py quality
        ;;
    full)
        echo -e "\033[33mRunning complete ELT pipeline...\033[0m"
        python3 run_elt.py full
        ;;
    report)
        echo -e "\033[33mGenerating weekly report...\033[0m"
        python3 run_elt.py report
        ;;
    *)
        echo -e "\033[31mUnknown command: $COMMAND\033[0m"
        echo "Use -h or --help for available commands"
        exit 1
        ;;
esac

echo -e "\033[32m✓ Operation completed\033[0m"
