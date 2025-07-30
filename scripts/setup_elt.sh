#!/bin/bash
# Setup ELT Pipeline for Market Data Collection (Linux Version)
# This script installs dependencies and prepares the environment

echo -e "\033[32m=== Market Data ELT Pipeline Setup (Linux) ===\033[0m"
echo ""

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo -e "\033[31m✗ Python 3 is not installed\033[0m"
    echo "Please install Python 3.8+ first"
    exit 1
fi

python_version=$(python3 --version | cut -d' ' -f2)
echo -e "\033[32m✓ Python $python_version found\033[0m"

# Check pip
if ! command -v pip3 &> /dev/null; then
    echo -e "\033[31m✗ pip3 is not installed\033[0m"
    echo "Please install pip3 first"
    exit 1
fi
echo -e "\033[32m✓ pip3 found\033[0m"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "\033[31m✗ Docker is not installed\033[0m"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi
echo -e "\033[32m✓ Docker found\033[0m"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "\033[31m✗ Docker Compose is not installed\033[0m"
    echo "Please install Docker Compose first"
    exit 1
fi
echo -e "\033[32m✓ Docker Compose found\033[0m"

# Install Python dependencies
echo ""
echo -e "\033[33mInstalling Python dependencies...\033[0m"
if ! pip3 install -r requirements.txt; then
    echo -e "\033[31m✗ Failed to install Python dependencies\033[0m"
    exit 1
fi
echo -e "\033[32m✓ Python dependencies installed\033[0m"

# Create necessary directories
echo -e "\033[33mCreating directories...\033[0m"
mkdir -p logs
mkdir -p data_extracts/{stocks,indexes,commodities,bonds,archive}
echo -e "\033[32m✓ Directories created\033[0m"

# Make shell scripts executable
echo -e "\033[33mMaking scripts executable...\033[0m"
chmod +x *.sh
echo -e "\033[32m✓ Scripts are now executable\033[0m"

# Check .env file
if [ ! -f ".env" ]; then
    echo -e "\033[31m✗ .env file not found\033[0m"
    echo "Please create .env file with your API configuration"
    exit 1
fi

# Check if API key is set
if grep -q "your_fmp_api_key_here" .env; then
    echo -e "\033[33m⚠ Please update your FMP API key in .env file\033[0m"
else
    echo -e "\033[32m✓ FMP API key is configured\033[0m"
fi

echo ""
echo -e "\033[32m=== Setup Complete! ===\033[0m"
echo ""
echo -e "\033[36mNext steps:\033[0m"
echo "1. Start database:     ./start_database.sh"
echo "2. Initialize tables:  ./setup_database.sh"
echo "3. Test extraction:    ./run_elt_ops.sh -c extract"
echo "4. Run full ELT:       ./run_elt_ops.sh -c full"
echo "5. Start scheduler:    ./start_elt.sh"
echo ""
echo -e "\033[33mAll scripts are now Linux-compatible!\033[0m"
