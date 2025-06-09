# Market Data Collector

A Python project to fetch stock, index, commodity, and US treasury bond data from Financial Modeling Prep API and store it in MySQL.

## Setup
1. Create and initialize venv if needed:\
`python -m venv .venv`\
`source .venv/bin/activate`   
2. Install dependencies:\
`pip install -r requirements.txt`
3. Set up a mysql user for the script and a new database:\`CREATE DATABASE ATS`
4. Create a *.env* file and add your API key and DB credentials.
5. Change date range and symbols in *config.py* if needed
6. Execute the run scripts: *run_createDB.sh*, *run_collect.sh* and *run_export.sh* (make executable using `chmod +x`)