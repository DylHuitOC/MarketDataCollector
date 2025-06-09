#!/bin/bash
python collect_stocks.py
python collect_indexes.py
python collect_commodities.py
python collect_bond.py
echo "✅ All data fetched."