import mysql.connector
import pandas as pd
from config import FROM_DATE, TO_DATE, STOCK_SYMBOLS, INDEX_SYMBOLS, COMMODITY_SYMBOLS
from utils import get_db_connection

# Connect to MySQL
conn = get_db_connection()

# Prepare stock symbol list for SQL
stock_symbols_sql = ', '.join([f"'{symbol}'" for symbol in STOCK_SYMBOLS])

# Function to clean symbols for SQL aliases and CSV column names
def clean_symbol(symbol):
    return symbol.replace('^', '').replace('/', '_')

# Base query
query = """
SELECT 
    s.symbol AS stock_ID,
    s.close AS Close,
    s.volume AS Volume_PP,
    s.date AS DATETIME,
    b.year2 AS US2Y_PP,
    b.year5 AS US5Y_PP,
    b.year10 AS US10Y_PP
"""

# Dynamically add index columns
for symbol in INDEX_SYMBOLS:
    clean_alias = clean_symbol(symbol)
    query += f", {clean_alias.lower()}.close AS {clean_alias}_PP\n"

# Dynamically add commodity columns
for symbol in COMMODITY_SYMBOLS:
    clean_alias = clean_symbol(symbol)
    query += f", {clean_alias.lower()}.close AS {clean_alias}_PP\n"

query += "FROM stock_data s\n"

# Bond join (common to all)
query += """
LEFT JOIN LATERAL (
    SELECT * FROM bond_data b 
    WHERE b.date <= DATE(s.date)
    ORDER BY b.date DESC
    LIMIT 1
) b ON TRUE
"""

# Dynamic joins for indexes
for symbol in INDEX_SYMBOLS:
    clean_alias = clean_symbol(symbol)
    query += f"""
LEFT JOIN LATERAL (
    SELECT close FROM index_data
    WHERE symbol = '{symbol}' AND date <= s.date
    ORDER BY date DESC
    LIMIT 1
) {clean_alias.lower()} ON TRUE
"""

# Dynamic joins for commodities
for symbol in COMMODITY_SYMBOLS:
    clean_alias = clean_symbol(symbol)
    query += f"""
LEFT JOIN LATERAL (
    SELECT close FROM commodity_data
    WHERE symbol = '{symbol}' AND date <= s.date
    ORDER BY date DESC
    LIMIT 1
) {clean_alias.lower()} ON TRUE
"""

# Add WHERE clause and ORDER
query += f"""
WHERE s.symbol IN ({stock_symbols_sql})
AND s.date BETWEEN '{FROM_DATE}' AND '{TO_DATE}'
ORDER BY s.date ASC;
"""

# Run query and export
df = pd.read_sql(query, conn)

# Export to CSV
csv_filename = 'market_data_export.csv'
df.to_csv(csv_filename, index=False)
print(f"✅ Data exported to {csv_filename}")

# Close connection
conn.close()
