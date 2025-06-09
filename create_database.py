import mysql.connector
from config import DB_CONFIG

# Extract DB name because it's inside DB_CONFIG
DB_NAME = DB_CONFIG['database']

# Connect to MySQL server (without selecting database yet)
conn = mysql.connector.connect(
    host=DB_CONFIG['host'],
    user=DB_CONFIG['user'],
    password=DB_CONFIG['password']
)
cursor = conn.cursor()

# Create database if it doesn't exist
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
print(f"Database '{DB_NAME}' is ready!")

# Connect to the newly created database
conn.database = DB_NAME

# Table creation queries
TABLES = {}

TABLES['stock_data'] = (
    """
    CREATE TABLE IF NOT EXISTS stock_data (
        date DATETIME NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        volume INT NOT NULL,
        symbol VARCHAR(10) NOT NULL
    )
    """
)

TABLES['bond_data'] = (
    """
    CREATE TABLE IF NOT EXISTS bond_data (
        date DATE NOT NULL PRIMARY KEY,
        month1 DECIMAL(5, 2) NOT NULL,
        month2 DECIMAL(5, 2) NOT NULL,
        month3 DECIMAL(5, 2) NOT NULL,
        month6 DECIMAL(5, 2) NOT NULL,
        year1 DECIMAL(5, 2) NOT NULL,
        year2 DECIMAL(5, 2) NOT NULL,
        year3 DECIMAL(5, 2) NOT NULL,
        year5 DECIMAL(5, 2) NOT NULL,
        year7 DECIMAL(5, 2) NOT NULL,
        year10 DECIMAL(5, 2) NOT NULL,
        year20 DECIMAL(5, 2) NOT NULL,
        year30 DECIMAL(5, 2) NOT NULL
    )
    """
)

TABLES['index_data'] = (
    """
    CREATE TABLE IF NOT EXISTS index_data (
        date DATETIME NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        open DECIMAL(12, 2) NOT NULL,
        low DECIMAL(12, 2) NOT NULL,
        high DECIMAL(12, 2) NOT NULL,
        close DECIMAL(12, 2) NOT NULL,
        volume BIGINT NOT NULL,
        PRIMARY KEY (date, symbol)
    )
    """
)

TABLES['commodity_data'] = (
    """
    CREATE TABLE IF NOT EXISTS commodity_data (
        date DATETIME NOT NULL,
        symbol VARCHAR(50) NOT NULL,
        open DECIMAL(12, 4) NOT NULL,
        low DECIMAL(12, 4) NOT NULL,
        high DECIMAL(12, 4) NOT NULL,
        close DECIMAL(12, 4) NOT NULL,
        volume BIGINT NOT NULL,
        PRIMARY KEY (date, symbol)
    )
    """
)

# Execute each table creation
for table_name, ddl in TABLES.items():
    try:
        cursor.execute(ddl)
        print(f"Table '{table_name}' is ready!")
    except mysql.connector.Error as err:
        print(f"Error creating table {table_name}: {err}")

# Clean up
cursor.close()
conn.close()
print("✅ All tables created successfully.")
