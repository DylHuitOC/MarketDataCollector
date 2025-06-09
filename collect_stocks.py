import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from config import API_KEY, FROM_DATE, TO_DATE, STOCK_SYMBOLS
from utils import get_db_connection


API_URL_TEMPLATE = 'https://financialmodelingprep.com/stable/historical-chart/15min?symbol={}&from={}&to={}&apikey={}'

# Connect to MySQL
conn = get_db_connection()
cursor = conn.cursor()

def fetch_stock_data(symbol, start, end):
    url = API_URL_TEMPLATE.format(symbol, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), API_KEY)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[❌] Error fetching {symbol} ({start} to {end}): {e}")
        return pd.DataFrame()

def store_data(df, symbol):
    for _, row in df.iterrows():
        try:
            date = pd.to_datetime(row['date']).to_pydatetime().replace(tzinfo=None)
            cursor.execute('SELECT COUNT(*) FROM stock_data WHERE date = %s AND symbol = %s', (date, symbol))
            if cursor.fetchone()[0] == 0:
                cursor.execute('''
                    INSERT INTO stock_data (date, open, low, high, close, volume, symbol)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (
                    date,
                    round(row['open'], 2),
                    round(row['low'], 2),
                    round(row['high'], 2),
                    round(row['close'], 2),
                    int(row['volume']),
                    symbol
                ))
        except Exception as e:
            print(f"[⚠️] Insert error for {symbol} at {row['date']}: {e}")
    conn.commit()

# Main loop
start_date = datetime.strptime(FROM_DATE, '%Y-%m-%d')
end_date = datetime.strptime(TO_DATE, '%Y-%m-%d')

for symbol in STOCK_SYMBOLS:
    print(f"\n📦 Processing {symbol}")
    current_start = start_date
    while current_start < end_date:
        print(f"📊 Fetching: {current_start.date()} to {min(current_start + timedelta(days=10), end_date).date()}")
        current_end = current_start + timedelta(days=10)
        df = fetch_stock_data(symbol, current_start, current_end)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            store_data(df, symbol)
        time.sleep(1)  # be nice to the API
        current_start = current_end

# Close DB connection
cursor.close()
conn.close()
print("\n✅ All done!")
