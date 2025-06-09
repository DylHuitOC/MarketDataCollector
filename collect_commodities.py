import requests
from datetime import datetime, timedelta, time as dtime
import time
from config import API_KEY, FROM_DATE, TO_DATE, COMMODITY_SYMBOLS
from utils import get_db_connection

API_URL = 'https://financialmodelingprep.com/stable/historical-chart/15min?symbol={}&from={}&to={}&apikey={}'

# Market hours
MARKET_OPEN = dtime(9, 30)
MARKET_CLOSE = dtime(15, 45)

conn = get_db_connection()
cursor = conn.cursor()

def insert_commodity_data(cursor, data, symbol):
    sql = """
    INSERT INTO commodity_data (date, symbol, open, low, high, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        open=VALUES(open),
        low=VALUES(low),
        high=VALUES(high),
        close=VALUES(close),
        volume=VALUES(volume)
    """
    for entry in data:
        try:
            dt = datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S')
            if MARKET_OPEN <= dt.time() <= MARKET_CLOSE:
                cursor.execute(sql, (
                    dt,
                    symbol,
                    float(entry['open']),
                    float(entry['low']),
                    float(entry['high']),
                    float(entry['close']),
                    int(entry['volume'])
                ))
        except Exception as e:
            print(f"[⚠️] Insert error for {symbol} at {entry.get('date', 'unknown')}: {e}")

# Main loop with batching
start_date = datetime.strptime(FROM_DATE, '%Y-%m-%d')
end_date = datetime.strptime(TO_DATE, '%Y-%m-%d')

for symbol in COMMODITY_SYMBOLS:
    print(f"\n📦 Fetching {symbol}")
    current_start = start_date
    while current_start < end_date:
        current_end = current_start + timedelta(days=10)
        url = API_URL.format(symbol, current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d'), API_KEY)
        try:
            print(f"Fetching data from {current_start.date()} to {current_end.date()} for {symbol}...")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if data:
                insert_commodity_data(cursor, data, symbol)
                conn.commit()
                print(f"[✅] {symbol}: {current_start.date()} → {current_end.date()} ({len(data)} raw records)")
        except Exception as e:
            print(f"[❌] Fetch error for {symbol} ({current_start.date()} to {current_end.date()}): {e}")
        time.sleep(1)
        current_start = current_end

# Clean up
cursor.close()
conn.close()
print("\n✅ All done!")
