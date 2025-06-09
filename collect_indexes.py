import requests
import pandas as pd
from datetime import datetime, timedelta
import time
from config import API_KEY, FROM_DATE, TO_DATE, INDEX_SYMBOLS
from utils import get_db_connection

API_URL_TEMPLATE = 'https://financialmodelingprep.com/stable/historical-chart/5min?symbol={}&from={}&to={}&apikey={}'

# Connect to MySQL
conn = get_db_connection()
cursor = conn.cursor()

def fetch_index_data(symbol, start, end):
    url = API_URL_TEMPLATE.format(symbol, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), API_KEY)
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[❌] Error fetching {symbol} ({start} to {end}): {e}")
        return pd.DataFrame()

def aggregate_5min_to_15min(df):
    # Ensure data is sorted by datetime ascending
    df = df.sort_values(by='date').reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'])
    
    # Create a group index: every 3 rows belong to one group
    df['group'] = df.index // 3
    
    # Aggregate groups
    agg_df = df.groupby('group').agg({
        'date': 'first',                     # first date of group (start of 15-min)
        'open': 'first',                     # open of first candle
        'close': 'last',                     # close of last candle
        'high': 'max',                      # max high
        'low': 'min',                       # min low
        'volume': 'sum'                     # sum volume
    }).reset_index(drop=True)
    
    return agg_df

def store_data(df, symbol):
    for _, row in df.iterrows():
        try:
            date = row['date'].to_pydatetime().replace(tzinfo=None)
            cursor.execute('SELECT COUNT(*) FROM index_data WHERE date = %s AND symbol = %s', (date, symbol))
            if cursor.fetchone()[0] == 0:
                cursor.execute('''
                    INSERT INTO index_data (date, open, low, high, close, volume, symbol)
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

start_date = datetime.strptime(FROM_DATE, '%Y-%m-%d')
end_date = datetime.strptime(TO_DATE, '%Y-%m-%d')

for symbol in INDEX_SYMBOLS:
    print(f"\n📦 Processing {symbol}")
    current_start = start_date
    while current_start < end_date:
        print(f"📊 Fetching: {current_start.date()} to {min(current_start + timedelta(days=10), end_date).date()}")
        current_end = current_start + timedelta(days=10)
        df = fetch_index_data(symbol, current_start, current_end)
        if not df.empty:
            aggregated_df = aggregate_5min_to_15min(df)
            store_data(aggregated_df, symbol)
        time.sleep(1)
        current_start = current_end

cursor.close()
conn.close()
print("\n✅ All done!")
