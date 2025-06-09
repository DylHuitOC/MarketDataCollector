import requests
import mysql.connector
from datetime import datetime, timedelta
import time
from config import API_KEY, FROM_DATE, TO_DATE
from utils import get_db_connection

API_URL = 'https://financialmodelingprep.com/stable/treasury-rates?from={}&to={}&apikey={}'

conn = get_db_connection()
cursor = conn.cursor()

# Insert function
def insert_bond_data(records):
    sql = """
    INSERT INTO bond_data (
        date, month1, month2, month3, month6,
        year1, year2, year3, year5, year7,
        year10, year20, year30
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s
    ) ON DUPLICATE KEY UPDATE
        month1=VALUES(month1), month2=VALUES(month2),
        month3=VALUES(month3), month6=VALUES(month6),
        year1=VALUES(year1), year2=VALUES(year2),
        year3=VALUES(year3), year5=VALUES(year5),
        year7=VALUES(year7), year10=VALUES(year10),
        year20=VALUES(year20), year30=VALUES(year30)
    """
    for record in records:
        try:
            date = datetime.strptime(record['date'], '%Y-%m-%d').date()
            values = tuple(record.get(k, None) for k in [
                'date', 'month1', 'month2', 'month3', 'month6',
                'year1', 'year2', 'year3', 'year5', 'year7',
                'year10', 'year20', 'year30'
            ])
            cursor.execute(sql, values)
        except Exception as e:
            print(f"[⚠️] Insert error at {record['date']}: {e}")
    conn.commit()

# Main batching loop
start_date = datetime.strptime(FROM_DATE, '%Y-%m-%d')
end_date = datetime.strptime(TO_DATE, '%Y-%m-%d')

while start_date < end_date:
    batch_end = start_date + timedelta(days=30)
    print(f"📊 Fetching: {start_date.date()} to {min(batch_end, end_date).date()}")

    url = API_URL.format(
        start_date.strftime('%Y-%m-%d'),
        min(batch_end, end_date).strftime('%Y-%m-%d'),
        API_KEY
    )

    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        insert_bond_data(data)
    else:
        print(f"[❌] Failed batch {start_date} - {batch_end}: {response.status_code}")

    time.sleep(1)  # API throttle
    start_date = batch_end

cursor.close()
conn.close()
print("\n✅ All done!")
