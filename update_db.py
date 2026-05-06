import pandas as pd
import sqlite3

print("Loading GenericFBrazil.xlsx...")
df = pd.read_excel('GenericFBrazil.xlsx', header=None)

from datetime import datetime

# Row 1 contains dates from column 1 (index 1) onwards. 
dates_row = df.iloc[1]

# Find the first column index that contains a valid datetime
start_col = None
for i, val in enumerate(dates_row):
    if pd.notnull(val):
        if isinstance(val, (datetime, pd.Timestamp)):
            start_col = i
            break
        try:
            pd.to_datetime(val)
            start_col = i
            break
        except:
            continue

if start_col is None:
    print("Could not find dates row")
    exit(1)

dates = dates_row[start_col:].values
contract_names = df.iloc[2:, 0].values

records = []
for row_idx in range(2, len(df)):
    contract = df.iloc[row_idx, 0]
    if pd.isnull(contract):
        continue
    
    prices = df.iloc[row_idx, start_col:].values
    for date_idx, price in enumerate(prices):
        if pd.notnull(price) and price != '':
            try:
                date_str = pd.to_datetime(dates[date_idx]).strftime('%Y-%m-%d %H:%M:%S.%f')
                records.append((contract, date_str, float(price)))
            except:
                pass

print(f"Parsed {len(records)} price records.")

con = sqlite3.connect('trading_data.db')
cur = con.cursor()

# We can completely replace market_prices since this file contains all data
cur.execute("DROP TABLE IF EXISTS market_prices")
cur.execute("""
CREATE TABLE market_prices (
    contract_name TEXT,
    date TEXT,
    price REAL,
    PRIMARY KEY(contract_name, date)
)
""")

cur.executemany("INSERT INTO market_prices VALUES (?, ?, ?)", records)
con.commit()
print(f"Saved {len(records)} records to market_prices.")

# We also need BZDIOVRA Index? Let's check if BZDIOVRA Index is in the file.
count_idx = cur.execute("SELECT COUNT(*) FROM market_prices WHERE contract_name='BZDIOVRA Index'").fetchone()[0]
print(f"BZDIOVRA Index records: {count_idx}")

con.close()
