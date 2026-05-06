import sqlite3
con = sqlite3.connect('trading_data.db')
cur = con.cursor()

# Tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", cur.fetchall())

# Contract samples
cur.execute("SELECT DISTINCT contract_name FROM market_prices ORDER BY contract_name LIMIT 30")
print("\nSample contracts:")
for r in cur.fetchall():
    print(f"  {r[0]}")

cur.execute("SELECT COUNT(DISTINCT contract_name) FROM market_prices")
print(f"\nTotal contracts: {cur.fetchone()[0]}")

cur.execute("SELECT MIN(date), MAX(date) FROM market_prices")
print(f"Date range: {cur.fetchone()}")
con.close()
