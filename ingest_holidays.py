"""
ingest_holidays.py - One-time script to load Brazilian holidays into trading_data.db
"""
import pandas as pd
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trading_data.db')
EXCEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Holidays_Brazil.xlsx')

def ingest():
    # Read first column, first 1264 rows
    df = pd.read_excel(EXCEL_PATH, header=None, nrows=1264)
    holidays = df[0].dropna()
    holidays = pd.to_datetime(holidays)

    # Format as date strings for storage
    records = [(d.strftime('%Y-%m-%d'),) for d in holidays]

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # Create table (drop if exists for clean re-run)
    cur.execute("DROP TABLE IF EXISTS market_holidays")
    cur.execute("""
        CREATE TABLE market_holidays (
            holiday_date TEXT PRIMARY KEY
        )
    """)

    cur.executemany("INSERT OR IGNORE INTO market_holidays (holiday_date) VALUES (?)", records)
    con.commit()

    # Verify
    cur.execute("SELECT COUNT(*) FROM market_holidays")
    count = cur.fetchone()[0]
    cur.execute("SELECT MIN(holiday_date), MAX(holiday_date) FROM market_holidays")
    date_range = cur.fetchone()

    print(f"[OK] Ingested {count} Brazilian holidays into 'market_holidays' table.")
    print(f"     Date range: {date_range[0]} -> {date_range[1]}")

    # Show a few samples
    cur.execute("SELECT holiday_date FROM market_holidays LIMIT 5")
    print(f"     Sample: {[r[0] for r in cur.fetchall()]}")

    con.close()

if __name__ == "__main__":
    ingest()
