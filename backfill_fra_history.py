import os
import sqlite3
import pandas as pd

from analysis_engine import load_raw_data, load_holidays, calculate_fra_curve

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trading_data.db')

def build_historical_fra():
    print("[1] Loading raw market data and holidays...")
    df = load_raw_data()
    holidays = load_holidays()
    
    # Extract unique observation dates directly from the DataFrame
    # df['date'] is a pandas Datetime object because of load_raw_data()
    unique_dates = df['date'].dt.date.unique()
    unique_dates.sort()
    
    records = []
    
    print(f"[2] Calculating FRA history for {len(unique_dates)} dates...")
    for i, target_d in enumerate(unique_dates):
        if i > 0 and i % 500 == 0:
            print(f"    Processed {i} / {len(unique_dates)} dates...")
            
        res = calculate_fra_curve(df, target_d.isoformat(), holidays)
        
        for f in res['fra_rates']:
            records.append((
                target_d.isoformat(),
                f['front'],
                f['back'],
                f['tenor_months'],
                f['front_rate'],
                f['back_rate'],
                f['front_bd'], # Using literal calendar days mapped previously
                f['back_bd'],
                f['period_bd'],
                f['fra_rate']
            ))
            
    print(f"[3] Total pairs computed: {len(records)}. Saving to database table 'FRA'...")
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    
    cur.execute("DROP TABLE IF EXISTS FRA")
    cur.execute("""
        CREATE TABLE FRA (
            observation_date TEXT,
            front_contract TEXT,
            back_contract TEXT,
            tenor_months INTEGER,
            front_rate REAL,
            back_rate REAL,
            d1_cal_days INTEGER,
            d2_cal_days INTEGER,
            period_cal_days INTEGER,
            fra_base_new_r REAL,
            PRIMARY KEY (observation_date, front_contract, back_contract)
        )
    """)
    
    chunk_size = 50000
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i+chunk_size]
        cur.executemany("""
            INSERT INTO FRA (
                observation_date, front_contract, back_contract, tenor_months,
                front_rate, back_rate, d1_cal_days, d2_cal_days, period_cal_days, fra_base_new_r
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, chunk)
    
    con.commit()
    count = cur.execute("SELECT COUNT(*) FROM FRA").fetchone()[0]
    con.close()
    
    print(f"[OK] Successfully saved {count} historical FRA equations to 'FRA' table.")

if __name__ == "__main__":
    build_historical_fra()
