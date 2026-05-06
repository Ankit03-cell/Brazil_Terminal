"""
backfill_fra_spreads.py - Tier 2: Compute FRA Spreads & Flies from saved FRA base rates.

Spread = FRA_base[i] - FRA_base[i+1]  (consecutive pairs within same tenor)
Fly    = Spread[i] - Spread[i+1]       (consecutive spreads within same tenor)

Saves results to FRA_spreads and FRA_flies tables.
"""
import os
import sqlite3
from analysis_engine import get_target_contract, parse_contract_details, MONTH_MAP

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trading_data.db')

def build_spreads_and_flies():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # ── Step 1: Load all FRA base rates, ordered properly ──
    print("[1] Loading FRA base rates from database...")
    rows = cur.execute("""
        SELECT observation_date, front_contract, back_contract, tenor_months, fra_base_new_r
        FROM FRA
        ORDER BY observation_date, tenor_months, front_contract
    """).fetchall()

    print(f"    Loaded {len(rows)} FRA base records.")

    # Group by (observation_date, tenor_months)
    from collections import defaultdict
    grouped = defaultdict(list)
    for obs_date, front, back, tenor, fra_rate in rows:
        grouped[(obs_date, tenor)].append({
            'front': front,
            'back': back,
            'fra_rate': fra_rate,
        })

    # ── Step 2: Calculate Spreads ──
    print("[2] Calculating FRA Spreads...")
    spread_records = []

    def _contract_sort_key(contract_name):
        month_code, year_short = parse_contract_details(contract_name)
        if not month_code:
            return 10**12
        return (2000 + year_short) * 100 + MONTH_MAP[month_code]

    for (obs_date, tenor), fra_list in grouped.items():
        fra_map = {f['front']: f for f in fra_list}
        ordered_fronts = sorted(fra_map.keys(), key=_contract_sort_key)
        for leg1_front in ordered_fronts:
            leg2_front = get_target_contract(leg1_front, tenor)
            if not leg2_front or leg2_front not in fra_map:
                continue

            f1 = fra_map[leg1_front]
            f2 = fra_map[leg2_front]
            spread_val = round(f1['fra_rate'] - f2['fra_rate'], 4)

            spread_records.append((
                obs_date,
                tenor,
                f1['front'],   # spread leg 1 front
                f1['back'],    # spread leg 1 back
                f2['front'],   # spread leg 2 front
                f2['back'],    # spread leg 2 back
                f1['fra_rate'],
                f2['fra_rate'],
                spread_val,
            ))

    print(f"    Computed {len(spread_records)} spread records.")

    # ── Step 3: Calculate Flies ──
    print("[3] Calculating FRA Flies...")
    # Group spreads by (obs_date, tenor) to compute flies
    spread_grouped = defaultdict(list)
    for rec in spread_records:
        obs_date, tenor = rec[0], rec[1]
        spread_grouped[(obs_date, tenor)].append(rec)

    fly_records = []
    for (obs_date, tenor), sp_list in spread_grouped.items():
        for i in range(len(sp_list) - 1):
            s1 = sp_list[i]
            s2 = sp_list[i + 1]
            fly_val = round(s1[8] - s2[8], 4)  # spread_val difference

            fly_records.append((
                obs_date,
                tenor,
                s1[2],  # fly leg1 spread front
                s1[3],  # fly leg1 spread back
                s1[4],  # fly center (spread leg2 front)
                s1[5],  # fly center (spread leg2 back)
                s2[4],  # fly leg2 spread front (= leg3 of fly)
                s2[5],  # fly leg2 spread back
                s1[8],  # spread_1 value
                s2[8],  # spread_2 value
                fly_val,
            ))

    print(f"    Computed {len(fly_records)} fly records.")

    # ── Step 4: Save to database ──
    print("[4] Saving to database...")

    cur.execute("DROP TABLE IF EXISTS FRA_spreads")
    cur.execute("""
        CREATE TABLE FRA_spreads (
            observation_date TEXT,
            tenor_months INTEGER,
            leg1_front TEXT,
            leg1_back TEXT,
            leg2_front TEXT,
            leg2_back TEXT,
            fra1_rate REAL,
            fra2_rate REAL,
            spread_value REAL,
            PRIMARY KEY (observation_date, tenor_months, leg1_front, leg2_front)
        )
    """)

    chunk = 50000
    for i in range(0, len(spread_records), chunk):
        cur.executemany("""
            INSERT INTO FRA_spreads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, spread_records[i:i+chunk])

    cur.execute("DROP TABLE IF EXISTS FRA_flies")
    cur.execute("""
        CREATE TABLE FRA_flies (
            observation_date TEXT,
            tenor_months INTEGER,
            wing1_front TEXT,
            wing1_back TEXT,
            belly_front TEXT,
            belly_back TEXT,
            wing2_front TEXT,
            wing2_back TEXT,
            spread1_value REAL,
            spread2_value REAL,
            fly_value REAL,
            PRIMARY KEY (observation_date, tenor_months, wing1_front, belly_front, wing2_front)
        )
    """)

    for i in range(0, len(fly_records), chunk):
        cur.executemany("""
            INSERT INTO FRA_flies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, fly_records[i:i+chunk])

    con.commit()

    sp_count = cur.execute("SELECT COUNT(*) FROM FRA_spreads").fetchone()[0]
    fl_count = cur.execute("SELECT COUNT(*) FROM FRA_flies").fetchone()[0]
    con.close()

    print(f"[OK] Saved {sp_count} spreads to 'FRA_spreads' table.")
    print(f"[OK] Saved {fl_count} flies to 'FRA_flies' table.")

if __name__ == "__main__":
    build_spreads_and_flies()
