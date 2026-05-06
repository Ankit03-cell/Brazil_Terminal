"""
analysis_engine.py — Core analysis functions for ARB Terminal.
Provides dict-based interfaces for the FastAPI backend (backend/api.py).
"""
import os
import re
import pandas as pd
import numpy as np
try:
    from sqlalchemy import create_engine
except Exception:
    create_engine = None

# ─── Constants ─────────────────────────────────────────────────
MONTH_MAP = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}

# Prefer centralized config if available, fallback to local DB path
try:
    from config import DB_PATH
except Exception:
    DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'trading_data.db')


# ─── Helpers ───────────────────────────────────────────────────
def parse_contract_details(contract_name):
    match = re.search(r'OD([FGHJKMNQUVXZ])(\d{2})', str(contract_name))
    if match:
        return match.group(1), int(match.group(2))
    return None, None


def get_maturity_date(contract_name):
    month_code, year_short = parse_contract_details(contract_name)
    if month_code:
        year = int("20" + str(year_short))
        return year, MONTH_MAP[month_code]
    return None, None


# ─── Data Loading ──────────────────────────────────────────────
def load_raw_data():
    """Load all market data from the SQLite database."""
    if create_engine is not None:
        engine = create_engine(f'sqlite:///{DB_PATH}')
        df = pd.read_sql("SELECT * FROM market_prices", engine)
    else:
        import sqlite3
        con = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM market_prices", con)
        con.close()
    df['date'] = pd.to_datetime(df['date'])
    return df


def get_metadata(df):
    """Return summary metadata about the loaded dataset."""
    return {
        'total_records': int(len(df)),
        'total_contracts': int(df['contract_name'].nunique()),
        'min_date': df['date'].min().isoformat(),
        'max_date': df['date'].max().isoformat(),
    }


# ─── Spread Calculations ──────────────────────────────────────
def get_all_calendar_spreads(df, tenor_years=1):
    """Calculates (Back - Front) * 100 for a given tenor (1Y, 2Y, etc).
    Returns dict: { 'pairName': pd.Series(date->value), ... }
    """
    contracts = df['contract_name'].unique()
    all_spreads = {}
    for c in contracts:
        month, year = parse_contract_details(c)
        if month:
            target_year = year + tenor_years
            target = f"OD{month}{target_year:02} Comdty"
            if target in contracts:
                df_front = df[df['contract_name'] == c].set_index('date')['price']
                df_back = df[df['contract_name'] == target].set_index('date')['price']
                spread = ((df_back - df_front) * 100).dropna()
                if not spread.empty:
                    all_spreads[f"{c}-{target}"] = spread
    return all_spreads


def get_spreads_on_date(all_spreads, target_date, filter_date=None):
    """Returns list of dicts: [{pair, value, sort_key}, ...] sorted by maturity."""
    if filter_date is None:
        filter_date = target_date
    data_at_date = []
    target_dt = pd.to_datetime(target_date)
    filter_dt = pd.to_datetime(filter_date)
    for pair_name, series in all_spreads.items():
        if target_dt in series.index:
            front_c = pair_name.split('-')[0]
            m_year, m_month = get_maturity_date(front_c)
            if m_year and ((m_year > filter_dt.year) or
                           (m_year == filter_dt.year and m_month > filter_dt.month)):
                data_at_date.append({
                    'pair': pair_name,
                    'value': round(float(series.loc[target_dt]), 4),
                    'sort_key': (m_year * 100) + m_month
                })
    return sorted(data_at_date, key=lambda x: x['sort_key'])


def get_previous_date_snapshot(all_spreads, date):
    """Find the previous business day's spread snapshot."""
    for i in range(1, 10):
        prev_date = pd.to_datetime(date) - pd.Timedelta(days=i)
        snap = get_spreads_on_date(all_spreads, prev_date)
        if snap:
            return snap
    return []


def find_closest_historical_curve(target_snap, all_spreads, ignore_date=None):
    """
    Computes Mean Squared Error (MSE) Euclidean distance between the target snapshot 
    (live curve) and all historical dates, returning the date with the lowest mathematically defined penalty.
    """
    import numpy as np
    
    # Restrict matching specifically to 'ODF' generic term structure pairs
    target_f = {s['pair']: s['value'] for s in target_snap if 'ODF' in s['pair']}
    if not target_f:
        return None
        
    pairs = list(target_f.keys())
    target_vec = np.array([target_f[p] for p in pairs])
    
    df_spreads = pd.DataFrame(all_spreads)
    
    min_error = float('inf')
    best_date = None
    
    ignore_dt = pd.to_datetime(ignore_date) if ignore_date else None
    
    for date, row in df_spreads.iterrows():
        if ignore_dt and date >= ignore_dt:
            continue
            
        hist_vec = []
        valid = True
        for p in pairs:
            val = row.get(p)
            if pd.isna(val):
                valid = False
                break
            hist_vec.append(val)
            
        if valid:
            mse = np.mean((target_vec - np.array(hist_vec))**2)
            if mse < min_error:
                min_error = mse
                best_date = date
                
    if best_date:
        return best_date.strftime('%Y-%m-%d')
    return None


# ─── Generic History ───────────────────────────────────────────
def calculate_generic_history(all_spreads, end_date, lookback_days, rank=1, month_filter=None):
    """Creates a constant maturity series by rolling the Nth available contract.
    Returns list of dicts: [{date, value, actual_ticker}, ...]
    """
    end_dt = pd.to_datetime(end_date)
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    all_dates = sorted(set(d for series in all_spreads.values() for d in series.index))
    relevant_dates = [d for d in all_dates if start_dt <= d <= end_dt]

    generic_series = []
    for d in relevant_dates:
        daily_options = []
        for pair, series in all_spreads.items():
            if d in series.index:
                front_c = pair.split('-')[0]
                m_code, y_short = parse_contract_details(front_c)
                if month_filter is None or m_code == month_filter:
                    y = int("20" + str(y_short))
                    m = MONTH_MAP[m_code]
                    if (y > d.year) or (y == d.year and m > d.month):
                        daily_options.append({
                            'pair': pair,
                            'value': float(series.loc[d]),
                            'sort_key': (y * 100) + m
                        })
        if daily_options:
            daily_options.sort(key=lambda x: x['sort_key'])
            if len(daily_options) >= rank:
                sel = daily_options[rank - 1]
                generic_series.append({
                    'date': d.isoformat(),
                    'value': round(sel['value'], 4),
                    'actual_ticker': sel['pair']
                })
    return generic_series


def calculate_rolling_stats(all_spreads, _unused, end_date, lookback_days, rank=1, month_filter=None):
    """Calculate generic history with rolling MA and Bollinger bands.
    Returns: {'history': [...], 'bands': {'ma_20': [...], 'ma_60': [...], ...}}
    """
    history = calculate_generic_history(
        all_spreads, end_date, lookback_days, rank=rank, month_filter=month_filter
    )
    if not history:
        return {'history': [], 'bands': {}}

    values = [h['value'] for h in history]
    s = pd.Series(values)
    bands = {}

    if len(values) >= 20:
        ma20 = s.rolling(20).mean()
        std20 = s.rolling(20).std()
        bands['ma_20'] = [None if pd.isna(v) else round(v, 4) for v in ma20]
        bands['upper_20'] = [None if pd.isna(v) else round(v, 4) for v in (ma20 + 2 * std20)]
        bands['lower_20'] = [None if pd.isna(v) else round(v, 4) for v in (ma20 - 2 * std20)]
    if len(values) >= 60:
        ma60 = s.rolling(60).mean()
        bands['ma_60'] = [None if pd.isna(v) else round(v, 4) for v in ma60]

    return {'history': history, 'bands': bands}


# ─── Fly Calculations ─────────────────────────────────────────
def get_fly_curve_on_date(all_spreads, target_date, fly_tenor=1, show_all=False, filter_date=None):
    """Symmetrical Fly: (m, m+T) - (m+T, m+2T).
    Returns list of dicts: [{name, value, sort_key}, ...]
    """
    snap = get_spreads_on_date(all_spreads, target_date, filter_date=filter_date)
    if not snap:
        return []

    snap_map = {s['pair']: s['value'] for s in snap}
    fly_data = []

    for s in snap:
        pair = s['pair']
        # mid_node is the back contract of this spread
        mid_node = pair.split('-')[1].split(' ')[0]
        # Find a spread that starts with mid_node
        back_pair = next((p for p in snap_map if p.startswith(mid_node)), None)
        if back_pair:
            lead_c = pair.split('-')[0].split(' ')[0]
            if not show_all and 'ODF' not in lead_c:
                continue
            fly_data.append({
                'name': f"{lead_c} {fly_tenor * 12}M Fly",
                'value': round(s['value'] - snap_map[back_pair], 4),
                'sort_key': s.get('sort_key', 0)
            })

    return sorted(fly_data, key=lambda x: x.get('sort_key', 0))


def calculate_generic_fly_history(all_spreads, target_date, lookback, rank=1, fly_tenor=1, month_filter=None):
    """Historical symmetrical curvature.
    Returns list of dicts: [{date, value, actual_ticker}, ...]
    """
    jump = fly_tenor if month_filter == 'F' else (fly_tenor * 12)

    hist_f = calculate_generic_history(
        all_spreads, target_date, lookback, rank=rank, month_filter=month_filter
    )
    hist_b = calculate_generic_history(
        all_spreads, target_date, lookback, rank=rank + jump, month_filter=month_filter
    )

    if not hist_f or not hist_b:
        return []

    df_f = pd.DataFrame(hist_f)
    df_b = pd.DataFrame(hist_b)
    merged = pd.merge(df_f, df_b, on='date', suffixes=('_f', '_b'))

    result = []
    for _, row in merged.iterrows():
        result.append({
            'date': row['date'],
            'value': round(row['value_f'] - row['value_b'], 4),
            'actual_ticker': f"({row['actual_ticker_f']}) - ({row['actual_ticker_b']})"
        })
    return result


# ─── Index / Overlay ──────────────────────────────────────────
def get_index_history(df, index_name, end_date, lookback_days):
    """Returns list of dicts: [{date, value}, ...]"""
    end_dt = pd.to_datetime(end_date)
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    idx_df = df[df['contract_name'] == index_name].sort_values('date')
    filtered = idx_df[(idx_df['date'] >= start_dt) & (idx_df['date'] <= end_dt)]
    return [
        {'date': row['date'].isoformat(), 'value': round(float(row['price']), 4)}
        for _, row in filtered.iterrows()
    ]


# ─── Stats ─────────────────────────────────────────────────────
def calculate_spread_stats(all_spreads, date, lookback):
    """Calculate z-scores, percentiles, min/max for all active spreads on a date."""
    target_dt = pd.to_datetime(date)
    start_dt = target_dt - pd.Timedelta(days=lookback)
    snap = get_spreads_on_date(all_spreads, date)

    stats = []
    for s in snap:
        pair = s['pair']
        if pair not in all_spreads:
            continue

        series = all_spreads[pair]
        hist = series[(series.index >= start_dt) & (series.index <= target_dt)]
        if len(hist) < 2:
            continue

        current = float(series.loc[target_dt]) if target_dt in series.index else None
        if current is None:
            continue

        mean_val = float(hist.mean())
        std_val = float(hist.std())
        z_score = (current - mean_val) / std_val if std_val > 0 else 0.0
        percentile = float((hist < current).sum()) / len(hist) * 100

        # 1-day change
        prev_dates = [d for d in hist.index if d < target_dt]
        chg_1d = (current - float(hist.loc[prev_dates[-1]])) if prev_dates else 0.0

        stats.append({
            'pair': pair,
            'value': round(current, 2),
            'chg_1d': round(chg_1d, 2),
            'z_score': round(z_score, 2),
            'percentile': round(percentile, 1),
            'min': round(float(hist.min()), 2),
            'max': round(float(hist.max()), 2),
            'mean': round(mean_val, 2),
        })
    return stats


# ═══════════════════════════════════════════════════════════════
#  FRA (Forward Rate Agreement) Engine
# ═══════════════════════════════════════════════════════════════

def load_holidays():
    """Load Brazilian holidays from the market_holidays table.
    Returns a set of datetime.date objects for fast lookup.
    """
    import sqlite3
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT holiday_date FROM market_holidays")
    holidays = set()
    for row in cur.fetchall():
        try:
            holidays.add(pd.to_datetime(row[0]).date())
        except Exception:
            pass
    con.close()
    return holidays


def is_business_day(d, holidays_set):
    """Check if a date is a business day (not weekend, not holiday)."""
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if d in holidays_set:
        return False
    return True


def adjust_to_business_day(d, holidays_set):
    """Roll a date forward to the next business day if it falls on
    a weekend or a Brazilian holiday."""
    from datetime import timedelta
    while not is_business_day(d, holidays_set):
        d = d + timedelta(days=1)
    return d


def get_contract_expiry(contract_name, holidays_set):
    """DI1 contracts expire on the 1st business day of their expiry month.
    Returns a datetime.date with holiday/weekend adjustment applied.
    """
    from datetime import date
    month_code, year_short = parse_contract_details(contract_name)
    if not month_code:
        return None
    year = 2000 + year_short
    month = MONTH_MAP[month_code]
    raw_expiry = date(year, month, 1)
    return adjust_to_business_day(raw_expiry, holidays_set)


def count_business_days(start_date, end_date, holidays_set):
    """Count business days between start_date (exclusive) and end_date (inclusive).
    Both inputs should be datetime.date objects.
    Returns integer count — this is the raw D used in power calculations.
    """
    from datetime import timedelta
    if start_date >= end_date:
        return 0
    count = 0
    current = start_date + timedelta(days=1)
    while current <= end_date:
        if is_business_day(current, holidays_set):
            count += 1
        current += timedelta(days=1)
    return count


def count_calendar_days(start_date, end_date):
    """Count raw calendar days between start_date and end_date."""
    if start_date >= end_date:
        return 0
    return (end_date - start_date).days

def get_target_contract(contract_name, months_offset):
    """Get the name of the contract exactly months_offset ahead."""
    month_code, year_short = parse_contract_details(contract_name)
    if not month_code:
        return None
    current_month = MONTH_MAP[month_code]
    total_months = current_month + months_offset - 1
    target_month = (total_months % 12) + 1
    target_year = year_short + (total_months // 12)
    
    target_month_code = 'F'
    for k, v in MONTH_MAP.items():
        if v == target_month:
            target_month_code = k
            break
            
    return f"OD{target_month_code}{target_year:02d} Comdty"

def calculate_fra_curve(df, target_date, holidays_set):
    """
    FRA Calculation Engine using raw calendar days.

    Tier 1 — Spot DI rates for each active OD contract on target_date.
        Computes adjusted expiry and raw calendar days D.

    Tier 2 — FRA rates between tenors that are 3, 6, 9, 12, and 24 months apart.
        FRA_base = 100 * [ ( (1 + R2/100)^D2 / (1 + R1/100)^D1 )^(1 / (D2 - D1)) - 1 ]

    Returns dict with 'spot_rates' and 'fra_rates' lists.
    """
    target_dt = pd.to_datetime(target_date)
    target_d = target_dt.date()

    # Get all unique OD contracts with prices on target date
    day_df = df[(df['date'] == target_dt) & (df['contract_name'].str.startswith('OD'))].copy()
    if day_df.empty:
        return {'spot_rates': [], 'fra_rates': [], 'as_of': str(target_date)}

    # Build spot rate table (Tier 1)
    spot_rows = []
    for _, row in day_df.iterrows():
        contract = row['contract_name']
        rate = float(row['price'])

        expiry = get_contract_expiry(contract, holidays_set)
        if expiry is None or expiry <= target_d:
            continue  # Expired

        bd = count_business_days(target_d, expiry, holidays_set)
        if bd <= 0:
            continue

        month_code, year_short = parse_contract_details(contract)
        sort_key = (2000 + year_short) * 100 + MONTH_MAP[month_code]

        spot_rows.append({
            'contract': contract,
            'rate': round(rate, 4),
            'expiry': expiry.isoformat(),
            'bus_days': bd, # Accurately representing Brazil Business Days
            'compound_factor': 0.0, # Deprecated in new model 
            'sort_key': sort_key,
        })

    spot_rows.sort(key=lambda x: x['sort_key'])
    spot_dict = {s['contract']: s for s in spot_rows}

    # Build FRA table (Tier 2) — specific tenors
    fra_rows = []
    target_tenors = [3, 6, 9, 12, 24]
    
    for s1 in spot_rows:
        for tenor in target_tenors:
            target_name = get_target_contract(s1['contract'], tenor)
            if target_name in spot_dict:
                s2 = spot_dict[target_name]
                
                d1 = s1['bus_days']
                d2 = s2['bus_days']
                if d2 <= d1:
                    continue
                
                r1 = s1['rate'] / 100.0
                r2 = s2['rate'] / 100.0
                
                # Equation: FRA_base = 100 * [ ( (1 + r2)^D2 / (1 + r1)^D1 )^(1 / (D2 - D1)) - 1 ]
                # Mathematically transformed to prevent float overflow on massive D2 bounds:
                base_r2 = (1 + r2) ** (d2 / (d2 - d1))
                base_r1 = (1 + r1) ** (d1 / (d2 - d1))
                fra_rate = ((base_r2 / base_r1) - 1) * 100

                fra_rows.append({
                    'front': s1['contract'],
                    'back': s2['contract'],
                    'front_expiry': s1['expiry'],
                    'back_expiry': s2['expiry'],
                    'front_rate': s1['rate'],
                    'back_rate': s2['rate'],
                    'front_bd': d1,
                    'back_bd': d2,
                    'period_bd': d2 - d1,
                    'fra_rate': round(fra_rate, 4),
                    'tenor_months': tenor
                })

    return {
        'spot_rates': spot_rows,
        'fra_rates': fra_rows,
        'as_of': str(target_date),
    }

def _build_fra_spread_curve_from_rows(rows, tenor_months, f_only=False):
    """Build tenor-stepped FRA spread rows from FRA base-rate rows.

    `rows` format: [(front_contract, back_contract, fra_base_new_r), ...]
    """
    def _contract_sort_key(contract_name):
        month_code, year_short = parse_contract_details(contract_name)
        if not month_code:
            return 10**12
        return (2000 + year_short) * 100 + MONTH_MAP[month_code]

    fra_map = {}
    for front, back, fra_rate in rows:
        fra_map[front] = {
            'front': front,
            'back': back,
            'fra_rate': float(fra_rate),
            'sort_key': _contract_sort_key(front),
        }

    ordered_fronts = sorted(fra_map.keys(), key=_contract_sort_key)
    result = []
    for leg1_front in ordered_fronts:
        if f_only and 'ODF' not in leg1_front:
            continue

        leg2_front = get_target_contract(leg1_front, tenor_months)
        if not leg2_front or leg2_front not in fra_map:
            continue

        fra1 = fra_map[leg1_front]['fra_rate']
        fra2 = fra_map[leg2_front]['fra_rate']
        spread_val = fra1 - fra2

        result.append({
            'leg1_front': leg1_front,
            'leg1_back': fra_map[leg1_front]['back'],
            'leg2_front': leg2_front,
            'leg2_back': fra_map[leg2_front]['back'],
            'fra1_rate': round(fra1, 4),
            'fra2_rate': round(fra2, 4),
            'spread_value': round(spread_val, 4),
            'spread_bps': round(spread_val * 100, 2),
            'sort_key': fra_map[leg1_front]['sort_key'],
        })
    return result


def get_fra_spreads_on_date(target_date, tenor_months, f_only=False):
    """
    Build FRA spread curve for a specific date/tenor directly from FRA base rates.
    For a given tenor T, each spread is:
      FRA(front -> front+T) - FRA((front+T) -> (front+2T))
    This guarantees the gap between spread legs matches the selected tenor.
    """
    import sqlite3

    date_str = pd.to_datetime(target_date).strftime('%Y-%m-%d')
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    query = """
        SELECT front_contract, back_contract, fra_base_new_r
        FROM FRA
        WHERE observation_date = ?
          AND tenor_months = ?
    """
    cur.execute(query, (date_str, tenor_months))
    rows = cur.fetchall()
    con.close()

    result = _build_fra_spread_curve_from_rows(rows, tenor_months, f_only=f_only)
    for row in result:
        row.pop('sort_key', None)
    return result


def get_fra_generic_history(table_name, tenor_months, f_only, end_date, lookback_days, max_generics=3):
    """
    Tier 2 FRA Spreads & Flies Generic History.
    Fetch rolling history of the 1st, 2nd, etc. active constructs within a date range.
    Uses C-level SQLite Window partitions to drastically accelerate retrieval speeds.
    """
    import sqlite3
    
    # Process date bounds safely via string slicing to eliminate pd object initialization overhead
    end_dt = pd.to_datetime(end_date)
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    end_str = end_dt.strftime('%Y-%m-%d')
    start_str = start_dt.strftime('%Y-%m-%d')

    con = sqlite3.connect(DB_PATH)
    # Use FRA base table as single source of truth so history and curve are consistent.
    query = """
        SELECT observation_date, front_contract, back_contract, fra_base_new_r
        FROM FRA
        WHERE tenor_months = ?
          AND observation_date >= ?
          AND observation_date <= ?
        ORDER BY observation_date
    """
    cur = con.cursor()
    cur.execute(query, (tenor_months, start_str, end_str))
    rows = cur.fetchall()
    con.close()

    if not rows:
        return []

    from collections import defaultdict
    fra_by_date = defaultdict(list)
    for obs_date, front, back, fra_rate in rows:
        fra_by_date[obs_date].append((front, back, fra_rate))

    generic_map = {rank: {'name': f'Generic {rank}', 'x': [], 'y': [], 'labels': []}
                   for rank in range(1, max_generics + 1)}

    for obs_date in sorted(fra_by_date.keys()):
        spread_curve = _build_fra_spread_curve_from_rows(
            fra_by_date[obs_date], tenor_months, f_only=f_only
        )
        if table_name == 'FRA_flies':
            curve_rows = []
            for i in range(len(spread_curve) - 1):
                s1 = spread_curve[i]
                s2 = spread_curve[i + 1]
                fly_value = s1['spread_value'] - s2['spread_value']
                curve_rows.append({
                    'label': s1['leg1_front'],
                    'bp_val': round(-fly_value * 100, 2),
                })
        else:
            curve_rows = [{
                'label': s['leg1_front'],
                'bp_val': round(-s['spread_value'] * 100, 2),
            } for s in spread_curve]

        if not curve_rows:
            continue

        for idx, row in enumerate(curve_rows[:max_generics], start=1):
            generic_map[idx]['x'].append(obs_date)
            generic_map[idx]['y'].append(row['bp_val'])
            generic_map[idx]['labels'].append(row['label'])

    return [generic_map[k] for k in sorted(generic_map.keys()) if generic_map[k]['x']]

