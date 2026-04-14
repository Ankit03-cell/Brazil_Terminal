"""
analysis_engine.py — Core analysis functions for ARB Terminal.
Provides dict-based interfaces for the FastAPI backend (backend/api.py).
"""
import os
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# ─── Constants ─────────────────────────────────────────────────
MONTH_MAP = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}

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
    engine = create_engine(f'sqlite:///{DB_PATH}')
    df = pd.read_sql("SELECT * FROM market_prices", engine)
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
