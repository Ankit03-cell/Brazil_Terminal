import pandas as pd
import re
import streamlit as st

MONTH_MAP = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6, 
             'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}

def parse_contract_details(contract_name):
    match = re.search(r'OD([FGHJKMNQUVXZ])(\d{2})', str(contract_name))
    if match: return match.group(1), int(match.group(2))
    return None, None

def get_maturity_date(contract_name):
    month_code, year_short = parse_contract_details(contract_name)
    if month_code:
        year = int("20" + str(year_short))
        return year, MONTH_MAP[month_code]
    return None, None

@st.cache_data
def get_all_calendar_spreads(df, tenor_years=1):
    """Heavy calculation cached: only runs once per tenor."""
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

def get_spreads_on_date(all_spreads, target_date):
    """Filtered lookup: very fast."""
    data_at_date = []
    target_dt = pd.to_datetime(target_date)
    for pair_name, series in all_spreads.items():
        if target_dt in series.index:
            front_c = pair_name.split('-')[0]
            m_year, m_month = get_maturity_date(front_c)
            if m_year and ((m_year > target_dt.year) or (m_year == target_dt.year and m_month >= target_dt.month)):
                data_at_date.append({
                    "Spread Pair": pair_name, "Value": series.loc[target_dt],
                    "sort_key": (m_year * 100) + m_month
                })
    res_df = pd.DataFrame(data_at_date)
    return res_df.sort_values("sort_key") if not res_df.empty else res_df

@st.cache_data
def calculate_generic_history(all_spreads, end_date, lookback_days, rank=1, month_filter=None):
    """Cached rolling logic: pre-calculates the generic series."""
    end_dt = pd.to_datetime(end_date)
    start_dt = end_dt - pd.Timedelta(days=lookback_days)
    all_dates = sorted(set([d for series in all_spreads.values() for d in series.index]))
    relevant_dates = [d for d in all_dates if start_dt <= d <= end_dt]
    
    generic_series = []
    for d in relevant_dates:
        daily_options = []
        for pair, series in all_spreads.items():
            if d in series.index:
                front_c = pair.split('-')[0]
                m_code, y_short = parse_contract_details(front_c)
                if month_filter is None or m_code == month_filter:
                    y, m = int("20" + str(y_short)), MONTH_MAP[m_code]
                    if (y > d.year) or (y == d.year and m >= d.month):
                        daily_options.append({'pair': pair, 'value': series.loc[d], 'sort_key': (y * 100) + m})
        if daily_options:
            daily_options.sort(key=lambda x: x['sort_key'])
            if len(daily_options) >= rank:
                sel = daily_options[rank-1]
                generic_series.append({'date': d, 'value': sel['value'], 'actual_ticker': sel['pair']})
    return pd.DataFrame(generic_series)

def get_fly_curve_on_date(all_spreads_tenor, target_date, fly_tenor=1, show_all=False):
    """Uses pre-passed spreads to avoid re-calculation."""
    snap_tenor = get_spreads_on_date(all_spreads_tenor, target_date)
    if snap_tenor.empty: return pd.DataFrame()
    fly_data = []
    for i in range(len(snap_tenor)):
        front_s = snap_tenor.iloc[i]
        mid_node = front_s['Spread Pair'].split('-')[1].split(' ')[0]
        back_pair = next((p for p in snap_tenor['Spread Pair'] if p.startswith(mid_node)), None)
        if back_pair:
            back_val = snap_tenor[snap_tenor['Spread Pair'] == back_pair]['Value'].iloc[0]
            lead_c = front_s['Spread Pair'].split('-')[0].split(' ')[0]
            if not show_all and 'ODF' not in lead_c: continue
            fly_data.append({"Fly Name": f"{lead_c} {fly_tenor*12}M Fly", "Value": front_s['Value'] - back_val, "sort_key": front_s['sort_key']})
    return pd.DataFrame(fly_data)

def calculate_generic_fly_history(all_spreads_ten, target_date, lookback, rank=1, fly_tenor=1, month_filter=None):
    """Calculates Fly history using pre-calculated spreads."""
    jump = fly_tenor if month_filter == 'F' else (fly_tenor * 12)
    df_f = calculate_generic_history(all_spreads_ten, target_date, lookback, rank=rank, month_filter=month_filter)
    df_b = calculate_generic_history(all_spreads_ten, target_date, lookback, rank=rank + jump, month_filter=month_filter)
    if df_f.empty or df_b.empty: return pd.DataFrame()
    merged = pd.merge(df_f, df_b, on='date', suffixes=('_f', '_b'))
    merged['value'] = merged['value_f'] - merged['value_b']
    merged['actual_ticker'] = f"({merged['actual_ticker_f']}) - ({merged['actual_ticker_b']})"
    return merged[['date', 'value', 'actual_ticker']]

@st.cache_data
def get_index_history(df, index_name, end_date, lookback_days):
    end_dt, start_dt = pd.to_datetime(end_date), pd.to_datetime(end_date) - pd.Timedelta(days=lookback_days)
    idx_df = df[df['contract_name'] == index_name].sort_values('date')
    return idx_df[(idx_df['date'] >= start_dt) & (idx_df['date'] <= end_dt)].rename(columns={'price': 'index_value'})

def get_value_change(series, target_date, lookback_days):
    """Calculates the bps change from a historical window to today."""
    target_dt = pd.to_datetime(target_date)
    past_dt = target_dt - pd.Timedelta(days=lookback_days)
    
    # Find the closest available date in history
    if target_dt in series.index:
        current_val = series.loc[target_dt]
        past_series = series[series.index <= past_dt]
        if not past_series.empty:
            past_val = past_series.iloc[-1]
            return current_val - past_val
    return None