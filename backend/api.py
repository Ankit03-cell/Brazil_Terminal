import sys, os
python_dir = os.path.dirname(sys.executable)
if python_dir not in os.environ['PATH']:
    os.environ['PATH'] = python_dir + os.pathsep + os.environ['PATH']
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # Trigger Uvicorn reload

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
import sqlite3
try:
    from config import DB_PATH
except Exception:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'trading_data.db')
from backend.live_monitor_service import LiveMonitorService
from analysis_engine import (
    load_raw_data, get_all_calendar_spreads, get_spreads_on_date,
    get_previous_date_snapshot, calculate_generic_history,
    get_fly_curve_on_date, calculate_generic_fly_history,
    get_index_history, calculate_spread_stats, calculate_rolling_stats,
    get_metadata, load_holidays, calculate_fra_curve
)

app = FastAPI(title="ARB Terminal API", version="2.0")
# Instantiate live monitor lazily at startup to avoid import-time failures
live_monitor_service = None

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup_live_monitor():
    global live_monitor_service
    try:
        if live_monitor_service is None:
            live_monitor_service = LiveMonitorService()
        await live_monitor_service.start()
    except Exception as exc:
        # Do not fail app startup if live monitor cannot start; expose degraded snapshot instead
        live_monitor_service = None
        print(f"[WARN] LiveMonitorService failed to start: {exc}")


@app.on_event("shutdown")
async def shutdown_live_monitor():
    global live_monitor_service
    try:
        if live_monitor_service is not None:
            await live_monitor_service.stop()
    except Exception:
        pass

# ─── Load data once at startup ─────────────────────────────────────
print("[INFO] Loading market data...")
DF_RAW = load_raw_data()
S12 = get_all_calendar_spreads(DF_RAW, tenor_years=1)
S24 = get_all_calendar_spreads(DF_RAW, tenor_years=2)
print(f"[OK] Loaded {len(DF_RAW)} records. 12M spreads: {len(S12)}, 24M spreads: {len(S24)}")
HOLIDAYS = load_holidays()
print(f"[OK] Loaded {len(HOLIDAYS)} Brazilian holidays for FRA engine.")

def _get_spreads(tenor):
    return S12 if tenor == 12 else S24

# ─── API Endpoints ─────────────────────────────────────────────────

def _resolve_date(date_str, spreads):
    if not date_str:
        return DF_RAW['date'].max().strftime('%Y-%m-%d')
    target_dt = pd.to_datetime(date_str)
    max_dt = DF_RAW['date'].max()
    if target_dt > max_dt:
        return max_dt.strftime('%Y-%m-%d')
    if get_spreads_on_date(spreads, date_str):
        return date_str
    for i in range(1, 14):
        test_dt = target_dt - pd.Timedelta(days=i)
        if get_spreads_on_date(spreads, test_dt):
            return test_dt.strftime('%Y-%m-%d')
    return DF_RAW['date'].max().strftime('%Y-%m-%d')

def _resolve_fra_date(date_str):
    """Resolve FRA date to latest available market date on/before requested date."""
    max_dt = DF_RAW['date'].max()
    if not date_str:
        return max_dt.strftime('%Y-%m-%d')

    target_dt = pd.to_datetime(date_str)
    if target_dt > max_dt:
        target_dt = max_dt

    available_dates = DF_RAW.loc[DF_RAW['date'] <= target_dt, 'date']
    if available_dates.empty:
        return max_dt.strftime('%Y-%m-%d')
    return available_dates.max().strftime('%Y-%m-%d')

@app.get("/api/metadata")
def api_metadata():
    # Read metadata live from DB so UI "Today" always reflects latest imported date.
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total_records = cur.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0]
    total_contracts = cur.execute("SELECT COUNT(DISTINCT contract_name) FROM market_prices").fetchone()[0]
    min_date, max_date = cur.execute("SELECT MIN(date), MAX(date) FROM market_prices").fetchone()
    con.close()
    return {
        'total_records': int(total_records),
        'total_contracts': int(total_contracts),
        'min_date': pd.to_datetime(min_date).isoformat() if min_date else None,
        'max_date': pd.to_datetime(max_date).isoformat() if max_date else None,
    }


@app.get("/api/market-snapshot")
async def api_market_snapshot():
    # Return a degraded snapshot if the live monitor service failed to start
    if live_monitor_service is None:
        return {
            "as_of": None,
            "connected": False,
            "latency_ms": None,
            "last_success_at": None,
            "status": "unavailable",
            "error_message": "Live monitor service unavailable",
            "groups": {"3M": {"spreads": [], "flies": []}, "6M": {"spreads": [], "flies": []}, "12M": {"spreads": [], "flies": []}, "24M": {"spreads": [], "flies": []}},
        }
    return await live_monitor_service.get_snapshot()

@app.get("/api/spreads")
def api_spreads(tenor: int = 12, date: str = None, show_all: bool = False):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    req_date = date if date else valid_date
    snap = get_spreads_on_date(spreads, valid_date, filter_date=req_date)
    if not show_all:
        snap = [s for s in snap if 'ODF' in s['pair']]
    prev = get_previous_date_snapshot(spreads, valid_date)
    prev_map = {p['pair']: p['value'] for p in prev}
    for s in snap:
        s['chg'] = round(s['value'] - prev_map.get(s['pair'], s['value']), 4)
    return {"data": snap, "date": valid_date}

@app.get("/api/spreads/history")
def api_spread_history(
    tenor: int = 12, date: str = None, lookback: int = 252,
    rank: int = 1, filter: str = "F"
):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    month_filter = filter if filter != "all" else None
    result = calculate_rolling_stats(spreads, None, valid_date, lookback, rank=rank, month_filter=month_filter)
    idx = get_index_history(DF_RAW, "BZDIOVRA Index", valid_date, lookback)
    return {"data": result['history'], "bands": result['bands'], "index": idx, "date": valid_date}

@app.get("/api/spreads/past")
def api_spreads_past(tenor: int = 12, date: str = None, n1: int = 5, n2: int = 21, n3: int = 63, show_all: bool = False):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    req_date = date if date else valid_date
    target_dt = pd.to_datetime(valid_date)
    result = {}
    for label, days in [("n3", n3), ("n2", n2), ("n1", n1)]:
        past_date = target_dt - pd.Timedelta(days=days)
        snap = get_spreads_on_date(spreads, past_date, filter_date=req_date)
        if not show_all:
            snap = [s for s in snap if 'ODF' in s['pair']]
        result[label] = {"data": snap, "date": past_date.strftime('%Y-%m-%d')}
    return result

@app.get("/api/flies")
def api_flies(tenor: int = 12, date: str = None, show_all: bool = False):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    req_date = date if date else valid_date
    fly_tenor = 2 if tenor == 24 else 1
    snap = get_fly_curve_on_date(spreads, valid_date, fly_tenor=fly_tenor, show_all=show_all, filter_date=req_date)
    prev_snap = []
    for i in range(1, 10):
        prev_date = pd.to_datetime(valid_date) - pd.Timedelta(days=i)
        prev_snap = get_fly_curve_on_date(spreads, prev_date, fly_tenor=fly_tenor, show_all=show_all)
        if prev_snap:
            break
    prev_map = {p['name']: p['value'] for p in prev_snap}
    for s in snap:
        s['chg'] = round(s['value'] - prev_map.get(s['name'], s['value']), 4)
    return {"data": snap, "date": valid_date}

@app.get("/api/flies/past")
def api_flies_past(tenor: int = 12, date: str = None, n1: int = 5, n2: int = 21, n3: int = 63, show_all: bool = False):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    req_date = date if date else valid_date
    fly_tenor = 2 if tenor == 24 else 1
    target_dt = pd.to_datetime(valid_date)
    result = {}
    for label, days in [("n3", n3), ("n2", n2), ("n1", n1)]:
        past_date = target_dt - pd.Timedelta(days=days)
        snap = get_fly_curve_on_date(spreads, past_date, fly_tenor=fly_tenor, show_all=show_all, filter_date=req_date)
        result[label] = {"data": snap, "date": past_date.strftime('%Y-%m-%d')}
    return result

@app.get("/api/flies/history")
def api_fly_history(
    tenor: int = 12, date: str = None, lookback: int = 252,
    rank: int = 1, filter: str = "F"
):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    fly_tenor = 2 if tenor == 24 else 1
    month_filter = filter if filter != "all" else None
    history = calculate_generic_fly_history(spreads, valid_date, lookback, rank=rank, fly_tenor=fly_tenor, month_filter=month_filter)
    idx = get_index_history(DF_RAW, "BZDIOVRA Index", valid_date, lookback)
    return {"data": history, "index": idx, "date": valid_date}

@app.get("/api/stats")
def api_stats(tenor: int = 12, date: str = None, lookback: int = 252):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    stats = calculate_spread_stats(spreads, valid_date, lookback)
    return {"data": stats, "date": valid_date}

@app.get("/api/export")
def api_export(tenor: int = 12, date: str = None, lookback: int = 252):
    spreads = _get_spreads(tenor)
    valid_date = _resolve_date(date, spreads)
    stats = calculate_spread_stats(spreads, valid_date, lookback)
    df = pd.DataFrame(stats)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(buf, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename=arb_spreads_{tenor}M_{date}.csv"})

# ─── FRA Endpoints ─────────────────────────────────────────────────

@app.get("/api/fra")
def api_fra(date: str = None):
    """Two-tier FRA calculation: spot DI rates + implied forward rates."""
    valid_date = _resolve_fra_date(date)
    return calculate_fra_curve(DF_RAW, valid_date, HOLIDAYS)

@app.get("/api/fra/spreads")
def api_fra_spreads_on_date(tenor: int = 3, f_only: bool = False, date: str = None):
    """Fetch FRA spread curve for a specific date and tenor."""
    from analysis_engine import get_fra_spreads_on_date
    valid_date = _resolve_fra_date(date)
    return {"data": get_fra_spreads_on_date(valid_date, tenor, f_only), "date": valid_date}

@app.get("/api/fra/spreads/past")
def api_fra_spreads_past(tenor: int = 3, f_only: bool = False, date: str = None, n1: int = 5, n2: int = 21, n3: int = 63):
    """Fetch historical FRA spreads for n1, n2, n3 offsets."""
    from analysis_engine import get_fra_spreads_on_date
    valid_date = _resolve_fra_date(date)
            
    target_dt = pd.to_datetime(valid_date)
    result = {}
    
    # Try finding the exact date, or walk backwards a few days if holiday/weekend
    # Since sqlite queries are fast, we can try up to 5 times
    for label, days in [("n3", n3), ("n2", n2), ("n1", n1)]:
        past_dt = target_dt - pd.Timedelta(days=days)
        
        found_data = []
        found_dt_str = ""
        for offset in range(5):
            test_dt = past_dt - pd.Timedelta(days=offset)
            test_dt_str = test_dt.strftime('%Y-%m-%d')
            snap = get_fra_spreads_on_date(test_dt_str, tenor, f_only)
            if snap:
                found_data = snap
                found_dt_str = test_dt_str
                break
        
        result[label] = {"data": found_data, "date": found_dt_str}
        
    return result

@app.get("/api/fra/spreads/history")
def api_fra_spreads_history(tenor: int = 12, f_only: bool = False, max_generics: int = 10, date: str = None, lookback: int = 180):
    """Fetch generic spread history."""
    from analysis_engine import get_fra_generic_history
    valid_date = _resolve_fra_date(date)
    return get_fra_generic_history('FRA_spreads', tenor, f_only, valid_date, lookback, max_generics)

@app.get("/api/fra/flies/history")
def api_fra_flies_history(tenor: int = 12, f_only: bool = False, max_generics: int = 10, date: str = None, lookback: int = 180):
    """Fetch generic fly history."""
    from analysis_engine import get_fra_generic_history
    valid_date = _resolve_fra_date(date)
    return get_fra_generic_history('FRA_flies', tenor, f_only, valid_date, lookback, max_generics)

# ─── Serve Frontend ────────────────────────────────────────────────
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

# Mount static files (CSS, JS, etc.)
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
