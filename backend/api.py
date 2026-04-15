import sys, os
python_dir = os.path.dirname(sys.executable)
if python_dir not in os.environ['PATH']:
    os.environ['PATH'] = python_dir + os.pathsep + os.environ['PATH']
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import io
from backend.live_monitor_service import LiveMonitorService
from analysis_engine import (
    load_raw_data, get_all_calendar_spreads, get_spreads_on_date,
    get_previous_date_snapshot, calculate_generic_history,
    get_fly_curve_on_date, calculate_generic_fly_history,
    get_index_history, calculate_spread_stats, calculate_rolling_stats,
    get_metadata
)

app = FastAPI(title="ARB Terminal API", version="2.0")
live_monitor_service = LiveMonitorService()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup_live_monitor():
    await live_monitor_service.start()


@app.on_event("shutdown")
async def shutdown_live_monitor():
    await live_monitor_service.stop()

# ─── Load data once at startup ─────────────────────────────────────
print("[INFO] Loading market data...")
DF_RAW = load_raw_data()
S12 = get_all_calendar_spreads(DF_RAW, tenor_years=1)
S24 = get_all_calendar_spreads(DF_RAW, tenor_years=2)
print(f"[OK] Loaded {len(DF_RAW)} records. 12M spreads: {len(S12)}, 24M spreads: {len(S24)}")

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

@app.get("/api/metadata")
def api_metadata():
    return get_metadata(DF_RAW)


@app.get("/api/market-snapshot")
async def api_market_snapshot():
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

# ─── Serve Frontend ────────────────────────────────────────────────
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")

@app.get("/")
def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

# Mount static files (CSS, JS, etc.)
if os.path.exists(FRONTEND_DIR):
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")
