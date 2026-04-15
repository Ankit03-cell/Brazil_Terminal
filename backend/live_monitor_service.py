"""
live_monitor_service.py - Async Excel polling for Curve Monitor snapshots.
"""
from __future__ import annotations

import asyncio
import os
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import xlwings as xw
except Exception:  # pragma: no cover - handled in runtime status
    xw = None


InstrumentMapping = Tuple[str, str, str, str, str]


class LiveMonitorService:
    """Polls live values from Excel and exposes cached market snapshots."""

    INSTRUMENT_COLUMNS: List[InstrumentMapping] = [
        ("12M", "spreads", "A", "B", "C"),
        ("12M", "flies", "D", "E", "F"),
        ("24M", "spreads", "H", "I", "J"),
        ("24M", "flies", "K", "L", "M"),
        ("6M", "spreads", "O", "P", "Q"),
        ("6M", "flies", "R", "S", "T"),
        ("3M", "spreads", "V", "W", "X"),
        ("3M", "flies", "Y", "Z", "AA"),
    ]

    def __init__(
        self,
        workbook_name: str = "Live_Brazil_terminal.xlsm",
        sheet_index: int = 2,
        poll_interval_seconds: float = 5.0,
        max_rows: int = 250,
    ) -> None:
        self.workbook_name = workbook_name
        self.sheet_index = sheet_index
        self.poll_interval_seconds = poll_interval_seconds
        self.max_rows = max_rows
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._snapshot = self._build_base_snapshot()
        self._last_success_at: Optional[str] = None
        self._status = "initializing"
        self._error_message: Optional[str] = None
        self._previous_net_change: Dict[str, float] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._poll_loop(), name="live-monitor-poller")

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def get_snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return deepcopy(self._snapshot)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self._refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - runtime resilience
                await self._set_error_snapshot(f"Unexpected polling error: {exc}")
            await asyncio.sleep(self.poll_interval_seconds)

    async def _refresh_once(self) -> None:
        started = time.perf_counter()
        result = await asyncio.to_thread(self._read_market_snapshot)
        latency_ms = round((time.perf_counter() - started) * 1000, 2)

        if not result["connected"]:
            await self._set_error_snapshot(result.get("error_message", "Excel unavailable"))
            return

        now_iso = self._utc_now_iso()
        groups = result["groups"]
        self._apply_tick_state(groups)

        async with self._lock:
            self._last_success_at = now_iso
            self._status = "ok"
            self._error_message = None
            self._snapshot = {
                "as_of": now_iso,
                "connected": True,
                "latency_ms": latency_ms,
                "last_success_at": self._last_success_at,
                "status": self._status,
                "error_message": self._error_message,
                "groups": groups,
            }

    async def _set_error_snapshot(self, message: str) -> None:
        async with self._lock:
            self._status = "degraded"
            self._error_message = message
            self._snapshot = {
                "as_of": self._utc_now_iso(),
                "connected": False,
                "latency_ms": None,
                "last_success_at": self._last_success_at,
                "status": self._status,
                "error_message": self._error_message,
                "groups": self._empty_groups(),
            }

    def _read_market_snapshot(self) -> Dict[str, Any]:
        if xw is None:
            return {
                "connected": False,
                "error_message": "xlwings is not installed in this environment",
                "groups": self._empty_groups(),
            }

        try:
            workbook, sheet = self._resolve_workbook_sheet()
            groups = self._empty_groups()
            for tenure, category, name_col, live_col, settled_col in self.INSTRUMENT_COLUMNS:
                rows = self._read_rows(sheet, name_col, live_col, settled_col)
                groups[tenure][category] = rows
            _ = workbook  # keep reference explicit
            return {"connected": True, "groups": groups}
        except Exception as exc:
            return {
                "connected": False,
                "error_message": str(exc),
                "groups": self._empty_groups(),
            }

    def _resolve_workbook_sheet(self) -> Tuple[Any, Any]:
        app = xw.apps.active
        if app is None:
            raise RuntimeError("No active Excel application instance found")

        target_name = self.workbook_name.lower()
        workbook = None
        for book in app.books:
            if book.name.lower() == target_name:
                workbook = book
                break

        if workbook is None:
            local_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "Live_Brazil_Terminal.xlsm",
            )
            if os.path.exists(local_path):
                workbook = app.books.open(local_path, update_links=False, read_only=True)
            else:
                raise RuntimeError(f"Workbook '{self.workbook_name}' is not open")

        sheet = workbook.sheets[self.sheet_index - 1]
        return workbook, sheet

    def _read_rows(
        self,
        sheet: Any,
        name_col: str,
        live_col: str,
        settled_col: str,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        blank_streak = 0

        for idx in range(1, self.max_rows + 1):
            name_val = sheet.range(f"{name_col}{idx}").value
            live_val = sheet.range(f"{live_col}{idx}").value
            settled_val = sheet.range(f"{settled_col}{idx}").value

            if name_val in (None, "") and live_val in (None, "") and settled_val in (None, ""):
                blank_streak += 1
                if blank_streak >= 4:
                    break
                continue

            blank_streak = 0
            name = str(name_val).strip() if name_val not in (None, "") else f"{name_col}{idx}"
            live_price = self._to_float_or_none(live_val)
            last_settled = self._to_float_or_none(settled_val)
            net_change = (
                round(live_price - last_settled, 4)
                if live_price is not None and last_settled is not None
                else None
            )

            rows.append(
                {
                    "name": name,
                    "live_price": live_price,
                    "last_settled": last_settled,
                    "net_change": net_change,
                    "tick_delta": None,
                    "tick_event": False,
                    "tick_direction": None,
                }
            )
        return rows

    def _apply_tick_state(self, groups: Dict[str, Dict[str, List[Dict[str, Any]]]]) -> None:
        next_state: Dict[str, float] = {}
        for tenure, categories in groups.items():
            for category, rows in categories.items():
                for row in rows:
                    key = f"{tenure}:{category}:{row['name']}"
                    current = row["net_change"]
                    if current is None:
                        continue

                    previous = self._previous_net_change.get(key)
                    delta = round(current - previous, 4) if previous is not None else 0.0
                    tick_event = previous is not None and abs(delta) >= 0.5

                    row["tick_delta"] = delta
                    row["tick_event"] = tick_event
                    row["tick_direction"] = "up" if tick_event and delta > 0 else ("down" if tick_event and delta < 0 else None)
                    next_state[key] = current

        self._previous_net_change = next_state

    def _build_base_snapshot(self) -> Dict[str, Any]:
        return {
            "as_of": self._utc_now_iso(),
            "connected": False,
            "latency_ms": None,
            "last_success_at": None,
            "status": "initializing",
            "error_message": "Polling not started",
            "groups": self._empty_groups(),
        }

    @staticmethod
    def _empty_groups() -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        return {
            "3M": {"spreads": [], "flies": []},
            "6M": {"spreads": [], "flies": []},
            "12M": {"spreads": [], "flies": []},
            "24M": {"spreads": [], "flies": []},
        }

    @staticmethod
    def _to_float_or_none(value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()
