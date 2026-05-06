"""Centralized configuration for the ARB analysis workspace.

Place overrides in an environment-specific module or set environment variables
if you need to modify paths in different environments.
"""
import os

# Database path (default: trading_data.db in workspace root)
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get('ARB_DB_PATH', os.path.join(ROOT_DIR, 'trading_data.db'))

# Live Excel workbook name (used by LiveMonitorService)
# Default matches the shipped file name; change via env var if needed.
LIVE_WORKBOOK_NAME = os.environ.get('ARB_LIVE_WORKBOOK', 'Live_Brazil_Terminal.xlsm')

# Excel sheet index used by LiveMonitorService (1-based)
LIVE_WORKBOOK_SHEET_INDEX = int(os.environ.get('ARB_LIVE_SHEET_INDEX', '2'))

# Poll interval (seconds) for LiveMonitorService
LIVE_POLL_INTERVAL_SECONDS = float(os.environ.get('ARB_LIVE_POLL_INTERVAL', '5.0'))
