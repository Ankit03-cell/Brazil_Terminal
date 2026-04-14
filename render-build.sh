#!/usr/bin/env bash
# exit on error
set -o errexit

# Install dependencies
pip install -r requirements.txt

# Run ingestion to build SQLite database from Excel
python ingestion.py
