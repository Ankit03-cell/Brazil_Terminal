---
title: ARB Terminal
emoji: 🛡️
colorFrom: green
colorTo: blue
sdk: docker
pinned: false
license: mit
---

# ARB Financial Terminal

Professional-grade financial terminal for Brazil DI spread and butterfly analysis.

## Features
- Real-time spread and fly curve visualization.
- Historical trend analysis with Z-scores and Percentiles.
- Automated data ingestion from Excel.

## Local Development
```bash
pip install -r requirements.txt
python ingestion.py
uvicorn backend.api:app --reload
```
