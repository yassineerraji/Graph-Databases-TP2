#!/usr/bin/env bash
set -euo pipefail

# Optional: run ETL before starting the API when RUN_ETL is set
if [ "${RUN_ETL:-0}" = "1" ] || [ "${RUN_ETL:-false}" = "true" ]; then
  echo "▶ Running ETL before starting API..."
  python etl.py
fi

exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload

