# stream-analytics-platform

## Overview
Educational **mini data platform** built on the [MovieLens dataset](https://grouplens.org/datasets/movielens/).  
Key features:
- **ELT flow**: CSV → pandas → SQLite/DuckDB
- **Silver layer**: normalized dimension & fact tables
- **Gold layer**: aggregated marts for analytics
- **Pluggable engines**: run on `sqlite` (default) or `duckdb`

## Quickstart (local)
```bash
  # create and activate virtual environment
  python -m venv .venv
  source .venv/Scripts/activate    # on Windows Git Bash
  # .venv\Scripts\activate         # on Windows PowerShell

  # install dependencies
  pip install -r requirements.txt

  # copy env template and adjust if needed
  cp .env.example .env

  # run full ETL pipeline into SQLite
  python -m etl.run --engine sqlite
```
