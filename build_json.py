#!/usr/bin/env python3
"""
build_json.py
─────────────────────────────────────────────────────────────────
Reads the raw Parquet files from the prediction-market-analysis
dataset and writes small, pre-aggregated JSON files to json_data/.

Each JSON file is kept under 4 MB so Dune's http_get() can fetch
it within the 5-second timeout and 4 MiB response limit.

GitHub Actions runs this daily after downloading the fresh Parquet
data, then commits the updated JSON files back to the repo so Dune
always reads the latest version via raw.githubusercontent.com.

Run locally:
    python build_json.py --data-dir ./data --out-dir ./json_data
"""

import argparse
import json
import glob
import sys
from typing import Optional
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import duckdb

# ── CLI ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", default="./data",    help="Path to extracted data/")
parser.add_argument("--out-dir",  default="./json_data", help="Output directory for JSON files")
args = parser.parse_args()

DATA = Path(args.data_dir)
OUT  = Path(args.out_dir)
OUT.mkdir(parents=True, exist_ok=True)

GENERATED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_cutoff(pattern: str, time_col: str, days: int = 180) -> Optional[datetime]:
    """Finds the latest date in the parquet files and returns a cutoff date."""
    files = list(DATA.glob(pattern))
    if not files:
        return None
    try:
        # Use DuckDB to quickly find the max timestamp across all files
        sql = f"SELECT MAX({time_col}) FROM read_parquet('{DATA}/{pattern}')"
        res = duckdb.sql(sql).fetchone()
        if res and res[0]:
            # DuckDB timestamps might be pydatetime or numpy.datetime64
            latest = pd.to_datetime(res[0], utc=True)
            return latest - timedelta(days=days)
    except Exception as e:
        print(f"  ⚠️ Error calculating cutoff for {pattern}: {e}")
    return None

def load_df(pattern: str, time_col: Optional[str] = None, cutoff: Optional[datetime] = None) -> pd.DataFrame:
    """Uses DuckDB to load parquet files into a Pandas DataFrame with optional filtering."""
    path = DATA / pattern
    if not list(DATA.glob(pattern)):
        return pd.DataFrame()
    
    sql = f"SELECT * FROM read_parquet('{path}')"
    if time_col and cutoff:
        # Format cutoff for SQL
        ts_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')
        sql += f" WHERE {time_col} >= '{ts_str}'"
    
    return duckdb.sql(sql).to_df()

def save(name: str, data: list):
    """Saves data as a pre-aggregated JSON file for Dune."""
    path = OUT / f"{name}.json"
    payload = {
        "generated_at": GENERATED_AT,
        "data": data
    }
    # Use default=str to handle any nested datetime/numpy types
    path.write_text(json.dumps(payload, indent=2, default=str))
    print(f"  ✓ {name}.json ({len(data):,} records)")

print("\n" + "═"*55)
print("  Building JSON datasets for Dune LiveFetch")
print("═"*55)


# ── 1. Kalshi: monthly volume ─────────────────────────────────────
print("\n[1] Kalshi monthly volume")
cutoff_kt = get_cutoff("kalshi/trades/*.parquet", "created_time")
kt = load_df("kalshi/trades/*.parquet", "created_time", cutoff_kt)
if not kt.empty:
    kt["month"] = pd.to_datetime(kt["created_time"], errors="coerce", utc=True).dt.strftime("%Y-%m")
    out = (
        kt.dropna(subset=["month"])
          .groupby("month")
          .agg(
              total_contracts=("count", "sum"),
              num_trades=("count", "count"),
              active_markets=("ticker", "nunique"),
          )
          .reset_index()
          .sort_values("month")
    )
    save("kalshi_monthly_volume", out.to_dict(orient="records"))


# ── 2. Kalshi: calibration (price bucket → actual resolution rate) ─
print("\n[2] Kalshi calibration")
cutoff_km = get_cutoff("kalshi/markets/*.parquet", "close_time")
km = load_df("kalshi/markets/*.parquet", "close_time", cutoff_km)
if not kt.empty and not km.empty:
    resolved = km[km["status"] == "finalized"][["ticker", "result"]].dropna()
    resolved = resolved[resolved["result"].isin(["yes", "no"])]

    merged = kt.merge(resolved, on="ticker", how="inner")
    merged["price_bucket"] = (merged["yes_price"] / 5).round() * 5
    merged["resolved_yes"] = (merged["result"] == "yes").astype(int)

    cal = (
        merged[merged["yes_price"].between(5, 95)]
        .groupby("price_bucket")
        .agg(
            total_contracts=("count", "sum"),
            pct_resolved_yes=("resolved_yes", "mean"),
            sample_markets=("ticker", "nunique"),
        )
        .reset_index()
    )
    cal["pct_resolved_yes"] = (cal["pct_resolved_yes"] * 100).round(2)
    cal = cal[cal["total_contracts"] > 500]
    save("kalshi_calibration", cal.to_dict(orient="records"))


# ── 3. Kalshi: top 100 markets by volume ─────────────────────────
print("\n[3] Kalshi top markets")
if not kt.empty and not km.empty:
    agg = (
        kt.groupby("ticker")
          .agg(total_contracts=("count", "sum"), num_trades=("count", "count"),
               avg_price=("yes_price", "mean"), min_price=("yes_price", "min"),
               max_price=("yes_price", "max"))
          .reset_index()
    )
    top = (
        agg.merge(km[["ticker","title","status","result","close_time"]], on="ticker", how="left")
           .sort_values("total_contracts", ascending=False)
           .head(100)
    )
    top["avg_price"] = top["avg_price"].round(2)
    save("kalshi_top_markets", top.to_dict(orient="records"))


# ── 4. Kalshi: volume by price bucket (longshot bias) ────────────
print("\n[4] Kalshi longshot bias")
if not kt.empty:
    vol = (
        kt[kt["yes_price"].between(1, 99)]
          .groupby("yes_price")
          .agg(total_contracts=("count", "sum"))
          .reset_index()
          .sort_values("yes_price")
    )
    total = vol["total_contracts"].sum()
    vol["pct_of_volume"] = (vol["total_contracts"] / total * 100).round(4)
    save("kalshi_price_distribution", vol.to_dict(orient="records"))


# ── 5. Kalshi: market resolution stats ───────────────────────────
print("\n[5] Kalshi resolution stats")
if not km.empty:
    finalized = km[km["status"] == "finalized"].copy()
    finalized["close_month"] = pd.to_datetime(
        finalized["close_time"], errors="coerce", utc=True
    ).dt.strftime("%Y-%m")

    stats = (
        finalized.dropna(subset=["close_month", "result"])
        .groupby(["close_month", "result"])
        .agg(count=("ticker", "count"))
        .reset_index()
        .sort_values("close_month")
    )
    save("kalshi_resolution_by_month", stats.to_dict(orient="records"))


# ── 6. Polymarket: monthly volume ────────────────────────────────
print("\n[6] Polymarket monthly volume")
cutoff_pt = get_cutoff("polymarket/trades/*.parquet", "timestamp")
pt = load_df("polymarket/trades/*.parquet", "timestamp", cutoff_pt)

if not pt.empty:
    pt["month"] = pd.to_datetime(pt["timestamp"], errors="coerce", utc=True).dt.strftime("%Y-%m")
    out = (
        pt.dropna(subset=["month"])
          .groupby("month")
          .agg(
              total_usdc=("size", "sum"),
              num_trades=("size", "count"),
              active_markets=("condition_id", "nunique"),
          )
          .reset_index()
          .sort_values("month")
    )
    out["total_usdc"] = out["total_usdc"].round(2)
    save("poly_monthly_volume", out.to_dict(orient="records"))


# ── 7. Polymarket: top 100 markets by USDC volume ────────────────
print("\n[7] Polymarket top markets")
cutoff_pm = get_cutoff("polymarket/markets/*.parquet", "closed")
pm = load_df("polymarket/markets/*.parquet", "closed", cutoff_pm)

if not pt.empty:
    agg = (
        pt.groupby("condition_id")
          .agg(total_usdc=("size", "sum"), num_trades=("size", "count"),
               avg_price=("price", "mean"))
          .reset_index()
    )
    if not pm.empty and "condition_id" in pm.columns:
        cols = [c for c in ["condition_id","question","outcome","closed"] if c in pm.columns]
        agg  = agg.merge(pm[cols], on="condition_id", how="left")

    top = agg.sort_values("total_usdc", ascending=False).head(100)
    top["total_usdc"] = top["total_usdc"].round(2)
    top["avg_price"]  = top["avg_price"].round(4)
    save("poly_top_markets", top.to_dict(orient="records"))


# ── 8. Polymarket: price distribution ────────────────────────────
print("\n[8] Polymarket price distribution")
if not pt.empty:
    pt["price_bucket"] = (pt["price"] * 20).round() / 20   # 0.05 buckets
    dist = (
        pt.groupby("price_bucket")
          .agg(total_usdc=("size", "sum"), num_trades=("size", "count"))
          .reset_index()
          .sort_values("price_bucket")
    )
    dist["total_usdc"] = dist["total_usdc"].round(2)
    save("poly_price_distribution", dist.to_dict(orient="records"))


# ── 10. Kalshi: Yesterday Snapshot ──────────────────────────────
print("\n[10] Kalshi yesterday snapshot")
if not kt.empty:
    # Find the latest date in the dataset
    latest_dt = pd.to_datetime(kt["created_time"], errors="coerce", utc=True).max()
    if latest_dt:
        yesterday_date = (latest_dt - timedelta(days=1)).date()
        print(f"  Filtering for date: {yesterday_date}")
        
        y_trades = kt[pd.to_datetime(kt["created_time"], errors="coerce", utc=True).dt.date == yesterday_date]
        
        if not y_trades.empty:
            y_summary = (
                y_trades.groupby("ticker")
                .agg(
                    contracts=("count", "sum"),
                    trades=("count", "count"),
                    avg_price=("yes_price", "mean")
                )
                .reset_index()
                .sort_values("contracts", ascending=False)
            )
            
            # Enrich with market titles if available
            if not km.empty:
                y_summary = y_summary.merge(km[["ticker", "title"]], on="ticker", how="left")
            
            save("kalshi_yesterday", y_summary.to_dict(orient="records"))
        else:
            print(f"  ⚠️ No trades found for {yesterday_date}")


# ── 11. Kalshi: Recent 500 Trades (No Aggregation) ──────────────
print("\n[11] Kalshi recent 500 trades")
if not kt.empty:
    recent = (
        kt.sort_values("created_time", ascending=False)
          .head(500)
          .copy()
    )
    # Convert to standard Python types for JSON saving
    save("kalshi_recent_trades", recent.to_dict(orient="records"))


# ── 9. Meta: index of all available files ────────────────────────
print("\n[9] Meta index")
files = sorted(OUT.glob("*.json"))
index = []
for f in files:
    if f.name == "index.json":
        continue
    size_kb = f.stat().st_size / 1024
    index.append({"file": f.name, "size_kb": round(size_kb, 1)})

meta = {
    "generated_at": GENERATED_AT,
    "files": index,
    "base_url": "https://raw.githubusercontent.com/timli789/Prediction-json-data-/main/json_data/"
}
(OUT / "index.json").write_text(json.dumps(meta, indent=2))
print(f"  ✓ index.json  ({len(index)} files listed)")

print("\n" + "═"*55)
print(f"  Done. {len(list(OUT.glob('*.json')))} JSON files in {OUT}/")
print("═"*55)
