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

# ── CLI ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--data-dir", default="./data",    help="Path to extracted data/")
parser.add_argument("--out-dir",  default="./json_data", help="Output directory for JSON files")
args = parser.parse_args()

DATA = Path(args.data_dir)
OUT  = Path(args.out_dir)
OUT.mkdir(parents=True, exist_ok=True)

GENERATED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load(pattern: str, time_col: Optional[str] = None) -> pd.DataFrame:
    files = sorted(glob.glob(str(DATA / pattern)))
    if not files:
        print(f"  ⚠️  No files: {DATA / pattern}")
        return pd.DataFrame()
        
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    dfs = []
    
    for f in files:
        df = pd.read_parquet(f)
        if time_col and time_col in df.columns:
            ts = pd.to_datetime(df[time_col], errors="coerce", utc=True)
            df = df[ts >= cutoff]
            
        if not df.empty:
            dfs.append(df)
            
    if not dfs:
        return pd.DataFrame()
        
    df  = pd.concat(dfs, ignore_index=True)
    print(f"  Loaded {len(df):,} rows from {len(files)} file(s)  [{pattern}]")
    return df

def save(name: str, data: dict | list):
    payload = {"generated_at": GENERATED_AT, "data": data}
    path    = OUT / f"{name}.json"
    text    = json.dumps(payload, default=str, separators=(",", ":"))
    size_kb = len(text.encode()) / 1024
    path.write_text(text)
    print(f"  ✓ {name}.json  ({size_kb:.0f} KB,  {len(data) if isinstance(data, list) else '—'} rows)")
    if size_kb > 3800:
        print(f"    ⚠️  WARNING: file is {size_kb:.0f} KB — approaching Dune's 4096 KB limit!")

print("\n" + "═"*55)
print("  Building JSON datasets for Dune LiveFetch")
print("═"*55)


# ── 1. Kalshi: monthly volume ─────────────────────────────────────
print("\n[1] Kalshi monthly volume")
kt = load("kalshi/trades/*.parquet", time_col="created_time")
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
km = load("kalshi/markets/*.parquet", time_col="close_time")
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
pt = load("polymarket/trades/*.parquet", time_col="timestamp")
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
pm = load("polymarket/markets/*.parquet", time_col="closed")
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
    "base_url": "https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/json_data/"
}
(OUT / "index.json").write_text(json.dumps(meta, indent=2))
print(f"  ✓ index.json  ({len(index)} files listed)")

print("\n" + "═"*55)
print(f"  Done. {len(list(OUT.glob('*.json')))} JSON files in {OUT}/")
print("═"*55)
