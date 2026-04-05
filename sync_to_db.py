#!/usr/bin/env python3
import os
import sys
import duckdb
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime, timezone

# ── CONFIG ───────────────────────────────────────────────────────
# Best practice: Load the URL from an environment variable
DB_URL = os.environ.get("COCKROACH_URL")
DATA_DIR = Path("./data")

if not DB_URL:
    print("❌ ERROR: COCKROACH_URL environment variable not set.")
    sys.exit(1)

def get_connection():
    return psycopg2.connect(DB_URL)

def sync_kalshi_trades(cur):
    print("\n[1] Syncing Kalshi Trades...")
    pattern = "kalshi/trades/*.parquet"
    files = list(DATA_DIR.glob(pattern))
    if not files:
        print("  ⚠️ No Kalshi trade data found. Skipping.")
        return

    # Use DuckDB to find the most recent 500
    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}') ORDER BY created_time DESC LIMIT 500").to_df()
    if df.empty: return

    # Corrected Kalshi trades schema
    # PK is trade_id
    cols = ["ticker", "count", "yes_price", "no_price", "created_time", "taker_side", "trade_id"]
    present_cols = [c for c in cols if c in df.columns]
    
    cur.execute(f"DROP TABLE IF EXISTS kalshi_trades")
    cur.execute(f"CREATE TABLE IF NOT EXISTS kalshi_trades ({', '.join([f'{c} STRING' for c in present_cols])}, PRIMARY KEY (trade_id))")
    
    df = df[present_cols].astype(object).where(df[present_cols].notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO kalshi_trades ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT (trade_id) DO UPDATE SET
            {", ".join([f"{c} = EXCLUDED.{c}" for c in present_cols if c != "trade_id"])}
    """
    execute_values(cur, query, data_tuples)
    print(f"  ✓ Synced {len(df):,} Kalshi trades.")

def sync_kalshi_markets(cur):
    print("\n[2] Syncing Kalshi Markets...")
    pattern = "kalshi/markets/*.parquet"
    files = list(DATA_DIR.glob(pattern))
    if not files:
        print("  ⚠️ No Kalshi market data found. Skipping.")
        return

    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}') ORDER BY open_time DESC LIMIT 500").to_df()
    if df.empty: return

    cols = ["ticker", "title", "yes_sub_title", "no_sub_title", "status", "result", "close_time", "open_time"]
    present_cols = [c for c in cols if c in df.columns]

    cur.execute(f"DROP TABLE IF EXISTS kalshi_markets")
    cur.execute(f"CREATE TABLE IF NOT EXISTS kalshi_markets ({', '.join([f'{c} STRING' for c in present_cols])}, PRIMARY KEY (ticker))")
    
    df = df[present_cols].astype(object).where(df[present_cols].notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO kalshi_markets ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT (ticker) DO UPDATE SET
            {", ".join([f"{c} = EXCLUDED.{c}" for c in present_cols if c != "ticker"])}
    """
    execute_values(cur, query, data_tuples)
    print(f"  ✓ Synced {len(df):,} Kalshi markets.")

def sync_polymarket_trades(cur):
    print("\n[3] Syncing Polymarket Trades...")
    pattern = "polymarket/trades/*.parquet"
    files = list(DATA_DIR.glob(pattern))
    if not files:
        print("  ⚠️ No Polymarket trade data found. Skipping.")
        return

    # Use transaction_hash + log_index as unique ID
    df = duckdb.sql(f"""
        SELECT 
            block_number, transaction_hash, log_index, maker, taker, maker_amount, taker_amount, timestamp,
            (transaction_hash || '-' || log_index) as trade_id
        FROM read_parquet('{DATA_DIR}/{pattern}') 
        ORDER BY timestamp DESC 
        LIMIT 500
    """).to_df()
    if df.empty: return

    cols = ["block_number", "transaction_hash", "log_index", "maker", "taker", "maker_amount", "taker_amount", "timestamp", "trade_id"]
    present_cols = [c for c in cols if c in df.columns]

    cur.execute(f"DROP TABLE IF EXISTS poly_trades")
    cur.execute(f"CREATE TABLE IF NOT EXISTS poly_trades ({', '.join([f'{c} STRING' for c in present_cols])}, PRIMARY KEY (trade_id))")
    
    df = df[present_cols].astype(object).where(df[present_cols].notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO poly_trades ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT (trade_id) DO UPDATE SET
            {", ".join([f"{c} = EXCLUDED.{c}" for c in present_cols if c != "trade_id"])}
    """
    execute_values(cur, query, data_tuples)
    print(f"  ✓ Synced {len(df):,} Polymarket trades.")

def sync_polymarket_markets(cur):
    print("\n[4] Syncing Polymarket Markets...")
    pattern = "polymarket/markets/*.parquet"
    files = list(DATA_DIR.glob(pattern))
    if not files:
        print("  ⚠️ No Polymarket market data found. Skipping.")
        return

    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}') ORDER BY closed DESC LIMIT 500").to_df()
    if df.empty: return

    cols = ["condition_id", "question", "outcomes", "volume", "closed", "end_date"]
    present_cols = [c for c in cols if c in df.columns]

    pk = "condition_id" if "condition_id" in present_cols else present_cols[0]
    cur.execute(f"DROP TABLE IF EXISTS poly_markets")
    cur.execute(f"CREATE TABLE IF NOT EXISTS poly_markets ({', '.join([f'{c} STRING' for c in present_cols])}, PRIMARY KEY ({pk}))")
    
    df = df[present_cols].astype(object).where(df[present_cols].notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO poly_markets ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT ({pk}) DO UPDATE SET
            {", ".join([f"{c} = EXCLUDED.{c}" for c in present_cols if c != pk])}
    """
    execute_values(cur, query, data_tuples)
    print(f"  ✓ Synced {len(df):,} Polymarket markets.")

def sync_polymarket_blocks(cur):
    print("\n[5] Syncing Polymarket Blocks...")
    pattern = "polymarket/blocks/*.parquet"
    files = list(DATA_DIR.glob(pattern))
    if not files:
        print("  ⚠️ No Polymarket block data found. Skipping.")
        return

    # Use DuckDB to find the most recent 500 blocks
    df = duckdb.sql(f"SELECT block_number, timestamp FROM read_parquet('{DATA_DIR}/{pattern}') ORDER BY block_number DESC LIMIT 500").to_df()
    if df.empty: return

    cols = ["block_number", "timestamp"]
    present_cols = [c for c in cols if c in df.columns]

    cur.execute(f"DROP TABLE IF EXISTS poly_blocks")
    cur.execute(f"CREATE TABLE IF NOT EXISTS poly_blocks ({', '.join([f'{c} STRING' for c in present_cols])}, PRIMARY KEY (block_number))")
    
    df = df[present_cols].astype(object).where(df[present_cols].notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO poly_blocks ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT (block_number) DO UPDATE SET
            {", ".join([f"{c} = EXCLUDED.{c}" for c in present_cols if c != "block_number"])}
    """
    execute_values(cur, query, data_tuples)
    print(f"  ✓ Synced {len(df):,} Polymarket blocks.")

if __name__ == "__main__":
    try:
        print(f"Connecting to CockroachDB...")
        with psycopg2.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                sync_kalshi_trades(cur)
                sync_kalshi_markets(cur)
                sync_polymarket_trades(cur)
                sync_polymarket_markets(cur)
                sync_polymarket_blocks(cur)
                conn.commit()
        print("\n🚀 Database Sync Complete!")
    except Exception as e:
        print(f"\n❌ ERROR during sync: {e}")
        sys.exit(1)


