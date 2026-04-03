#!/usr/bin/env python3
import os
import sys
import duckdb
import psycopg2
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

def sync_kalshi_trades():
    print("\n[1] Syncing Kalshi Trades...")
    pattern = "kalshi/trades/*.parquet"
    if not list(DATA_DIR.glob(pattern)):
        print("  ⚠️ No Kalshi trade data found.")
        return

    # 1. Load data via DuckDB
    # We take the last 30 days for a quick sync, or remove the filter for full backfill
    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}')").to_df()
    
    # 2. Connect to CockroachDB
    conn = get_connection()
    cur = conn.cursor()
    
    # 3. Create Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kalshi_trades (
            ticker STRING,
            count INT,
            yes_price INT,
            no_price INT,
            created_time TIMESTAMPTZ,
            side STRING,
            ID STRING PRIMARY KEY
        );
    """)
    
    # 4. Upsert Data
    # CockroachDB UPSERT syntax: INSERT INTO ... ON CONFLICT (ID) DO UPDATE SET ...
    columns = ["ticker", "count", "yes_price", "no_price", "created_time", "side", "ID"]
    data_tuples = [tuple(x) for x in df[columns].values]
    
    query = f"""
        INSERT INTO kalshi_trades ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (ID) DO UPDATE SET
            ticker = EXCLUDED.ticker,
            count = EXCLUDED.count,
            yes_price = EXCLUDED.yes_price,
            no_price = EXCLUDED.no_price,
            created_time = EXCLUDED.created_time,
            side = EXCLUDED.side;
    """
    
    execute_values(cur, query, data_tuples)
    conn.commit()
    print(f"  ✓ Synced {len(df):,} Kalshi trades.")
    cur.close()
    conn.close()

def sync_kalshi_markets():
    print("\n[2] Syncing Kalshi Markets...")
    pattern = "kalshi/markets/*.parquet"
    if not list(DATA_DIR.glob(pattern)):
        print("  ⚠️ No Kalshi market data found.")
        return

    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}')").to_df()
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kalshi_markets (
            ticker STRING PRIMARY KEY,
            title STRING,
            subtitle STRING,
            status STRING,
            result STRING,
            close_time TIMESTAMPTZ,
            open_time TIMESTAMPTZ
        );
    """)
    
    columns = ["ticker", "title", "subtitle", "status", "result", "close_time", "open_time"]
    # Ensure nested types/Nones are handled
    df = df[columns].where(df.notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO kalshi_markets ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (ticker) DO UPDATE SET
            title = EXCLUDED.title,
            subtitle = EXCLUDED.subtitle,
            status = EXCLUDED.status,
            result = EXCLUDED.result,
            close_time = EXCLUDED.close_time,
            open_time = EXCLUDED.open_time;
    """
    
    execute_values(cur, query, data_tuples)
    conn.commit()
    print(f"  ✓ Synced {len(df):,} Kalshi markets.")
    cur.close()
    conn.close()

def sync_polymarket_trades():
    print("\n[3] Syncing Polymarket Trades...")
    pattern = "polymarket/trades/*.parquet"
    if not list(DATA_DIR.glob(pattern)):
        print("  ⚠️ No Polymarket trade data found.")
        return

    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}')").to_df()
    
    conn = get_connection()
    cur = conn.cursor()
    
    # Polymarket trades use condition_id and a timestamp
    # We'll use a combination or a unique ID if it exists in the parquet
    cur.execute("""
        CREATE TABLE IF NOT EXISTS poly_trades (
            condition_id STRING,
            size FLOAT,
            price FLOAT,
            side STRING,
            timestamp TIMESTAMPTZ,
            transaction_hash STRING PRIMARY KEY
        );
    """)
    
    columns = ["condition_id", "size", "price", "side", "timestamp", "transaction_hash"]
    df = df[columns].where(df.notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO poly_trades ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (transaction_hash) DO UPDATE SET
            condition_id = EXCLUDED.condition_id,
            size = EXCLUDED.size,
            price = EXCLUDED.price,
            side = EXCLUDED.side,
            timestamp = EXCLUDED.timestamp;
    """
    
    execute_values(cur, query, data_tuples)
    conn.commit()
    print(f"  ✓ Synced {len(df):,} Polymarket trades.")
    cur.close()
    conn.close()

def sync_polymarket_markets():
    print("\n[4] Syncing Polymarket Markets...")
    pattern = "polymarket/markets/*.parquet"
    if not list(DATA_DIR.glob(pattern)):
        print("  ⚠️ No Polymarket market data found.")
        return

    df = duckdb.sql(f"SELECT * FROM read_parquet('{DATA_DIR}/{pattern}')").to_df()
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS poly_markets (
            condition_id STRING PRIMARY KEY,
            question STRING,
            outcome STRING,
            closed TIMESTAMPTZ
        );
    """)
    
    columns = ["condition_id", "question", "outcome", "closed"]
    # Filter columns and handle Nones
    present_cols = [c for c in columns if c in df.columns]
    df = df[present_cols].where(df.notnull(), None)
    data_tuples = [tuple(x) for x in df.values]
    
    query = f"""
        INSERT INTO poly_markets ({", ".join(present_cols)})
        VALUES %s
        ON CONFLICT (condition_id) DO UPDATE SET
            question = EXCLUDED.question,
            outcome = EXCLUDED.outcome,
            closed = EXCLUDED.closed;
    """
    
    execute_values(cur, query, data_tuples)
    conn.commit()
    print(f"  ✓ Synced {len(df):,} Polymarket markets.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    try:
        sync_kalshi_trades()
        sync_kalshi_markets()
        sync_polymarket_trades()
        sync_polymarket_markets()
        print("\n🚀 Database Sync Complete!")
    except Exception as e:
        print(f"\n❌ ERROR during sync: {e}")
        sys.exit(1)
