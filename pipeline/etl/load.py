# pipeline/etl/load.py
#
# Loads processed JSON files into Supabase PostgreSQL via the REST API.
# Reads from data/processed/<store>/<date>/ and inserts into the products table.
#
# Usage:
#   python3 pipeline/etl/load.py 2026-04-03   # specific date
#   python3 pipeline/etl/load.py              # defaults to today

import json
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

PROC_BASE = os.path.join("data", "processed")
STORES = ["fairprice", "shengsiong", "redmart", "coldstorage"]

BATCH_SIZE = 500  # Supabase REST API handles ~500 rows per request well


def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def load_date(date_str: str):
    supabase = get_client()

    print("=" * 60)
    print(f"Loading  {date_str}")
    print("=" * 60)

    total = 0

    for store in STORES:
        folder = os.path.join(PROC_BASE, store, date_str)
        if not os.path.exists(folder):
            print(f"  [{store}] No processed data for {date_str} — skipping")
            continue

        store_total = 0
        for fname in sorted(os.listdir(folder)):
            if not fname.endswith(".json"):
                continue

            with open(os.path.join(folder, fname), encoding="utf-8") as f:
                products = json.load(f)

            if not products:
                continue

            # Insert in batches
            for i in range(0, len(products), BATCH_SIZE):
                batch = products[i:i + BATCH_SIZE]
                supabase.table("products").upsert(batch, ignore_duplicates=True, on_conflict="name,store,scraped_at").execute()

            store_total += len(products)
            print(f"  [{store}] {fname:<50} {len(products):>5} rows")

        total += store_total
        print(f"  [{store}] Total inserted: {store_total}")

    print(f"\nDone. Total rows inserted: {total}")


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    load_date(date_arg)
