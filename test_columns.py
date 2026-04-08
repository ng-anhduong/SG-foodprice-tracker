# test_columns.py

import sys
sys.path.append("pipeline/etl")

from load import get_client

client = get_client()

for table in [
    "canonical_product_daily_recommendations",
    "canonical_product_daily_prices",
    "commodity_price_comparisons"
]:
    res = client.table(table).select("*").limit(1).execute()
    print(f"\n── {table} ──")
    if res.data:
        print(list(res.data[0].keys()))
    else:
        print("empty")