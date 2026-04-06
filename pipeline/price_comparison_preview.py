import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


load_dotenv(".env")

DEFAULT_CATEGORY = "Beverages"
DEFAULT_OUTPUT = Path("data") / "price_comparison_preview"


def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def slugify(value: str) -> str:
    import re

    value = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return value or "unknown"


def save_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main(category: str = DEFAULT_CATEGORY):
    client = get_client()
    table_name = (
        "canonical_product_daily_recommendations"
        if table_exists(client, "canonical_product_daily_recommendations")
        else "canonical_product_daily_summary"
    )
    rows = (
        client.table(table_name)
        .select("*")
        .eq("unified_category", category)
        .gte("stores_seen_for_day", 2)
        .order("price_spread_sgd", desc=True)
        .limit(50)
        .execute()
        .data
    )

    out = DEFAULT_OUTPUT / f"{slugify(category)}_top50.json"
    save_json(out, rows or [])
    print(f"Saved {len(rows or [])} rows from {table_name} to {out}")


def table_exists(client, table_name: str) -> bool:
    try:
        client.table(table_name).select("*").limit(1).execute()
        return True
    except Exception:
        return False


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CATEGORY
    main(arg)
