import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, Optional

from dotenv import load_dotenv
from supabase import create_client


load_dotenv(".env")

PAGE_SIZE = 1000


def get_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(url, key)


def batched(rows: list[dict[str, Any]], batch_size: int = 200) -> Iterable[list[dict[str, Any]]]:
    for i in range(0, len(rows), batch_size):
        yield rows[i:i + batch_size]


def fetch_all_rows(supabase, table_name: str, columns: str, *, eq: Optional[tuple[str, Any]] = None):
    rows: list[dict[str, Any]] = []
    offset = 0

    while True:
        query = (
            supabase.table(table_name)
            .select(columns)
            .range(offset, offset + PAGE_SIZE - 1)
        )
        if eq is not None:
            query = query.eq(eq[0], eq[1])
        response = query.execute()
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    return rows


def parse_scraped_date_sg(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.date().isoformat()


def normalize_numeric(value):
    return None if value is None else float(value)


def parse_iso_datetime(value: Optional[str]) -> datetime:
    if not value:
        return datetime.min
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def table_exists(supabase, table_name: str) -> bool:
    try:
        supabase.table(table_name).select("*").limit(1).execute()
        return True
    except Exception:
        return False


def choose_preferred_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        rows,
        key=lambda row: (
            1 if row.get("price_sgd") is not None else 0,
            parse_iso_datetime(row.get("scraped_at")),
            row.get("product_id") or 0,
        ),
    )


def build_rows(category: Optional[str] = None):
    supabase = get_client()
    if not table_exists(supabase, "canonical_product_daily_prices") or not table_exists(
        supabase, "canonical_product_daily_recommendations"
    ):
        raise RuntimeError(
            "Derived price tables do not exist yet. Run pipeline/schemas/price_comparison_tables_schema.sql first."
        )

    canonical_products = fetch_all_rows(
        supabase,
        "canonical_products",
        "id,canonical_key,canonical_name,brand,unified_category,size_total_value,size_base_unit,size_display,pack_count,packaging,variant_tokens",
    )
    if category:
        canonical_products = [
            row for row in canonical_products if row["unified_category"] == category
        ]

    canonical_by_id = {row["id"]: row for row in canonical_products}
    if not canonical_by_id:
        return [], []

    members = fetch_all_rows(
        supabase,
        "canonical_product_members",
        "canonical_product_id,product_id",
    )
    members = [row for row in members if row["canonical_product_id"] in canonical_by_id]
    product_ids = {row["product_id"] for row in members}

    products = fetch_all_rows(
        supabase,
        "products",
        "id,name,brand,price_sgd,original_price_sgd,discount_sgd,unit,product_url,store,scraped_at",
    )
    product_by_id = {row["id"]: row for row in products if row["id"] in product_ids}

    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)

    for member in members:
        canonical = canonical_by_id.get(member["canonical_product_id"])
        product = product_by_id.get(member["product_id"])
        if canonical is None or product is None or not product.get("scraped_at"):
            continue

        scraped_date_sg = parse_scraped_date_sg(product["scraped_at"])
        detail = {
            "canonical_product_id": canonical["id"],
            "canonical_key": canonical["canonical_key"],
            "canonical_name": canonical["canonical_name"],
            "canonical_brand": canonical["brand"],
            "unified_category": canonical["unified_category"],
            "size_total_value": normalize_numeric(canonical.get("size_total_value")),
            "size_base_unit": canonical.get("size_base_unit"),
            "size_display": canonical.get("size_display"),
            "pack_count": canonical.get("pack_count"),
            "packaging": canonical.get("packaging"),
            "variant_tokens": canonical.get("variant_tokens") or [],
            "product_id": product["id"],
            "store": product.get("store"),
            "store_product_name": product.get("name"),
            "store_brand": product.get("brand"),
            "scraped_at": product["scraped_at"],
            "scraped_date_sg": scraped_date_sg,
            "price_sgd": normalize_numeric(product.get("price_sgd")),
            "original_price_sgd": normalize_numeric(product.get("original_price_sgd")),
            "discount_sgd": normalize_numeric(product.get("discount_sgd")),
            "unit": product.get("unit"),
            "product_url": product.get("product_url"),
        }
        grouped[(canonical["id"], scraped_date_sg)].append(detail)

    detail_rows: list[dict[str, Any]] = []
    recommendation_rows: list[dict[str, Any]] = []
    refreshed_at = datetime.now().isoformat()

    for (canonical_product_id, scraped_date_sg), rows in grouped.items():
        rows_by_store: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            store_key = row.get("store") or f"product:{row['product_id']}"
            rows_by_store[store_key].append(row)

        distinct_store_rows = [
            choose_preferred_row(store_rows) for store_rows in rows_by_store.values()
        ]
        priced_rows = [row for row in distinct_store_rows if row["price_sgd"] is not None]
        if not priced_rows:
            continue

        priced_rows.sort(key=lambda row: (row["price_sgd"], row["store"] or ""))
        cheapest = priced_rows[0]
        priciest = max(priced_rows, key=lambda row: (row["price_sgd"], row["store"] or ""))
        cheapest_price = cheapest["price_sgd"]
        priciest_price = priciest["price_sgd"]
        store_count = len({row["store"] for row in priced_rows if row.get("store")})

        for rank, row in enumerate(priced_rows, start=1):
            row["cheapest_price_for_day"] = cheapest_price
            row["highest_price_for_day"] = priciest_price
            row["price_rank_for_day"] = rank
            row["price_gap_from_cheapest"] = round(row["price_sgd"] - cheapest_price, 4)
            row["is_cheapest_for_day"] = row["price_sgd"] == cheapest_price
            row["matched_store_count_for_day"] = store_count
            row["refreshed_at"] = refreshed_at
            detail_rows.append(row)

        store_prices = {}
        for row in priced_rows:
            store_prices[row["store"]] = {
                "product_id": row["product_id"],
                "store_product_name": row["store_product_name"],
                "price_sgd": row["price_sgd"],
                "original_price_sgd": row["original_price_sgd"],
                "discount_sgd": row["discount_sgd"],
                "unit": row["unit"],
                "product_url": row["product_url"],
                "price_gap_from_cheapest": row["price_gap_from_cheapest"],
                "is_cheapest_for_day": row["is_cheapest_for_day"],
            }

        if store_count < 2:
            continue

        recommendation_rows.append(
            {
                "canonical_product_id": canonical_product_id,
                "canonical_key": cheapest["canonical_key"],
                "canonical_name": cheapest["canonical_name"],
                "canonical_brand": cheapest["canonical_brand"],
                "unified_category": cheapest["unified_category"],
                "size_total_value": cheapest["size_total_value"],
                "size_base_unit": cheapest["size_base_unit"],
                "size_display": cheapest["size_display"],
                "pack_count": cheapest["pack_count"],
                "packaging": cheapest["packaging"],
                "variant_tokens": cheapest["variant_tokens"],
                "scraped_date_sg": scraped_date_sg,
                "stores_seen_for_day": store_count,
                "cheapest_store": cheapest["store"],
                "cheapest_price_sgd": cheapest_price,
                "priciest_store": priciest["store"],
                "priciest_price_sgd": priciest_price,
                "price_spread_sgd": round(priciest_price - cheapest_price, 4),
                "store_prices": store_prices,
                "refreshed_at": refreshed_at,
            }
        )

    return detail_rows, recommendation_rows


def clear_table_slice(supabase, table_name: str, category: Optional[str]) -> None:
    query = supabase.table(table_name).delete()
    if category:
        query = query.eq("unified_category", category)
    else:
        query = query.gte("id", 0)
    query.execute()


def sync_rows(
    detail_rows: list[dict[str, Any]],
    recommendation_rows: list[dict[str, Any]],
    category: Optional[str] = None,
):
    supabase = get_client()

    # These are cache tables derived entirely from matching outputs.
    # Clear the target slice first so reruns remove stale single-store rows.
    clear_table_slice(supabase, "canonical_product_daily_prices", category)
    clear_table_slice(supabase, "canonical_product_daily_recommendations", category)

    for batch in batched(detail_rows):
        supabase.table("canonical_product_daily_prices").upsert(
            batch,
            on_conflict="product_id",
        ).execute()

    for batch in batched(recommendation_rows):
        supabase.table("canonical_product_daily_recommendations").upsert(
            batch,
            on_conflict="canonical_product_id,scraped_date_sg",
        ).execute()


def main(category: Optional[str] = None):
    detail_rows, recommendation_rows = build_rows(category)
    sync_rows(detail_rows, recommendation_rows, category)
    print(
        json.dumps(
            {
                "category": category or "ALL",
                "daily_price_rows": len(detail_rows),
                "daily_recommendation_rows": len(recommendation_rows),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
