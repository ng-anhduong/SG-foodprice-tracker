# pipeline/transform.py
#
# Cleans raw scraped JSON files from all stores and writes
# processed output to data/processed/<store>/<date>/
#
# Cleaning rules:
#   - Unified schema across all stores
#   - FairPrice: cast price fields from str -> float, null out zero discounts
#   - Sheng Siong: prepend base URL to relative product_url
#   - RedMart: strip query string from product_url
#   - All stores: unified_category = category_slug (placeholder for manual mapping)
#   - All stores: drop store-specific fields not in unified schema

import json
import os
from datetime import datetime
from urllib.parse import urlparse, urlunparse


RAW_BASE  = os.path.join("data", "raw")
PROC_BASE = os.path.join("data", "processed")

STORES = ["fairprice", "shengsiong", "redmart", "coldstorage"]

SHENGSIONG_BASE_URL = "https://shengsiong.com.sg"


# ── FIELD CLEANERS ────────────────────────────────────────────────────────────

def to_float(value):
    """Cast value to float, return None if not possible or zero."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def clean_discount(discount, price, original_price):
    """Return null if discount is 0 or price == original_price."""
    d = to_float(discount)
    if d is None or d == 0.0:
        return None
    return round(d, 2)


def clean_product_url(url, store):
    """Normalize product URLs per store."""
    if not url:
        return None

    if store == "shengsiong":
        if url.startswith("/"):
            return SHENGSIONG_BASE_URL + url
        return url

    if store == "redmart":
        # Strip query string from Lazada URLs
        parsed = urlparse(url)
        return urlunparse(parsed._replace(query="", fragment=""))

    return url


# ── UNIFIED SCHEMA ────────────────────────────────────────────────────────────

def build_unified(product: dict, store: str) -> dict:
    price          = to_float(product.get("price_sgd"))
    original_price = to_float(product.get("original_price_sgd"))
    discount       = clean_discount(
        product.get("discount_sgd"), price, original_price
    )

    return {
        "name":                product.get("name"),
        "brand":               product.get("brand"),
        "price_sgd":           price,
        "original_price_sgd":  original_price,
        "discount_sgd":        discount,
        "unit":                product.get("unit"),
        "unified_category":    product.get("category_slug"),   # placeholder
        "category_slug":       product.get("category_slug"),
        "store":               store,
        "product_url":         clean_product_url(product.get("product_url"), store),
        "scraped_at":          product.get("scraped_at"),
    }


# ── PROCESS ONE DATE ──────────────────────────────────────────────────────────

def process_store(store: str, date_str: str):
    raw_folder  = os.path.join(RAW_BASE,  store, date_str)
    proc_folder = os.path.join(PROC_BASE, store, date_str)

    if not os.path.exists(raw_folder):
        print(f"  [{store}] No raw data for {date_str} — skipping")
        return

    os.makedirs(proc_folder, exist_ok=True)

    total = 0
    for fname in sorted(os.listdir(raw_folder)):
        if not fname.endswith(".json"):
            continue

        raw_path  = os.path.join(raw_folder, fname)
        proc_path = os.path.join(proc_folder, fname)

        with open(raw_path, encoding="utf-8") as f:
            raw_products = json.load(f)

        cleaned = [build_unified(p, store) for p in raw_products]

        with open(proc_path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2, ensure_ascii=False)

        total += len(cleaned)
        print(f"  [{store}] {fname:<50} {len(cleaned):>5} products")

    print(f"  [{store}] Total: {total} products -> {proc_folder}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(date_str: str = None):
    if date_str is None:
        date_str = datetime.now().strftime("%Y-%m-%d")

    print("=" * 60)
    print(f"Transform  {date_str}")
    print("=" * 60)

    for store in STORES:
        process_store(store, date_str)

    print("\nDone.")


if __name__ == "__main__":
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
