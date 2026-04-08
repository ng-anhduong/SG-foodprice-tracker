# pipeline/etl/transform.py
#
# Cleans raw scraped JSON files from all stores and writes
# processed output to data/processed/<store>/<date>/
#
# Cleaning rules:
#   - Unified schema across all stores
#   - FairPrice: cast price fields from str -> float, null out zero discounts
#   - Sheng Siong: prepend base URL to relative product_url
#   - RedMart: strip query string from product_url
#   - All stores: map category_slug -> unified_category, drop excluded categories
#   - All stores: drop store-specific fields not in unified schema

import json
import os
from datetime import datetime
from urllib.parse import urlparse, urlunparse


RAW_BASE  = os.path.join("data", "raw")
PROC_BASE = os.path.join("data", "processed")

STORES = ["fairprice", "shengsiong", "redmart", "coldstorage"]

SHENGSIONG_BASE_URL = "https://shengsiong.com.sg"

# Maps each store's category_slug to a unified category name.
# Slugs not in this map are excluded from the processed output.
UNIFIED_CATEGORY_MAP = {
    # FairPrice
    "dairy-chilled-eggs":               "Dairy",
    "fruits-vegetables":                "Fruits & Vegetables",
    "snacks":                           "Snacks & Confectionery",
    "sweets-1":                         "Snacks & Confectionery",
    "chocolates-1":                     "Snacks & Confectionery",
    "rice-noodles-cooking-ingredients": "Staples",
    "food-cupboard-6":                  "Staples",
    "dried-fruits--nuts":               "Staples",
    "meat-seafood":                     "Meat & Seafood",
    "drinks":                           "Beverages",
    "bakery":                           "Bakery & Breakfast",

    # Sheng Siong
    "dairy-chilled-eggs":               "Dairy",
    "fruits":                           "Fruits & Vegetables",
    "vegetables":                       "Fruits & Vegetables",
    "snacks-confectioneries":           "Snacks & Confectionery",
    "rice-noodles-pasta":               "Staples",
    "meat-poultry-seafood":             "Meat & Seafood",
    "beverages":                        "Beverages",
    "breakfast-spreads":                "Bakery & Breakfast",
    "cooking-baking":                   "Bakery & Breakfast",

    # RedMart
    "dairy-chilled-eggs":               "Dairy",
    "fruits":                           "Fruits & Vegetables",
    "vegetables":                       "Fruits & Vegetables",
    "snack-and-confectionery":          "Snacks & Confectionery",
    "rice-noodles-cooking-ingredients": "Staples",
    "meat":                             "Meat & Seafood",
    "seafood":                          "Meat & Seafood",
    "drinks":                           "Beverages",
    "bakery-breakfast":                 "Bakery & Breakfast",

    # Cold Storage
    "dairy-chilled-eggs":               "Dairy",
    "fruits-vegetables":                "Fruits & Vegetables",
    "snacks-confectionery":             "Snacks & Confectionery",
    "rice-oil-noodles":                 "Staples",
    "meat-seafood":                     "Meat & Seafood",
    "beverages":                        "Beverages",
    "breakfast-bakery":                 "Bakery & Breakfast",
}


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

def build_unified(product: dict, store: str) -> dict | None:
    """Returns None if the category is not in the unified map (i.e. excluded)."""
    slug = product.get("category_slug")
    unified_category = UNIFIED_CATEGORY_MAP.get(slug)
    if unified_category is None:
        return None

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
        "unified_category":    unified_category,
        "category_slug":       slug,
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
        cleaned = [p for p in cleaned if p is not None]

        if not cleaned:
            print(f"  [{store}] {fname:<50} skipped (excluded category)")
            continue

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
