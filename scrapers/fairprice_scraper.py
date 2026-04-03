# scrapers/fairprice_scraper.py

import json
import os
import time
from datetime import datetime

import requests


BASE_URL = "https://website-api.omni.fairprice.com.sg/api/product/v2"

FIXED_PARAMS = {
    "algopers": "prm-ppb-1,prm-ep-1,t-epds-1,t-ppb-0,t-ep-0",
    "experiments": "ls_deltime-sortA,searchVariant-B,gv-A,shelflife-B,ds-A,ls_comsl-B,cartfiller-a,catnav-hide,catbubog-B,sbanner-A,count-b,cam-a,promobanner-c,algopers-b,dlv_pref_mf-B,delivery_pref_ffs-C,delivery_pref_pfc-C,crtalc-B,crt-v-wbble-A,zero_search_swimlane-A,sd-var-a,mos-on,gsc-a,camp-lbl-B,poa-entry-B",
    "includeTagDetails": "true",
    "orderType": "DELIVERY",
    "pageType": "category",
    "sorting": "POPULARITY",
    "storeId": 165,
}

CATEGORIES = [
    "drinks",
    "bakery",
    "dairy-chilled-eggs",
    "rice-noodles-cooking-ingredients",
    "fruits-vegetables",
    "meat-seafood",
    "frozen",
]

CATEGORY_FILTER = [
    slug.strip()
    for slug in os.getenv("FAIRPRICE_CATEGORY", "").split(",")
    if slug.strip()
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fairprice.com.sg/",
    "Origin": "https://www.fairprice.com.sg",
}


def extract_product_fields(item: dict, category_slug: str) -> dict:
    final_price = item.get("final_price")

    store_data = item.get("storeSpecificData", [])
    if store_data and isinstance(store_data, list):
        original_price = store_data[0].get("mrp")
        discount = store_data[0].get("discount")
    else:
        original_price = None
        discount = None

    brand = item.get("brand", {})
    brand_name = brand.get("name") if isinstance(brand, dict) else None

    primary_cat = item.get("primaryCategory", {})
    subcategory = primary_cat.get("name") if primary_cat else None
    parent_cat = primary_cat.get("parentCategory", {}) if primary_cat else {}
    main_category = parent_cat.get("name") if parent_cat else None

    meta = item.get("metaData", {})
    unit = meta.get("DisplayUnit") or meta.get("Unit Of Weight")

    slug = item.get("slug")
    product_url = f"https://www.fairprice.com.sg/product/{slug}" if slug else None

    return {
        "name": item.get("name"),
        "brand": brand_name,
        "price_sgd": final_price,
        "original_price_sgd": original_price,
        "discount_sgd": discount,
        "unit": unit,
        "main_category": main_category,
        "subcategory": subcategory,
        "category_slug": category_slug,
        "product_url": product_url,
        "store": "fairprice",
        "scraped_at": datetime.now().isoformat(),
    }


def scrape_category(category_slug: str) -> list[dict]:
    products = []
    page = 1

    print(f"\n  Scraping: {category_slug}")

    while True:
        params = {
            **FIXED_PARAMS,
            "category": category_slug,
            "slug": category_slug,
            "url": category_slug,
            "page": page,
            "size": 48,
        }

        try:
            response = requests.get(
                BASE_URL,
                headers=HEADERS,
                params=params,
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            print(f"    Request error on page {page}: {e}")
            break

        if response.status_code != 200:
            print(f"    HTTP {response.status_code} on page {page} - stopping")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print(f"    Could not parse JSON on page {page}")
            break

        items = data.get("data", {}).get("product", [])

        if not items:
            print(f"    No more products at page {page} - done")
            break

        for item in items:
            products.append(extract_product_fields(item, category_slug))

        print(f"    Page {page}: {len(items)} products (total: {len(products)})")

        page += 1
        time.sleep(1)

    return products


def save_raw(products: list[dict], category_slug: str):
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join("data", "raw", "fairprice", date_str)
    os.makedirs(folder, exist_ok=True)

    filepath = os.path.join(folder, f"{category_slug}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    print(f"    Saved -> {filepath}")


def run():
    print("=" * 60)
    print(f"FairPrice Scraper started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total = 0

    categories = (
        CATEGORIES
        if not CATEGORY_FILTER
        else [category for category in CATEGORIES if category in CATEGORY_FILTER]
    )

    for category in categories:
        products = scrape_category(category)

        if products:
            save_raw(products, category)
            total += len(products)
        else:
            print("    No products found - skipping save")

        time.sleep(2)

    print("\n" + "=" * 60)
    print(f"Done. Total products scraped: {total}")
    print("=" * 60)


if __name__ == "__main__":
    run()
