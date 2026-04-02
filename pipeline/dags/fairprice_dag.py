# dags/fairprice_dag.py

import json
import os
import time
import requests
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# ── CONFIG ────────────────────────────────────────────────────────────────────

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
    "snacks-and-confectionery",
    "condiments-and-sauces",
    "breakfast-and-spreads",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fairprice.com.sg/",
    "Origin": "https://www.fairprice.com.sg",
}

# Update this to your actual project path
RAW_DATA_PATH = os.path.expanduser("~/SG-foodprice-tracker/fairprice-pipeline/data/raw/fairprice")


# ── DAG DEFINITION ────────────────────────────────────────────────────────────

@dag(
    dag_id="fairprice_scraper",
    description="Scrapes FairPrice product prices daily and saves raw JSON",
    schedule="0 9 * * *",   # runs every day at 9am
    start_date=datetime(2026, 4, 1),
    catchup=False,           # don't backfill missed runs
    tags=["fairprice", "scraping"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
)
def fairprice_pipeline():

    # ── TASK 1: Scrape all categories ─────────────────────────────────────────

    @task()
    def scrape_all_categories() -> dict:
        """
        Scrapes all categories from FairPrice and returns
        a summary of how many products were found per category.
        """

        def extract_fields(item: dict, category_slug: str) -> dict:
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
                "store": "fairprice",
                "scraped_at": datetime.now().isoformat(),
            }

        def scrape_category(category_slug: str) -> list[dict]:
            products = []
            page = 1

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
                    response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=15)
                except requests.exceptions.RequestException as e:
                    print(f"Request error on {category_slug} page {page}: {e}")
                    break

                if response.status_code != 200:
                    print(f"HTTP {response.status_code} on {category_slug} page {page}")
                    break

                try:
                    data = response.json()
                except json.JSONDecodeError:
                    print(f"JSON error on {category_slug} page {page}")
                    break

                items = data.get("data", {}).get("product", [])

                if not items:
                    break

                for item in items:
                    products.append(extract_fields(item, category_slug))

                print(f"  {category_slug} | page {page} | {len(products)} total")
                page += 1
                time.sleep(1)

            return products

        # Scrape all categories and save each one
        date_str = datetime.now().strftime("%Y-%m-%d")
        folder = os.path.join(RAW_DATA_PATH, date_str)
        os.makedirs(folder, exist_ok=True)

        summary = {}

        for category in CATEGORIES:
            print(f"\nScraping: {category}")
            products = scrape_category(category)

            if products:
                filepath = os.path.join(folder, f"{category}.json")
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(products, f, indent=2, ensure_ascii=False)
                print(f"Saved {len(products)} products → {filepath}")
                summary[category] = len(products)
            else:
                print(f"No products for {category} — skipping")
                summary[category] = 0

            time.sleep(2)

        return summary

    # ── TASK 2: Validate the output ───────────────────────────────────────────

    @task()
    def validate_output(summary: dict):
        """
        Checks that each category produced at least some products.
        Logs a warning for any category with 0 results.
        """
        print("\n── Scrape Summary ──")
        total = 0
        failed = []

        for category, count in summary.items():
            status = "✓" if count > 0 else "✗ EMPTY"
            print(f"  {status}  {category}: {count} products")
            total += count
            if count == 0:
                failed.append(category)

        print(f"\nTotal products scraped: {total}")

        if failed:
            print(f"\nWarning — these categories returned 0 products: {failed}")
            print("Check if the category slugs are still correct.")
        else:
            print("\nAll categories scraped successfully.")

    # ── WIRE UP TASKS ─────────────────────────────────────────────────────────

    summary = scrape_all_categories()
    validate_output(summary)


# Instantiate the DAG
fairprice_pipeline()