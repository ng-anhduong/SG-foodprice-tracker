# dags/shengsiong_dag.py

import json
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# ── CONFIG ────────────────────────────────────────────────────────────────────

BASE_URL = "https://shengsiong.com.sg"

CATEGORIES = [
    ("breakfast-spreads",      "Breakfast & Spreads"),
    ("dairy-chilled-eggs",     "Dairy, Chilled & Eggs"),
    ("fruits",                 "Fruits"),
    ("vegetables",             "Vegetables"),
    ("meat-poultry-seafood",   "Meat, Poultry & Seafood"),
    ("beverages",              "Beverages"),
    ("rice-noodles-pasta",     "Rice, Noodles & Pasta"),
    ("frozen-goods",           "Frozen Goods"),
    ("dried-food-herbs",       "Dried Food & Herbs"),
    ("cooking-baking",         "Cooking & Baking"),
    ("convenience-food-113",   "Convenience Food"),
    ("snacks-confectioneries", "Snacks & Confectioneries"),
]

STORE = "shengsiong"

PAGE_LOAD_TIMEOUT = 15_000   # ms
SCROLL_PAUSE      = 2_500    # ms
MAX_STALE_SCROLLS = 3

# Update this to your actual project path
RAW_DATA_PATH = os.path.expanduser("~/Documents/GitHub/SG-foodprice-tracker/data/raw/shengsiong")


# ── DAG DEFINITION ────────────────────────────────────────────────────────────

@dag(
    dag_id="shengsiong_scraper",
    description="Scrapes Sheng Siong product prices daily and saves raw JSON",
    schedule="20 6 * * *",   # runs every day at 2:20pm SGT (6:20am UTC)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["shengsiong", "scraping"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
)
def shengsiong_pipeline():

    # ── TASK 1: Scrape all categories ─────────────────────────────────────────

    @task()
    def scrape_all_categories() -> dict:
        """
        Launches a headless Chromium browser via Playwright, scrolls each
        category page to exhaust infinite scroll, extracts product cards
        from the DOM, and saves raw JSON per category.
        """
        from playwright.sync_api import sync_playwright

        def scroll_to_load_all(page) -> int:
            prev_count = 0
            stale = 0
            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(SCROLL_PAUSE)
                count = page.eval_on_selector_all(".product-preview", "els => els.length")
                if count > prev_count:
                    print(f"      {count} products loaded...")
                    prev_count = count
                    stale = 0
                else:
                    stale += 1
                    if stale >= MAX_STALE_SCROLLS:
                        break
            return prev_count

        def extract_from_dom(page, category_slug: str, category_name: str) -> list:
            scraped_at = datetime.now().isoformat()
            return page.evaluate("""
                (args) => {
                    const { category_slug, category_name, store, scraped_at } = args;
                    const results = [];
                    document.querySelectorAll('.product-preview').forEach(card => {
                        const nameEl = card.querySelector('.product-name');
                        const name = nameEl ? nameEl.innerText.trim() : null;

                        const unitEl = card.querySelector('.product-packSize');
                        const unit = unitEl ? unitEl.innerText.trim() : null;

                        const priceBlock = card.querySelector('.product-price');
                        if (!priceBlock) return;

                        const promoEl   = priceBlock.querySelector('.promo-price');
                        const regularEl = priceBlock.querySelector('span:not(.promo-price)');
                        const priceText = promoEl
                            ? promoEl.innerText.trim()
                            : (regularEl ? regularEl.innerText.trim() : null);
                        const price = priceText
                            ? parseFloat(priceText.replace(/[^0-9.]/g, ''))
                            : null;

                        const prevEl   = priceBlock.querySelector('.previous-price');
                        const prevText = prevEl ? prevEl.innerText.trim() : null;
                        const original_price = prevText
                            ? parseFloat(prevText.replace(/[^0-9.]/g, ''))
                            : null;

                        const href = card.getAttribute('href') || '';

                        if (name && price) {
                            results.push({
                                name,
                                brand: null,
                                price_sgd: price,
                                original_price_sgd: original_price,
                                discount_sgd: (original_price && price)
                                    ? parseFloat((original_price - price).toFixed(2))
                                    : null,
                                unit,
                                main_category: category_name,
                                subcategory: null,
                                category_slug,
                                product_url: href,
                                store,
                                scraped_at,
                            });
                        }
                    });
                    return results;
                }
            """, {"category_slug": category_slug, "category_name": category_name,
                  "store": STORE, "scraped_at": scraped_at})

        def scrape_category(browser, category_slug: str, category_name: str) -> list:
            print(f"\n  Scraping: {category_name} ({category_slug})")
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()
            try:
                page.goto(
                    f"{BASE_URL}/{category_slug}",
                    timeout=30_000,
                    wait_until="domcontentloaded",
                )
                page.wait_for_selector(".product-preview", timeout=PAGE_LOAD_TIMEOUT)
            except Exception as e:
                print(f"    Failed to load page: {e}")
                context.close()
                return []

            total = scroll_to_load_all(page)
            print(f"    Finished scrolling — {total} products in DOM")

            products = extract_from_dom(page, category_slug, category_name)

            if not products:
                debug_path = os.path.join("data", "debug", f"shengsiong_{category_slug}.html")
                os.makedirs(os.path.dirname(debug_path), exist_ok=True)
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(page.content())
                print(f"    No products — debug HTML saved to {debug_path}")

            context.close()
            print(f"    Extracted {len(products)} products")
            return products

        # ── Run scraper ───────────────────────────────────────────────────────

        date_str = datetime.now().strftime("%Y-%m-%d")
        folder = os.path.join(RAW_DATA_PATH, date_str)
        os.makedirs(folder, exist_ok=True)

        summary = {}

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)

            for slug, name in CATEGORIES:
                products = scrape_category(browser, slug, name)

                if products:
                    filepath = os.path.join(folder, f"{slug}.json")
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(products, f, indent=2, ensure_ascii=False)
                    print(f"    Saved {len(products)} products → {filepath}")
                    summary[slug] = len(products)
                else:
                    print(f"    Skipping save — nothing found for {slug}")
                    summary[slug] = 0

                import time
                time.sleep(3)

            browser.close()

        return summary

    # ── TASK 2: Validate the output ───────────────────────────────────────────

    @task()
    def validate_output(summary: dict):
        """Logs per-category counts and warns about any empty categories."""
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
            print("Check if the category slugs are still valid.")
        else:
            print("\nAll categories scraped successfully.")

    # ── WIRE UP TASKS ─────────────────────────────────────────────────────────

    summary = scrape_all_categories()
    validate_output(summary)


# Instantiate the DAG
shengsiong_pipeline()
