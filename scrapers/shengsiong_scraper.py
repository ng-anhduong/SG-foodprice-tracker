# scrapers/shengsiong_scraper.py
#
# Sheng Siong uses Meteor.js (client-side rendered) + Imperva WAF.
# Product data loads via WebSockets (DDP), not plain HTTP, so we use
# Playwright to drive a headless browser, scroll to trigger loading,
# and extract from the fully rendered DOM.
#
# Install deps:
#   pip install playwright
#   python -m playwright install chromium

import asyncio
import json
import os
from datetime import datetime

from playwright.async_api import async_playwright


BASE_URL = "https://shengsiong.com.sg"

# Real subcategory slugs confirmed from the live site navigation.
CATEGORIES = [
    ("breakfast-spreads", "Breakfast & Spreads"),
    ("dairy-chilled-eggs", "Dairy, Chilled & Eggs"),
    ("fruits", "Fruits"),
    ("vegetables", "Vegetables"),
    ("meat-poultry-seafood", "Meat, Poultry & Seafood"),
    ("beverages", "Beverages"),
    ("rice-noodles-pasta", "Rice, Noodles & Pasta"),
    ("frozen-goods", "Frozen Goods"),
    ("dried-food-herbs", "Dried Food & Herbs"),
    ("cooking-baking", "Cooking & Baking"),
    ("convenience-food-113", "Convenience Food"),
    ("snacks-confectioneries", "Snacks & Confectioneries"),
]

STORE = "shengsiong"

PAGE_LOAD_TIMEOUT = 15_000
SCROLL_PAUSE = 2_500
MAX_STALE_SCROLLS = 3


async def scroll_to_load_all(page) -> int:
    """Scroll down until no new products appear. Returns final product count."""
    prev_count = 0
    stale = 0

    while True:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(SCROLL_PAUSE)

        count = await page.eval_on_selector_all(".product-preview", "els => els.length")

        if count > prev_count:
            print(f"      {count} products loaded...")
            prev_count = count
            stale = 0
        else:
            stale += 1
            if stale >= MAX_STALE_SCROLLS:
                break

    return prev_count


async def extract_from_dom(page, category_slug: str, category_name: str) -> list[dict]:
    """
    Extract all rendered product cards from the page.
    Confirmed Sheng Siong class names:
      .product-preview               card anchor, href = /product/<slug>
      .product-name                  product name text
      .product-packSize              unit / weight
      .product-price > span          regular price
      .product-price .promo-price    discounted selling price
      .product-price .previous-price original price
    """
    scraped_at = datetime.now().isoformat()

    products = await page.evaluate(
        """
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

                const promoEl = priceBlock.querySelector('.promo-price');
                const regularEl = priceBlock.querySelector('span:not(.promo-price)');
                const priceText = promoEl
                    ? promoEl.innerText.trim()
                    : (regularEl ? regularEl.innerText.trim() : null);
                const price = priceText
                    ? parseFloat(priceText.replace(/[^0-9.]/g, ''))
                    : null;

                const prevEl = priceBlock.querySelector('.previous-price');
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
        """,
        {
            "category_slug": category_slug,
            "category_name": category_name,
            "store": STORE,
            "scraped_at": scraped_at,
        },
    )

    return products


async def scrape_category(browser, category_slug: str, category_name: str) -> list[dict]:
    print(f"\n  Scraping: {category_name} ({category_slug})")

    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 900},
    )
    page = await context.new_page()

    try:
        await page.goto(
            f"{BASE_URL}/{category_slug}",
            timeout=30_000,
            wait_until="domcontentloaded",
        )
        await page.wait_for_selector(".product-preview", timeout=PAGE_LOAD_TIMEOUT)
    except Exception as e:
        print(f"    Failed to load page: {e}")
        await context.close()
        return []

    total = await scroll_to_load_all(page)
    print(f"    Finished scrolling - {total} products in DOM")

    products = await extract_from_dom(page, category_slug, category_name)

    if not products:
        debug_path = os.path.join("data", "debug", f"shengsiong_{category_slug}.html")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(await page.content())
        print(f"    No products extracted - debug HTML saved to {debug_path}")

    await context.close()
    print(f"    Extracted {len(products)} products")
    return products


def save_raw(products: list[dict], category_slug: str):
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join("data", "raw", STORE, date_str)
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, f"{category_slug}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    print(f"    Saved -> {filepath}")


async def run():
    print("=" * 60)
    print(f"Sheng Siong Scraper  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for slug, name in CATEGORIES:
            products = await scrape_category(browser, slug, name)
            if products:
                save_raw(products, slug)
                total += len(products)
            else:
                print(f"    Skipping save - nothing found for {slug}")

            await asyncio.sleep(3)

        await browser.close()

    print("\n" + "=" * 60)
    print(f"Done. Total products scraped: {total}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run())
