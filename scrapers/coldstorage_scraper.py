import asyncio
import json
import os
import re
from datetime import datetime
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from playwright.async_api import Browser, Page, async_playwright


BASE_URL = "https://coldstorage.com.sg"
STORE = "coldstorage"

CATEGORIES = [
    {
        "slug": "beverages",
        "name": "Beverages",
        "url": "https://coldstorage.com.sg/category/beverages-1599",
    },
    {
        "slug": "dairy-chilled-eggs",
        "name": "Dairy, Chilled & Eggs",
        "url": "https://coldstorage.com.sg/category/dairy-chilled-eggs-1583",
    },
    {
        "slug": "fruits-vegetables",
        "name": "Fruits & Vegetables",
        "url": "https://coldstorage.com.sg/category/fruits-vegetables",
    },
    {
        "slug": "meat-seafood",
        "name": "Meat & Seafood",
        "url": "https://coldstorage.com.sg/category/meat-seafood",
    },
    {
        "slug": "rice-oil-noodles",
        "name": "Rice, Oil & Noodles",
        "url": "https://coldstorage.com.sg/category/rice-oil-noodles-1714",
    },
    {
        "slug": "breakfast-bakery",
        "name": "Breakfast & Bakery",
        "url": "https://coldstorage.com.sg/category/breakfast-bakery",
    },
    {
        "slug": "snacks-confectionery",
        "name": "Snacks & Confectionery",
        "url": "https://coldstorage.com.sg/category/snacks-confectionery",
    },
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

PAGE_LOAD_TIMEOUT_MS = 90_000
SCROLL_PAUSE_MS = 2_500
MAX_STALE_SCROLLS = 4
CATEGORY_SLEEP_SECONDS = 2
DETAIL_SLEEP_SECONDS = 0.15
ENABLE_DETAIL_ENRICHMENT = False
DETAIL_CONCURRENCY = 6
SAVE_DEBUG_HTML = True
CATEGORY_FILTER = {
    slug.strip()
    for slug in os.getenv("COLDSTORAGE_CATEGORY", "").split(",")
    if slug.strip()
}


def make_empty_record() -> dict[str, Any]:
    return {
        "name": None,
        "brand": None,
        "price_sgd": None,
        "original_price_sgd": None,
        "discount_sgd": None,
        "promo_text": None,
        "unit": None,
        "main_category": None,
        "category_slug": None,
        "country_of_origin": None,
        "storage_type": None,
        "item_code": None,
        "product_url": None,
        "image_url": None,
        "store": STORE,
        "scraped_at": None,
    }


def clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def maybe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().replace(",", "")
    if not s:
        return None

    try:
        return float(s)
    except ValueError:
        return None


def normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    return urljoin(BASE_URL, url)


def infer_brand_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None

    tokens = name.split()
    if not tokens:
        return None

    if len(tokens) >= 2 and tokens[0].lower() in {"a2", "dr", "mr"}:
        return f"{tokens[0]} {tokens[1]}"

    return tokens[0]


def compute_discount(
    price_sgd: Optional[float], original_price_sgd: Optional[float]
) -> Optional[float]:
    if price_sgd is None or original_price_sgd is None:
        return None

    diff = round(original_price_sgd - price_sgd, 2)
    return diff if diff > 0 else None


def extract_unit_from_name(name: Optional[str]) -> Optional[str]:
    if not name:
        return None

    match = re.search(
        r"(?:,|\b)\s*([0-9]+(?:\.[0-9]+)?\s*(?:kg|g|mg|ml|l|cl|oz|lb|pcs?|pc|s|x[0-9]+).*)$",
        name,
        flags=re.I,
    )
    if match:
        return clean_text(match.group(1))
    return None


def extract_source_image_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None

    parsed = urlparse(url)
    if "_next/image" not in parsed.path:
        return normalize_url(url)

    src = parse_qs(parsed.query).get("url", [None])[0]
    return normalize_url(unquote(src)) if src else normalize_url(url)


def validate_record(record: dict[str, Any]) -> dict[str, Any]:
    for key, value in list(record.items()):
        if isinstance(value, str):
            record[key] = clean_text(value)

    for key in ("price_sgd", "original_price_sgd", "discount_sgd"):
        record[key] = maybe_float(record.get(key))

    if record.get("discount_sgd") is None:
        record["discount_sgd"] = compute_discount(
            record.get("price_sgd"),
            record.get("original_price_sgd"),
        )

    schema = make_empty_record()
    for key, default_value in schema.items():
        record.setdefault(key, default_value)

    return record


def save_raw(products: list[dict[str, Any]], category_slug: str) -> None:
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join("data", "raw", STORE, date_str)
    os.makedirs(folder, exist_ok=True)

    filepath = os.path.join(folder, f"{category_slug}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    print(f"    Saved -> {filepath}")


def save_debug_html(category_slug: str, html: str) -> None:
    if not SAVE_DEBUG_HTML:
        return

    folder = os.path.join("data", "debug")
    os.makedirs(folder, exist_ok=True)

    filepath = os.path.join(folder, f"{STORE}_{category_slug}.html")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"    Debug HTML saved -> {filepath}")


async def build_browser() -> Browser:
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    browser._playwright = playwright
    return browser


async def close_browser(browser: Browser) -> None:
    playwright = getattr(browser, "_playwright", None)
    await browser.close()
    if playwright is not None:
        await playwright.stop()


async def new_page(browser: Browser) -> Page:
    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1440, "height": 1800},
        locale="en-SG",
    )
    page = await context.new_page()
    return page


async def scroll_to_load_all_products(page: Page) -> int:
    prev_count = -1
    stale = 0
    step = 0

    while stale < MAX_STALE_SCROLLS:
        count = await page.locator("a.product-item_product-item__details__wi7dH").count()
        if count == prev_count:
            stale += 1
        else:
            stale = 0
            prev_count = count
            print(f"      Loaded {count} products so far")

        await page.mouse.wheel(0, 12_000)
        await page.wait_for_timeout(SCROLL_PAUSE_MS)
        step += 1

        if step > 80:
            break

    return max(prev_count, 0)


async def extract_listing_products(
    page: Page, category_slug: str, category_name: str
) -> list[dict[str, Any]]:
    scraped_at = datetime.now().isoformat()

    raw_products = await page.evaluate(
        """
        (args) => {
            const { categorySlug, categoryName, store, scrapedAt } = args;
            const cards = Array.from(document.querySelectorAll('div.product-item_product-item__BWbnO'));

            return cards.map((card) => {
                const detailsLink = card.querySelector('a.product-item_product-item__details__wi7dH');
                const imageLink = card.querySelector('a.product-item_product-item__img-link__FJWGM');
                const nameEl = card.querySelector('p.product-item_product-item__name__piwWX');
                const blurbEls = Array.from(card.querySelectorAll('.product-item_product-item__blurbs__yInZN .blurb_blurb__oN_zE'));
                const priceEls = Array.from(card.querySelectorAll('.product-price_product-price__price__qk_1n'));
                const imgEl = card.querySelector('img');

                const href = detailsLink?.getAttribute('href') || imageLink?.getAttribute('href') || null;
                const name = nameEl?.innerText?.trim() || detailsLink?.getAttribute('aria-label') || imgEl?.getAttribute('alt') || null;

                let price = null;
                let originalPrice = null;

                for (const el of priceEls) {
                    const text = (el.innerText || '').trim();
                    const parsed = parseFloat(text.replace(/[^0-9.]/g, ''));
                    if (Number.isNaN(parsed)) continue;

                    if (el.className.includes('price--original')) {
                        originalPrice = parsed;
                    } else if (price === null) {
                        price = parsed;
                    }
                }

                const promoTexts = blurbEls
                    .map((el) => (el.innerText || '').trim())
                    .filter(Boolean);

                return {
                    name,
                    brand: null,
                    price_sgd: price,
                    original_price_sgd: originalPrice,
                    discount_sgd: null,
                    promo_text: promoTexts.length ? promoTexts.join(' | ') : null,
                    unit: null,
                    main_category: categoryName,
                    category_slug: categorySlug,
                    country_of_origin: null,
                    storage_type: null,
                    item_code: null,
                    product_url: href,
                    image_url: imgEl?.getAttribute('src') || imgEl?.getAttribute('data-src') || null,
                    store,
                    scraped_at: scrapedAt,
                };
            });
        }
        """,
        {
            "categorySlug": category_slug,
            "categoryName": category_name,
            "store": STORE,
            "scrapedAt": scraped_at,
        },
    )

    products: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for product in raw_products:
        product["product_url"] = normalize_url(product.get("product_url"))
        product["image_url"] = extract_source_image_url(product.get("image_url"))

        name = clean_text(product.get("name"))
        product["name"] = name
        product["brand"] = infer_brand_from_name(name)
        product["unit"] = extract_unit_from_name(name)
        product["discount_sgd"] = compute_discount(
            maybe_float(product.get("price_sgd")),
            maybe_float(product.get("original_price_sgd")),
        )

        product_url = product.get("product_url")
        if not product_url or product_url in seen_urls:
            continue
        if not name or product.get("price_sgd") is None:
            continue

        seen_urls.add(product_url)
        products.append(validate_record(product))

    return products


async def enrich_product_detail(browser: Browser, product: dict[str, Any]) -> dict[str, Any]:
    product_url = product.get("product_url")
    if not product_url:
        return product

    page = await new_page(browser)

    try:
        await page.goto(product_url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT_MS)
        await page.wait_for_timeout(int(DETAIL_SLEEP_SECONDS * 1000))

        details = await page.evaluate(
            """
            () => {
                const textOf = (selector) => {
                    const node = document.querySelector(selector);
                    return node ? node.innerText.trim() : null;
                };

                const allText = document.body ? document.body.innerText : '';
                const name = textOf('h1');
                const priceNodes = Array.from(document.querySelectorAll('.product-price_product-price__price__qk_1n'));
                const imageNode = document.querySelector('img');

                let price = null;
                let originalPrice = null;
                for (const el of priceNodes) {
                    const parsed = parseFloat((el.innerText || '').replace(/[^0-9.]/g, ''));
                    if (Number.isNaN(parsed)) continue;

                    if (el.className.includes('price--original')) {
                        originalPrice = parsed;
                    } else if (price === null) {
                        price = parsed;
                    }
                }

                const readLabel = (label) => {
                    const regex = new RegExp(label + '\\\\s+([^\\\\n]+)', 'i');
                    const match = allText.match(regex);
                    return match ? match[1].trim() : null;
                };

                const itemCodeMatch = allText.match(/Item code:\\s*([A-Za-z0-9\\-]+)/i);

                return {
                    name,
                    price_sgd: price,
                    original_price_sgd: originalPrice,
                    unit: readLabel('Size'),
                    country_of_origin: readLabel('Country'),
                    storage_type: readLabel('Storage'),
                    item_code: itemCodeMatch ? itemCodeMatch[1] : null,
                    image_url: imageNode?.getAttribute('src') || imageNode?.getAttribute('data-src') || null,
                };
            }
            """
        )

        if details.get("name"):
            product["name"] = clean_text(details["name"])
            product["brand"] = infer_brand_from_name(product["name"])

        if details.get("price_sgd") is not None:
            product["price_sgd"] = details["price_sgd"]
        if details.get("original_price_sgd") is not None:
            product["original_price_sgd"] = details["original_price_sgd"]

        for key in ("unit", "country_of_origin", "storage_type", "item_code"):
            if details.get(key):
                product[key] = clean_text(details[key])

        if details.get("image_url"):
            product["image_url"] = extract_source_image_url(details["image_url"])

        product["discount_sgd"] = compute_discount(
            maybe_float(product.get("price_sgd")),
            maybe_float(product.get("original_price_sgd")),
        )

        return validate_record(product)
    except Exception as e:
        print(f"      Detail scrape failed for {product_url}: {e}")
        return product
    finally:
        await page.context.close()


async def enrich_products(
    browser: Browser, products: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(DETAIL_CONCURRENCY)

    async def worker(product: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            return await enrich_product_detail(browser, product)

    return await asyncio.gather(*(worker(product) for product in products))


async def scrape_category(
    browser: Browser,
    category_slug: str,
    category_name: str,
    category_url: str,
) -> list[dict[str, Any]]:
    print(f"\n  Scraping: {category_name} ({category_slug})")
    print(f"    URL: {category_url}")

    page = await new_page(browser)

    try:
        await page.goto(category_url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT_MS)
        await page.wait_for_selector(
            "div.product-item_product-item__BWbnO",
            timeout=PAGE_LOAD_TIMEOUT_MS,
        )
        await page.wait_for_timeout(2_000)

        loaded_count = await scroll_to_load_all_products(page)
        print(f"    Finished loading category DOM with {loaded_count} products")

        html = await page.content()
        if SAVE_DEBUG_HTML:
            save_debug_html(category_slug, html)

        products = await extract_listing_products(page, category_slug, category_name)
        print(f"    Extracted {len(products)} products from listing")

        if ENABLE_DETAIL_ENRICHMENT and products:
            print(
                f"    Enriching product detail pages for {len(products)} products"
            )
            products = await enrich_products(browser, products)

        return [validate_record(product) for product in products]
    except Exception as e:
        print(f"    Failed to scrape {category_slug}: {e}")
        if SAVE_DEBUG_HTML:
            try:
                save_debug_html(category_slug, await page.content())
            except Exception:
                pass
        return []
    finally:
        await page.context.close()


async def run() -> None:
    print("=" * 70)
    print(f"Cold Storage Scraper started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    browser = await build_browser()
    total = 0

    try:
        categories = [
            category
            for category in CATEGORIES
            if not CATEGORY_FILTER or category["slug"] in CATEGORY_FILTER
        ]

        for category in categories:
            products = await scrape_category(
                browser=browser,
                category_slug=category["slug"],
                category_name=category["name"],
                category_url=category["url"],
            )

            if products:
                save_raw(products, category["slug"])
                total += len(products)
            else:
                print(f"    No products found for {category['slug']} - skipping save")

            await asyncio.sleep(CATEGORY_SLEEP_SECONDS)
    finally:
        await close_browser(browser)

    print("\n" + "=" * 70)
    print(f"Done. Total products scraped: {total}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(run())
