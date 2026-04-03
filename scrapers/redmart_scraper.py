# scrapers/redmart_scraper.py

import json
import os
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


CATEGORIES = [
    {
        "url": "https://redmart.lazada.sg/beverages/?m=redmart",
        "raw_category": "Beverages",
        "standardized_category": "Drinks",
        "category_slug": "drinks",
    },
    {
        "url": "https://redmart.lazada.sg/shop-dairy-chilled-&-eggs/?m=redmart",
        "raw_category": "Dairy, Chilled & Eggs",
        "standardized_category": "Dairy",
        "category_slug": "dairy-chilled-eggs",
    },
    {
        "url": "https://redmart.lazada.sg/shop-Groceries-FoodStaplesCookingEssentials/?m=redmart",
        "raw_category": "Food Staples & Cooking Essentials",
        "standardized_category": "Staples",
        "category_slug": "rice-noodles-cooking-ingredients",
    },
    {
        "url": "https://redmart.lazada.sg/shop-groceries-fresh-produce-fresh-fruit/?m=redmart",
        "raw_category": "Fresh Produce",
        "standardized_category": "Fresh Fruit",
        "category_slug": "fruits",
    },
    {
        "url": "https://redmart.lazada.sg/shop-groceries-fresh-produce-fresh-vegetables/?m=redmart",
        "raw_category": "Fresh Produce",
        "standardized_category": "Fresh Vegetables",
        "category_slug": "vegetables",
    },
    {
        "url": "https://redmart.lazada.sg/shop-groceries-meat-seafood-fresh-meat/?m=redmart",
        "raw_category": "Meat & Seafood",
        "standardized_category": "Fresh Meat",
        "category_slug": "meat",
    },
    {
        "url": "https://redmart.lazada.sg/shop-groceries-meat-seafood-fresh-seafood/?m=redmart",
        "raw_category": "Meat & Seafood",
        "standardized_category": "Fresh Seafood",
        "category_slug": "seafood",
    },
    {
        "url": "https://redmart.lazada.sg/shop-groceries-frozen/?m=redmart",
        "raw_category": "Frozen",
        "standardized_category": "Frozen",
        "category_slug": "frozen",
    },
    {
        "url": "https://redmart.lazada.sg/shop-snacks-&-confectionery/?&m=redmart",
        "raw_category": "Snack & Confectionery",
        "standardized_category": "Snack & Confectionery",
        "category_slug": "snack-and-confectionery",
    },
    {
        "url": "https://redmart.lazada.sg/shop-bakery-&-breakfast/?m=redmart",
        "raw_category": "Bakery & Breakfast",
        "standardized_category": "Bakery & Breakfast",
        "category_slug": "bakery-breakfast",
    },
    {
        "url": "https://redmart.lazada.sg/shop-cooking-sauces-condiments-&-dressings/?m=redmart",
        "raw_category": "Cooking Sauces, Condiments & Dressings",
        "standardized_category": "Cooking Sauces",
        "category_slug": "condiments-and-sauces",
    },
]

CATEGORY_FILTER = {
    slug.strip()
    for slug in os.getenv("REDMART_CATEGORY", "").split(",")
    if slug.strip()
}

MAX_PRODUCTS_PER_CATEGORY = 410  # ~4500 total across 11 categories


def extract_product_fields(prod_link, category_link):
    listing_text = prod_link.text.strip()
    href = prod_link.get_attribute("href")
    split_text = listing_text.split("\n")
    rows = []

    for row in split_text:
        if row.strip():
            rows.append(row.strip())

    price_sgd = None
    original_price_sgd = None
    discount_sgd = None
    unit = None
    product_name = None

    price_list = []

    for row in rows:
        if "$" in row and "/" not in row:
            clean_row = row.replace("$", "").strip()

            try:
                float(clean_row)
                price_list.append(row)
            except ValueError:
                continue

    clean_prices = []
    for price in price_list:
        price_without_sign = price.replace("$", "")
        clean_prices.append(float(price_without_sign))

    if len(clean_prices) < 1:
        return None

    price_sgd = clean_prices[0]

    if len(clean_prices) > 1:
        original_price_sgd = clean_prices[1]
        discount_sgd = round(original_price_sgd - price_sgd, 2)

    for row in rows:
        cleaned = row.strip().lower()
        if (
            (cleaned.endswith(" g")
             or cleaned.endswith(" kg")
             or cleaned.endswith(" ml")
             or cleaned.endswith(" l")
             or "x" in cleaned
             or "×" in cleaned)
            and len(cleaned.split()) <= 4
        ):
            unit = row
            break

    not_name = ["save", "off", "buy", "any", "spend", "multiple promo", "sold"]

    for row in rows:
        cleaned_row = row.strip()
        lower_row = cleaned_row.lower()

        if any(text in lower_row for text in not_name):
            continue
        if lower_row in ["sold out", "out of stock"]:
            continue
        if "$" in cleaned_row:
            continue
        if cleaned_row == unit:
            continue
        if cleaned_row.startswith("(") and cleaned_row.endswith(")"):
            continue
        if cleaned_row.replace(".", "").isdigit():
            continue
        if cleaned_row.endswith("D") or cleaned_row.endswith("W"):
            continue
        if len(cleaned_row) <= 3:
            continue

        product_name = cleaned_row
        break

    if product_name is None:
        return None

    return {
        "name": product_name,
        "brand": None,
        "price_sgd": price_sgd,
        "original_price_sgd": original_price_sgd,
        "discount_sgd": discount_sgd,
        "unit": unit,
        "main_category": category_link["standardized_category"],
        "subcategory": category_link["raw_category"],
        "category_slug": category_link["category_slug"],
        "store": "redmart",
        "product_url": href,
        "scraped_at": datetime.now().isoformat(),
    }


def scrape_category(driver, category_link):
    products = []
    page = 1

    print(f"\nScraping: {category_link['category_slug']}")

    driver.get(category_link["url"])
    time.sleep(8)

    while True:
        current_url = driver.current_url

        product_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/products/"]')
        print(f"Page {page}: found {len(product_links)} product links")

        if len(product_links) == 0:
            break

        for link in product_links:
            if len(products) >= MAX_PRODUCTS_PER_CATEGORY:
                break
            item = extract_product_fields(link, category_link)
            if item is None:
                continue
            products.append(item)

        if len(products) >= MAX_PRODUCTS_PER_CATEGORY:
            print(f"    Reached cap of {MAX_PRODUCTS_PER_CATEGORY} products")
            break

        try:
            next_button = driver.find_element(
                By.CSS_SELECTOR,
                "li.ant-pagination-next button, li.ant-pagination-next a",
            )

            parent_li = next_button.find_element(By.XPATH, "./ancestor::li[1]")
            class_name = parent_li.get_attribute("class") or ""

            if "disabled" in class_name.lower():
                break

            driver.execute_script("arguments[0].click();", next_button)
            time.sleep(5)

            if driver.current_url == current_url:
                break

            page += 1

        except Exception:
            break

    return products


def save_raw(products: list[dict], category_slug: str):
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join("data", "raw", "redmart", date_str)
    os.makedirs(folder, exist_ok=True)

    filepath = os.path.join(folder, f"{category_slug}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    print(f"    Saved -> {filepath}")


def run():
    print("=" * 60)
    print(f"Redmart Scraper started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    total = 0
    summary = {}

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--window-size=1600,1200")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        categories = [
            each_category
            for each_category in CATEGORIES
            if not CATEGORY_FILTER or each_category["category_slug"] in CATEGORY_FILTER
        ]

        for each_category in categories:
            products = scrape_category(driver, each_category)

            category_slug = each_category["category_slug"]

            if products:
                save_raw(products, category_slug)
                count = len(products)
                total += count
                summary[category_slug] = count
            else:
                print("No products found - skipping save")
                summary[category_slug] = 0

            time.sleep(2)

    finally:
        driver.quit()

    print("\n" + "=" * 60)
    print(f"Done. Total products scraped: {total}")
    print("=" * 60)

    return summary


if __name__ == "__main__":
    run()
