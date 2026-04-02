import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRAPER_PATH = REPO_ROOT / "scrapers" / "coldstorage_scraper.py"
RAW_DATA_PATH = REPO_ROOT / "data" / "raw" / "coldstorage"

CATEGORIES = [
    {"slug": "beverages", "name": "Beverages"},
    {"slug": "dairy-chilled-eggs", "name": "Dairy, Chilled & Eggs"},
    {"slug": "fruits-vegetables", "name": "Fruits & Vegetables"},
    {"slug": "meat-seafood", "name": "Meat & Seafood"},
    {"slug": "rice-oil-noodles", "name": "Rice, Oil & Noodles"},
    {"slug": "breakfast-bakery", "name": "Breakfast & Bakery"},
    {"slug": "snacks-confectionery", "name": "Snacks & Confectionery"},
]


@dag(
    dag_id="coldstorage_scraper",
    description="Scrapes Cold Storage product prices daily and saves raw JSON",
    schedule="30 9 * * *",
    start_date=datetime(2026, 4, 2),
    catchup=False,
    tags=["coldstorage", "scraping"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
)
def coldstorage_pipeline():
    @task()
    def scrape_all_categories() -> dict:
        """
        Runs the Cold Storage scraper once per configured category and
        returns a count summary from the generated JSON files.
        """

        if not SCRAPER_PATH.exists():
            raise FileNotFoundError(f"Cold Storage scraper not found: {SCRAPER_PATH}")

        date_str = datetime.now().strftime("%Y-%m-%d")
        summary: dict[str, int] = {}

        for category in CATEGORIES:
            slug = category["slug"]
            print(f"\nScraping: {category['name']} ({slug})")

            env = os.environ.copy()
            env["COLDSTORAGE_CATEGORY"] = slug

            result = subprocess.run(
                [sys.executable, str(SCRAPER_PATH)],
                cwd=str(REPO_ROOT),
                env=env,
                capture_output=True,
                text=True,
            )

            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)

            if result.returncode != 0:
                raise RuntimeError(
                    f"Cold Storage scraper failed for {slug} with exit code {result.returncode}"
                )

            output_path = RAW_DATA_PATH / date_str / f"{slug}.json"
            if not output_path.exists():
                print(f"Missing output for {slug}: {output_path}")
                summary[slug] = 0
                continue

            with output_path.open("r", encoding="utf-8") as f:
                products = json.load(f)

            count = len(products) if isinstance(products, list) else 0
            summary[slug] = count
            print(f"Saved {count} products -> {output_path}")

        return summary

    @task()
    def validate_output(summary: dict):
        """
        Checks that each category produced at least some products.
        """
        print("\nCold Storage Scrape Summary")
        total = 0
        failed = []

        for category, count in summary.items():
            status = "OK" if count > 0 else "EMPTY"
            print(f"  {status}  {category}: {count} products")
            total += count
            if count == 0:
                failed.append(category)

        print(f"\nTotal products scraped: {total}")

        if failed:
            print(f"\nWarning: these categories returned 0 products: {failed}")
            raise ValueError(f"Cold Storage empty categories: {failed}")

        print("\nAll Cold Storage categories scraped successfully.")

    summary = scrape_all_categories()
    validate_output(summary)


coldstorage_pipeline()
