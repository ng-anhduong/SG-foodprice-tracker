# dags/redmart_dag.py
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
from airflow.decorators import dag, task

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from scrapers.redmart_scraper import run as run_redmart

RAW_DATA_PATH = REPO_ROOT / "data" / "raw" / "redmart"


@dag(
    dag_id="redmart_scraper",
    schedule="0 6 * * *",  # runs every day at 2pm SGT (6am UTC)
    start_date=datetime(2026, 4, 2),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["redmart", "scraping"],
)
def redmart_pipeline():

    @task()
    def redmart_scrape():
        return run_redmart()

    @task()
    def validate_output(summary: dict):
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
        else:
            print("\nAll categories scraped successfully.")

    summary = redmart_scrape()
    validate_output(summary)


redmart_pipeline()