# dags/daily_pipeline_dag.py
#
# End-to-end daily pipeline:
# wait for scraper DAGs -> transform -> load -> matching -> pricing refresh.

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

PACKAGED_CATEGORIES = [
    "Beverages",
    "Dairy",
    "Staples",
    "Snacks & Confectionery",
    "Bakery & Breakfast",
]

FRESH_CANONICAL_CATEGORIES = [
    "Meat & Seafood",
    "Fruits & Vegetables",
]

PRICE_REFRESH_CATEGORIES = PACKAGED_CATEGORIES + FRESH_CANONICAL_CATEGORIES


def slug(value: str) -> str:
    return (
        value.lower()
        .replace("&", "and")
        .replace(" ", "_")
        .replace("-", "_")
    )


@dag(
    dag_id="daily_end_to_end_pipeline",
    description="Daily end-to-end pipeline from scraper completion through matching and pricing refresh",
    schedule="30 8 * * *",  # 4:30 PM SGT (8:30 AM UTC)
    start_date=datetime(2026, 4, 7),
    catchup=False,
    tags=["daily", "etl", "matching", "pricing"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def daily_end_to_end_pipeline():
    wait_fairprice = ExternalTaskSensor(
        task_id="wait_fairprice",
        external_dag_id="fairprice_scraper",
        external_task_id="validate_output",
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    wait_redmart = ExternalTaskSensor(
        task_id="wait_redmart",
        external_dag_id="redmart_scraper",
        external_task_id="validate_output",
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    wait_coldstorage = ExternalTaskSensor(
        task_id="wait_coldstorage",
        external_dag_id="coldstorage_scraper",
        external_task_id="validate_output",
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    wait_shengsiong = ExternalTaskSensor(
        task_id="wait_shengsiong",
        external_dag_id="shengsiong_scraper",
        external_task_id="validate_output",
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    @task()
    def run_transform():
        from datetime import datetime as dt
        from pipeline.etl.transform import run as transform

        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Transforming data for {date_str}")
        transform(date_str)
        return date_str

    @task()
    def run_load():
        from datetime import datetime as dt
        from pipeline.etl.load import load_date

        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Loading data for {date_str}")
        load_date(date_str)
        return date_str

    @task()
    def run_packaged_matching(category: str):
        from pipeline.matching.matching import run

        summary = run(category)
        sync = summary.get("supabase_sync", {})
        if not sync.get("synced", False):
            raise ValueError(f"Packaged matching failed to sync for {category}: {sync}")
        return summary

    @task()
    def run_meat_matching():
        from pipeline.matching.meat_produce_matching import run

        summary = run("Meat & Seafood")
        if not summary.get("supabase_sync"):
            raise ValueError("Meat matching did not return a Supabase sync summary")
        return summary

    @task()
    def run_vegetable_matching():
        from pipeline.matching.vegetable_produce_matching import run

        summary = run()
        if not summary.get("supabase_sync"):
            raise ValueError("Vegetable matching did not return a Supabase sync summary")
        return summary

    @task()
    def run_commodity_matching(category: str):
        from pipeline.matching.commodity_matching import run

        run(category)
        return {"category": category, "status": "completed"}

    @task()
    def refresh_price_tables(category: str):
        from pipeline.pricing.build_price_comparison_tables import main

        main(category)
        return {"category": category, "status": "completed"}

    sensors = [wait_fairprice, wait_redmart, wait_coldstorage, wait_shengsiong]
    transform_task = run_transform()
    load_task = run_load()

    packaged_tasks = [run_packaged_matching.override(task_id=f"match_{slug(cat)}")(cat) for cat in PACKAGED_CATEGORIES]
    meat_task = run_meat_matching()
    vegetable_task = run_vegetable_matching()

    commodity_tasks = [
        run_commodity_matching.override(task_id=f"commodity_{slug(cat)}")(cat)
        for cat in FRESH_CANONICAL_CATEGORIES
    ]

    price_tasks = [
        refresh_price_tables.override(task_id=f"refresh_prices_{slug(cat)}")(cat)
        for cat in PRICE_REFRESH_CATEGORIES
    ]

    sensors >> transform_task >> load_task

    load_task >> packaged_tasks
    load_task >> meat_task
    load_task >> vegetable_task
    load_task >> commodity_tasks

    for task_obj in packaged_tasks:
        task_obj >> price_tasks

    meat_task >> price_tasks
    vegetable_task >> price_tasks


daily_end_to_end_pipeline()
