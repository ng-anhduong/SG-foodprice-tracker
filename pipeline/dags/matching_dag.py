# dags/matching_dag.py
#
# Runs post-load matching and price-table refresh after the ETL DAG succeeds.

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


@dag(
    dag_id="product_matching_pipeline",
    description="Runs category-specific matching and cached price-table refresh after ETL load",
    schedule="45 8 * * *",  # 4:45 PM SGT (8:45 AM UTC)
    start_date=datetime(2026, 4, 7),
    catchup=False,
    tags=["matching", "pricing", "supabase"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def product_matching_pipeline():
    wait_for_load = ExternalTaskSensor(
        task_id="wait_for_etl_load",
        external_dag_id="etl_transform_load",
        external_task_id="run_load",
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

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
        sync = summary.get("supabase_sync", {})
        if not sync:
            raise ValueError("Meat matching did not return a Supabase sync summary")
        return summary

    @task()
    def run_vegetable_matching():
        from pipeline.matching.vegetable_produce_matching import run

        summary = run()
        sync = summary.get("supabase_sync", {})
        if not sync:
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

    wait_for_load >> packaged_tasks
    wait_for_load >> meat_task
    wait_for_load >> vegetable_task
    wait_for_load >> commodity_tasks

    for task_obj in packaged_tasks:
        task_obj >> price_tasks

    meat_task >> price_tasks
    vegetable_task >> price_tasks


def slug(value: str) -> str:
    return (
        value.lower()
        .replace("&", "and")
        .replace(" ", "_")
        .replace("-", "_")
    )


product_matching_pipeline()
