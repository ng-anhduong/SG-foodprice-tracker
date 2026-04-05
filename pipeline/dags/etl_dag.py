# dags/etl_dag.py
#
# Runs transform + load only after all 4 scraper DAGs succeed.
# Scheduled at 4:30 PM SGT (8:30 AM UTC) — after Sheng Siong finishes at 3:30 PM.

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))


@dag(
    dag_id="etl_transform_load",
    description="Transforms and loads data after all 4 scrapers succeed",
    schedule="30 8 * * *",  # 4:30 PM SGT (8:30 AM UTC)
    start_date=datetime(2026, 4, 5),
    catchup=False,
    tags=["etl", "transform", "load"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def etl_pipeline():

    # ── Wait for all 4 scrapers ───────────────────────────────────────────────

    wait_fairprice = ExternalTaskSensor(
        task_id="wait_fairprice",
        external_dag_id="fairprice_scraper",
        external_task_id="validate_output",
        timeout=7200,          # wait up to 2 hours
        mode="reschedule",     # frees up worker slot while waiting
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

    # ── Transform ─────────────────────────────────────────────────────────────

    @task()
    def run_transform():
        from datetime import datetime as dt
        from pipeline.transform import run as transform
        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Transforming data for {date_str}")
        transform(date_str)

    # ── Load ──────────────────────────────────────────────────────────────────

    @task()
    def run_load():
        from datetime import datetime as dt
        from pipeline.load import load_date
        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Loading data for {date_str}")
        load_date(date_str)

    # ── Wire up ───────────────────────────────────────────────────────────────

    sensors = [wait_fairprice, wait_redmart, wait_coldstorage, wait_shengsiong]
    transform_task = run_transform()
    load_task = run_load()

    sensors >> transform_task >> load_task


etl_pipeline()
