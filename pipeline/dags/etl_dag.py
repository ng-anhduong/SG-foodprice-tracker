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
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@dag(
    dag_id="etl_transform_load",
    description="Transforms and loads data after all 4 scrapers succeed",
    schedule="35 6 * * *",  # 2:35 PM SGT (6:35 AM UTC)
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

    # execution_delta = how far back to look from ETL's execution time (06:35 UTC)
    # FairPrice, RedMart, Cold Storage all run at 06:00 UTC → delta = 35min
    wait_fairprice = ExternalTaskSensor(
        task_id="wait_fairprice",
        external_dag_id="fairprice_scraper",
        external_task_id="validate_output",
        execution_delta=timedelta(minutes=35),
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    # RedMart runs at 06:00 UTC → delta = 35min
    wait_redmart = ExternalTaskSensor(
        task_id="wait_redmart",
        external_dag_id="redmart_scraper",
        external_task_id="validate_output",
        execution_delta=timedelta(minutes=35),
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    # Cold Storage runs at 06:00 UTC → delta = 35min
    wait_coldstorage = ExternalTaskSensor(
        task_id="wait_coldstorage",
        external_dag_id="coldstorage_scraper",
        external_task_id="validate_output",
        execution_delta=timedelta(minutes=35),
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    # Sheng Siong runs at 06:20 UTC → delta = 15min
    wait_shengsiong = ExternalTaskSensor(
        task_id="wait_shengsiong",
        external_dag_id="shengsiong_scraper",
        external_task_id="validate_output",
        execution_delta=timedelta(minutes=15),
        timeout=7200,
        mode="reschedule",
        poke_interval=60,
    )

    # ── Transform ─────────────────────────────────────────────────────────────

    @task()
    def run_transform():
        import sys
        from pathlib import Path
        repo_root = str(Path(__file__).resolve().parents[2])
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from datetime import datetime as dt
        from pipeline.etl.transform import run as transform
        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Transforming data for {date_str}")
        transform(date_str)

    # ── Load ──────────────────────────────────────────────────────────────────

    @task()
    def run_load():
        import sys
        from pathlib import Path
        repo_root = str(Path(__file__).resolve().parents[2])
        if repo_root not in sys.path:
            sys.path.insert(0, repo_root)
        from datetime import datetime as dt
        from pipeline.etl.load import load_date
        date_str = dt.now().strftime("%Y-%m-%d")
        print(f"Loading data for {date_str}")
        load_date(date_str)

    # ── Wire up ───────────────────────────────────────────────────────────────

    sensors = [wait_fairprice, wait_redmart, wait_coldstorage, wait_shengsiong]
    transform_task = run_transform()
    load_task = run_load()

    sensors >> transform_task >> load_task


etl_pipeline()
