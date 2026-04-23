from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pipeline.ml.product_clustering import run as run_clustering
from pipeline.ml.anomaly_detector import run as run_anomaly
from pipeline.ml.future_price import run as run_prediction

@dag(
    dag_id="ml_pipeline",
    schedule="0 6 * * *",   # runs every day at 2pm SGT (6am UTC)
    start_date=datetime(2026, 4, 20),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def ml_pipeline():
    @task()
    def clustering_task():
        run_clustering()

    @task()
    def anomaly_task():
        run_anomaly()

    @task()
    def prediction_task():
        run_prediction()

    c = clustering_task()
    a = anomaly_task()
    p = prediction_task()

    c >> a >> p

ml_pipeline()