# dags/seoul_ppltn_collect.py
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from collectors.r2_storage import get_r2_client
from collectors.seoul_citydata import collect_and_store, load_areas

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path("/opt/airflow/project")
AREAS_FILE = PROJECT_ROOT / "collectors" / "areas.txt"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


class _BrokenR2Client:
    def __init__(self, error: Exception):
        self._error = error

    def put_object(self, **kwargs):
        raise self._error


def run_collect_and_store() -> None:
    api_key = os.environ["SEOUL_API_KEY"]
    r2_bucket = os.environ.get("R2_BUCKET_NAME", "")
    area_names = load_areas(AREAS_FILE)
    try:
        r2_client = get_r2_client()
    except Exception as exc:
        logger.warning("failed to construct R2 client, all R2 uploads will fail until R2_* env vars are set: %s", exc)
        r2_client = _BrokenR2Client(exc)

    results = collect_and_store(
        area_names=area_names,
        api_key=api_key,
        base_dir=RAW_DATA_DIR,
        r2_client=r2_client,
        r2_bucket=r2_bucket,
    )

    succeeded = [r for r in results if r["error"] is None]
    failed = [r for r in results if r["error"] is not None]
    for r in failed:
        logger.warning("collect failed for %s: %s", r["area_name"], r["error"])
    logger.info("collected %d/%d areas successfully", len(succeeded), len(results))

    if not succeeded:
        raise RuntimeError(f"all {len(results)} areas failed to collect")


with DAG(
    dag_id="seoul_ppltn_collect",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 6, 28),
    catchup=False,
) as dag:
    collect_task = PythonOperator(
        task_id="collect_and_store",
        python_callable=run_collect_and_store,
    )
