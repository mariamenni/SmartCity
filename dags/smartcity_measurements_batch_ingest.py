from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    dag_id="smartcity_measurements_batch_ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "batch", "j1"],
)
def smartcity_measurements_batch_ingest():
    todo_extract = EmptyOperator(task_id="todo_extract_measurements")
    todo_stage = EmptyOperator(task_id="todo_stage_raw")
    todo_load = EmptyOperator(task_id="todo_load_timescaledb")

    todo_extract >> todo_stage >> todo_load


smartcity_measurements_batch_ingest()
