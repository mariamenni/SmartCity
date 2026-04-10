from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    dag_id="smartcity_sensors_dims_refresh_daily",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "dimensions", "j1"],
)
def smartcity_sensors_dims_refresh_daily():
    todo_extract = EmptyOperator(task_id="todo_extract_dimensions")
    todo_load = EmptyOperator(task_id="todo_load_dimensions")

    todo_extract >> todo_load


smartcity_sensors_dims_refresh_daily()
