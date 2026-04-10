from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    dag_id="smartcity_alert_check_batch",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "alerting", "j1"],
)
def smartcity_alert_check_batch():
    todo_read = EmptyOperator(task_id="todo_read_recent_measurements")
    todo_detect = EmptyOperator(task_id="todo_detect_thresholds")
    todo_write = EmptyOperator(task_id="todo_write_alerts")

    todo_read >> todo_detect >> todo_write


smartcity_alert_check_batch()
