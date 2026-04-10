from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    dag_id="smartcity_measurements_consumer_minutely",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "consumer", "j2"],
)
def smartcity_measurements_consumer_minutely():
    todo_consume = EmptyOperator(task_id="todo_poll_api")
    todo_transform = EmptyOperator(task_id="todo_transform_micro_batch")
    todo_flush = EmptyOperator(task_id="todo_flush_to_timescaledb")

    todo_consume >> todo_transform >> todo_flush


smartcity_measurements_consumer_minutely()
