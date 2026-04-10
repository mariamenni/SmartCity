from __future__ import annotations

from typing import Any

from airflow.providers.http.hooks.http import HttpHook


class SensorAPIHook(HttpHook):
    conn_name_attr = "sensor_api_conn_id"
    default_conn_name = "sensor_api"
    conn_type = "http"
    hook_name = "SmartCity Sensor API"

    def __init__(self, sensor_api_conn_id: str = default_conn_name, method: str = "GET") -> None:
        super().__init__(method=method, http_conn_id=sensor_api_conn_id)

    def get_sensors(self) -> Any:
        raise NotImplementedError("TODO Frederic: implement get_sensors()")

    def get_measurements(self, start_ts: str | None = None, end_ts: str | None = None) -> Any:
        raise NotImplementedError("TODO Frederic: implement get_measurements()")
