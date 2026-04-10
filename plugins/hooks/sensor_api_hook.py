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

    def get_sensors(
        self,
        sensor_type: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> Any:
        # GET /api/v1/sensors?type=<sensor_type>&status=<status>&limit=<limit>
        raise NotImplementedError("TODO Frederic: implement get_sensors()")

    def get_measurements(self, sensor_id: int, limit: int = 100) -> Any:
        # GET /api/v1/readings/{sensor_id}?limit=<limit>
        raise NotImplementedError("TODO Frederic: implement get_measurements()")

    def get_metrics(self) -> Any:
        # GET /api/v1/metrics
        raise NotImplementedError("TODO Frederic: implement get_metrics()")
