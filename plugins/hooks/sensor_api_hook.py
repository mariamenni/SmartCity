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

    # ---- helpers ----

    def _api_get(self, endpoint: str, params: dict | None = None) -> Any:
        response = self.run(endpoint, data=params or {}, headers={"Accept": "application/json"})
        response.raise_for_status()
        return response.json()

    # ---- public API ----

    def health_check(self) -> bool:
        try:
            data = self._api_get("/health")
            return data.get("status") == "ok"
        except Exception:
            return False

    def get_sensors(self) -> list[dict]:
        return self._api_get("/sensors")

    def get_locations(self) -> list[dict]:
        return self._api_get("/locations")

    def get_measurements(
        self,
        since: str | None = None,
        sensor_type: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if since:
            params["since"] = since
        if sensor_type:
            params["type"] = sensor_type
        if limit is not None:
            params["limit"] = str(limit)
        return self._api_get("/measurements", params=params)

    def get_latest_measurements(self) -> list[dict]:
        return self._api_get("/measurements/latest")
