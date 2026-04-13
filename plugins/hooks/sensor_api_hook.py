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

    def _api_get(self, endpoint: str, params: dict | None = None) -> Any:
        response = self.run(
            endpoint=endpoint,
            headers={"Accept": "application/json"},
            data=None,
            extra_options={
                "timeout": 10,
                "params": params or {}
            },

        )
        response.raise_for_status()
        return response.json()

    def health_check(self) -> bool:
        try:
            data = self._api_get("health")
            print("DEBUG SENSOR RESPONSE:", data)
            return data.get("status") == "ok"
        except Exception:
            print("ERROR SENSOR HOOK:", e)
            return False

    def get_sensors(self, active: bool | None = None) -> list[dict]:
        params = {}
        if active is not None:
            params["active"] = str(active).lower()
        return self._api_get("/sensors", params=params)

    def get_sensor_by_id(self, sensor_id: str) -> dict:
        return self._api_get(f"/sensors/{sensor_id}")

    def get_locations(self) -> list[dict]:
        return self._api_get("/locations")

    def get_measurements(
        self,
        since: str | None = None,
        sensor_type: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        params: dict[str, str | int] = {}
        if since:
            params["since"] = since
        if sensor_type:
            params["type"] = sensor_type
        if limit is not None:
            params["limit"] = limit
        return self._api_get("/measurements", params=params)

    def get_latest_measurements(self) -> list[dict]:
        return self._api_get("/measurements/latest")