from __future__ import annotations

from datetime import datetime
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

    # ---- public API  (smart-cities-api /api/v1) ----

    def health_check(self) -> bool:
        try:
            data = self._api_get("/health")
            return data.get("status") == "healthy"
        except Exception:
            return False

    def get_sensors(
        self,
        sensor_type: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        params: dict[str, str] = {}
        if sensor_type:
            params["type"] = sensor_type
        if status:
            params["status"] = status
        if limit is not None:
            params["limit"] = str(limit)
        return self._api_get("/api/v1/sensors", params=params)

    def get_sensor(self, sensor_id: int) -> dict:
        return self._api_get(f"/api/v1/sensors/{sensor_id}")

    def get_readings(self, sensor_id: int, limit: int | None = None) -> list[dict]:
        params: dict[str, str] = {}
        if limit is not None:
            params["limit"] = str(limit)
        return self._api_get(f"/api/v1/readings/{sensor_id}", params=params)

    def get_measurements(
        self,
        since: datetime | None = None,
        sensor_type: str | None = None,
    ) -> list[dict]:
        """Récupère les mesures de tous les capteurs.

        Combine get_sensors() + get_readings() pour fournir l'interface
        exigée par le cahier des charges §9.3.
        """
        sensors = self.get_sensors(sensor_type=sensor_type)
        measurements: list[dict] = []
        for sensor in sensors:
            sid = sensor.get("id")
            if sid is None:
                continue
            readings = self.get_readings(sid)
            for r in readings:
                if since is not None:
                    ts_str = r.get("timestamp") or r.get("ts")
                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            )
                            since_aware = since if since.tzinfo else since.replace(
                                tzinfo=ts.tzinfo
                            )
                            if ts < since_aware:
                                continue
                        except (ValueError, TypeError):
                            pass
                measurements.append(r)
        return measurements

    def get_metrics(self) -> dict:
        return self._api_get("/api/v1/metrics")

    def get_metrics_summary(self) -> dict:
        return self._api_get("/api/v1/metrics/summary")
