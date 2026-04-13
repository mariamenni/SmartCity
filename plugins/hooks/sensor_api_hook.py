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
        """Retourne True si l'API répond avec status='healthy'."""
        try:
            data = self._api_get("/health")
            return data.get("status") == "healthy"
        except Exception:
            return False

    def get_sensors(self) -> list[dict]:
        """GET /api/v1/sensors — liste de tous les capteurs."""
        return self._api_get("/api/v1/sensors")

    def get_readings(self, sensor_id: int | str) -> list[dict]:
        """GET /api/v1/readings/{sensor_id} — lectures pour un capteur donné."""
        return self._api_get(f"/api/v1/readings/{sensor_id}")

    def get_metrics(self) -> dict:
        """GET /api/v1/metrics — métriques agrégées de la ville."""
        return self._api_get("/api/v1/metrics")

    def get_metrics_summary(self) -> dict:
        """GET /api/v1/metrics/summary — résumé par type de capteur."""
        return self._api_get("/api/v1/metrics/summary")
