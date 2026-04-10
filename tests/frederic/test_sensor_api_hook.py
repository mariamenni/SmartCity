"""Tests unitaires — SensorAPIHook (Personne 1 — Frédéric).

11 tests couvrant : health_check, get_sensors, get_sensor, get_readings,
get_metrics, get_metrics_summary, et gestion d'erreur.

Endpoints testés (smart-cities-api) :
  /health, /api/v1/sensors, /api/v1/sensors/{id}, /api/v1/readings/{sensor_id},
  /api/v1/metrics, /api/v1/metrics/summary
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from hooks.sensor_api_hook import SensorAPIHook


# ---- helpers ----

def _mock_response(data, status_code: int = 200):
    """Fabrique un objet Response minimal pour mocker HttpHook.run()."""
    resp = SimpleNamespace()
    resp.status_code = status_code
    resp.json = lambda: data
    resp.raise_for_status = lambda: None
    resp.text = json.dumps(data)
    return resp


@pytest.fixture()
def hook():
    return SensorAPIHook()


# ---- health_check ----

class TestHealthCheck:
    def test_healthy(self, hook):
        with patch.object(hook, "run", return_value=_mock_response({"status": "healthy"})):
            assert hook.health_check() is True

    def test_unhealthy(self, hook):
        with patch.object(hook, "run", return_value=_mock_response({"status": "degraded"})):
            assert hook.health_check() is False

    def test_exception_returns_false(self, hook):
        with patch.object(hook, "run", side_effect=Exception("timeout")):
            assert hook.health_check() is False


# ---- get_sensors ----

class TestGetSensors:
    def test_returns_list(self, hook):
        data = [{"id": 1, "name": "Temp Sensor", "type": "temperature", "status": "active"}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_sensors()
            assert result == data

    def test_with_filters(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_sensors(sensor_type="temperature", status="active", limit=10)
            m.assert_called_once()
            call_kwargs = m.call_args
            data = call_kwargs[1].get("data", call_kwargs[0][1] if len(call_kwargs[0]) > 1 else {})
            assert data.get("type") == "temperature"
            assert data.get("status") == "active"
            assert data.get("limit") == "10"


# ---- get_sensor ----

class TestGetSensor:
    def test_returns_sensor(self, hook):
        data = {"id": 1, "name": "Temp Sensor", "type": "temperature", "status": "active"}
        with patch.object(hook, "run", return_value=_mock_response(data)) as m:
            result = hook.get_sensor(1)
            assert result == data
            endpoint = m.call_args[0][0]
            assert "/api/v1/sensors/1" in endpoint


# ---- get_readings ----

class TestGetReadings:
    def test_returns_list(self, hook):
        data = [{"sensor_id": 1, "value": 23.5, "unit": "°C"}]
        with patch.object(hook, "run", return_value=_mock_response(data)) as m:
            result = hook.get_readings(1)
            assert result == data
            endpoint = m.call_args[0][0]
            assert "/api/v1/readings/1" in endpoint

    def test_with_limit(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_readings(2, limit=5)
            call_kwargs = m.call_args
            data = call_kwargs[1].get("data", call_kwargs[0][1] if len(call_kwargs[0]) > 1 else {})
            assert data.get("limit") == "5"


# ---- get_metrics ----

class TestGetMetrics:
    def test_returns_metrics(self, hook):
        data = {"city": "Barcelona", "total_sensors": 3, "active_sensors": 2}
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_metrics()
            assert result["city"] == "Barcelona"
            assert result["total_sensors"] == 3


# ---- get_metrics_summary ----

class TestGetMetricsSummary:
    def test_returns_summary(self, hook):
        data = {"summary": {"temperature": {"total": 1, "active": 1}}, "total_sensors": 3}
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_metrics_summary()
            assert "summary" in result


# ---- error ----

class TestApiGetError:
    def test_run_raises_propagates(self, hook):
        with patch.object(hook, "run", side_effect=Exception("connection refused")):
            with pytest.raises(Exception, match="connection refused"):
                hook.get_sensors()
