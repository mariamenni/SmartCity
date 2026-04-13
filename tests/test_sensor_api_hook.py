"""Tests unitaires — SensorAPIHook (Personne 1 — Frédéric).

13 tests couvrant : health_check, get_sensors, get_readings,
get_metrics, get_metrics_summary, et gestion d'erreur.
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
        data = [{"id": 1, "type": "temperature", "status": "active", "latitude": 41.4, "longitude": 2.1}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_sensors()
            assert result == data

    def test_empty_list(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])):
            assert hook.get_sensors() == []

    def test_calls_correct_endpoint(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_sensors()
            endpoint = m.call_args[0][0]
            assert "/api/v1/sensors" in endpoint


# ---- get_readings ----

class TestGetReadings:
    def test_returns_list(self, hook):
        data = [{"sensor_id": 1, "value": 22.5, "unit": "celsius", "timestamp": "2025-01-01T00:00:00"}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_readings(1)
            assert result == data

    def test_empty_list(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])):
            assert hook.get_readings(99) == []

    def test_calls_correct_endpoint(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_readings(42)
            endpoint = m.call_args[0][0]
            assert "/api/v1/readings/42" in endpoint

    def test_string_sensor_id(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_readings("sensor-1")
            endpoint = m.call_args[0][0]
            assert "sensor-1" in endpoint


# ---- get_metrics ----

class TestGetMetrics:
    def test_returns_dict(self, hook):
        data = {"city": "Barcelona", "total_sensors": 3, "active_sensors": 2}
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_metrics()
            assert result == data


# ---- get_metrics_summary ----

class TestGetMetricsSummary:
    def test_returns_dict(self, hook):
        data = {"summary": {"temperature": {"total": 1, "active": 1}}, "total_sensors": 1}
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_metrics_summary()
            assert result == data


# ---- error propagation ----

class TestApiGetError:
    def test_run_raises_propagates(self, hook):
        with patch.object(hook, "run", side_effect=Exception("connection refused")):
            with pytest.raises(Exception, match="connection refused"):
                hook.get_sensors()

        data = [{"sensor_id": 1, "value": 20.0}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            assert hook.get_latest_measurements() == data


# ---- error ----

class TestApiGetError:
    def test_run_raises_propagates(self, hook):
        with patch.object(hook, "run", side_effect=Exception("connection refused")):
            with pytest.raises(Exception, match="connection refused"):
                hook.get_sensors()
