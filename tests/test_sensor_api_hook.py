"""Tests unitaires — SensorAPIHook (Personne 1 — Frédéric).

13 tests couvrant : health_check, get_sensors, get_locations,
get_measurements, get_latest_measurements, et gestion d'erreur.
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
        data = [{"sensor_id": 1, "type": "temperature", "is_active": True}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_sensors()
            assert result == data

    def test_empty_list(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])):
            assert hook.get_sensors() == []


# ---- get_locations ----

class TestGetLocations:
    def test_returns_list(self, hook):
        data = [{"location_id": 1, "district": "Centre", "zone_type": "residential"}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            result = hook.get_locations()
            assert result == data

    def test_empty_list(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])):
            assert hook.get_locations() == []


# ---- get_measurements ----

class TestGetMeasurements:
    def test_no_params(self, hook):
        data = [{"sensor_id": 1, "type": "temperature", "value": 22.5, "unit": "°C"}]
        with patch.object(hook, "run", return_value=_mock_response(data)) as m:
            result = hook.get_measurements()
            assert result == data

    def test_with_start_ts(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_measurements(start_ts="2025-01-01T00:00:00")
            m.assert_called_once()
            call_kwargs = m.call_args
            data = call_kwargs[1].get("data", call_kwargs[0][1] if len(call_kwargs[0]) > 1 else {})
            assert "start" in data

    def test_with_both_params(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])) as m:
            hook.get_measurements(start_ts="2025-01-01", end_ts="2025-01-02")
            m.assert_called_once()

    def test_empty_result(self, hook):
        with patch.object(hook, "run", return_value=_mock_response([])):
            assert hook.get_measurements() == []


# ---- get_latest_measurements ----

class TestGetLatestMeasurements:
    def test_returns_list(self, hook):
        data = [{"sensor_id": 1, "value": 20.0}]
        with patch.object(hook, "run", return_value=_mock_response(data)):
            assert hook.get_latest_measurements() == data


# ---- error ----

class TestApiGetError:
    def test_run_raises_propagates(self, hook):
        with patch.object(hook, "run", side_effect=Exception("connection refused")):
            with pytest.raises(Exception, match="connection refused"):
                hook.get_sensors()
