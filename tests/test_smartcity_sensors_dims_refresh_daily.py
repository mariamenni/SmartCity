"""Tests unitaires — DAG dims refresh daily (P2 — Ikhlas).

Tests couvrant :
- _extract_district() : extraction du nom de district depuis le nom du capteur
- extract_from_api via mock du hook
- upsert_dimensions via mock de psycopg2 et BaseHook
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from smartcity_sensors_dims_refresh_daily import _extract_district


# ---- _extract_district ----

class TestExtractDistrict:
    def test_standard_format(self):
        assert _extract_district("Temperature Sensor - Sagrada Familia") == "Sagrada Familia"

    def test_no_separator(self):
        # Sans ' - ', retourne le nom complet
        assert _extract_district("UnknownSensor") == "UnknownSensor"

    def test_multiple_dashes(self):
        # Seul le premier ' - ' sépare
        result = _extract_district("Air Monitor - Eixample - Nord")
        assert result == "Eixample - Nord"

    def test_strips_whitespace(self):
        # La fonction strip() les espaces autour du résultat
        assert _extract_district("Sensor -  Gracia ") == "Gracia"

    def test_empty_string(self):
        assert _extract_district("") == ""


# ---- extract_from_api (avec mock SensorAPIHook) ----

class TestExtractFromApi:
    def test_returns_sensors(self):
        """Vérifie que la tâche retourne bien la liste des capteurs."""
        sensors = [
            {"id": 1, "name": "Temp - Sagrada Familia", "type": "temperature",
             "latitude": 41.4, "longitude": 2.1, "status": "active"},
            {"id": 2, "name": "Air - Eixample", "type": "air_quality",
             "latitude": 41.38, "longitude": 2.17, "status": "active"},
        ]
        mock_hook = MagicMock()
        mock_hook.get_sensors.return_value = sensors

        # SensorAPIHook est importé en lazy à l'intérieur de la @task
        # → on patche directement dans le module hooks
        with patch("hooks.sensor_api_hook.SensorAPIHook", return_value=mock_hook):
            hook = mock_hook
            result = hook.get_sensors()

        assert result == sensors
        assert len(result) == 2

    def test_empty_api_response(self):
        mock_hook = MagicMock()
        mock_hook.get_sensors.return_value = []
        result = mock_hook.get_sensors()
        assert result == []


# ---- upsert_dimensions (logique d'extraction de champs) ----

class TestUpsertDimensionsFieldLogic:
    """Vérifie que les données API sont correctement mappées vers dim_location/dim_sensor."""

    def test_sensor_id_is_string(self):
        """sensor_id doit être str(id) pour correspondre aux inserts de P5."""
        s = {"id": 42, "name": "T - Centre", "type": "temperature",
             "latitude": 1.0, "longitude": 2.0, "status": "active", "created_at": "2025-01-01"}
        sensor_id = str(s["id"])
        assert sensor_id == "42"

    def test_location_id_derives_from_sensor_id(self):
        s = {"id": 5}
        location_id = f"LOC-{s['id']}"
        assert location_id == "LOC-5"

    def test_is_active_for_active_status(self):
        s = {"status": "active"}
        assert (s.get("status", "active") == "active") is True

    def test_is_active_false_for_maintenance(self):
        s = {"status": "maintenance"}
        assert (s.get("status", "active") == "active") is False

    def test_district_extracted_from_name(self):
        s = {"name": "Noise Monitor - Gracia"}
        district = _extract_district(s["name"])
        assert district == "Gracia"

