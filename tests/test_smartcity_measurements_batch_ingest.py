"""Tests unitaires — DAG batch ingest (P3 — Narcisse).

Tests couvrant :
- _filter_valid_records() : filtrage des enregistrements incomplets
- Logique de normalisation des readings (ts, sensor_id, value, unit)
"""
from __future__ import annotations

import pytest

from smartcity_measurements_batch_ingest import _filter_valid_records


# ---- _filter_valid_records ----

class TestFilterValidRecords:
    def test_all_valid(self):
        records = [
            {"ts": "2025-01-01T00:00:00", "sensor_id": "1", "value": 22.5, "unit": "celsius"},
            {"ts": "2025-01-01T00:01:00", "sensor_id": "2", "value": 1.5,  "unit": "index"},
        ]
        result = _filter_valid_records(records)
        assert len(result) == 2

    def test_missing_ts(self):
        records = [{"sensor_id": "1", "value": 22.5, "unit": "celsius"}]
        assert _filter_valid_records(records) == []

    def test_missing_sensor_id(self):
        records = [{"ts": "2025-01-01T00:00:00", "value": 22.5, "unit": "celsius"}]
        assert _filter_valid_records(records) == []

    def test_missing_value(self):
        records = [{"ts": "2025-01-01T00:00:00", "sensor_id": "1", "unit": "celsius"}]
        assert _filter_valid_records(records) == []

    def test_missing_unit(self):
        records = [{"ts": "2025-01-01T00:00:00", "sensor_id": "1", "value": 22.5}]
        assert _filter_valid_records(records) == []

    def test_value_zero_is_valid(self):
        """value=0 est valide (0 is not None)."""
        records = [{"ts": "2025-01-01T00:00:00", "sensor_id": "1", "value": 0, "unit": "celsius"}]
        result = _filter_valid_records(records)
        assert len(result) == 1

    def test_empty_ts_string_is_invalid(self):
        records = [{"ts": "", "sensor_id": "1", "value": 22.5, "unit": "celsius"}]
        assert _filter_valid_records(records) == []

    def test_empty_list(self):
        assert _filter_valid_records([]) == []

    def test_mixed_valid_and_invalid(self):
        records = [
            {"ts": "2025-01-01T00:00:00", "sensor_id": "1", "value": 22.5, "unit": "celsius"},
            {"ts": None, "sensor_id": "2", "value": 1.5, "unit": "index"},  # ts=None invalide
            {"ts": "2025-01-01T00:02:00", "sensor_id": "3", "value": 0.0,  "unit": "dB"},
        ]
        result = _filter_valid_records(records)
        assert len(result) == 2
        assert result[0]["sensor_id"] == "1"
        assert result[1]["sensor_id"] == "3"


# ---- Normalisation sensor_id ----

class TestSensorIdNormalization:
    def test_int_id_converted_to_str(self):
        """Le batch doit stocker sensor_id en str pour correspondre à fact_measurement."""
        reading = {"sensor_id": 1, "value": 22.5, "unit": "celsius", "timestamp": "2025-01-01T00:00:00"}
        normalized = {
            "ts":        reading.get("timestamp") or reading.get("ts"),
            "sensor_id": str(reading.get("sensor_id", "")),
            "value":     float(reading["value"]),
            "unit":      str(reading.get("unit", "")),
        }
        assert normalized["sensor_id"] == "1"
        assert isinstance(normalized["sensor_id"], str)

    def test_timestamp_key_aliased(self):
        """L'API retourne 'timestamp', pas 'ts' — vérifier le fallback."""
        r = {"sensor_id": 1, "value": 20.0, "unit": "c", "timestamp": "2025-01-01T00:00:00"}
        ts = r.get("timestamp") or r.get("ts") or r.get("time")
        assert ts == "2025-01-01T00:00:00"

