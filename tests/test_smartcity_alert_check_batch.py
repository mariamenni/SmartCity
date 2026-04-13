"""Tests unitaires — DAG alerting (P4 — Maria).

Tests couvrant :
- _check_violation() : logique de détection de dépassement de seuil
- THRESHOLDS : vérification de la cohérence des valeurs
- Logique de construction des violations (detect_thresholds)
"""
from __future__ import annotations

import pytest

from smartcity_alert_check_batch import THRESHOLDS, _check_violation


# ---- _check_violation ----

class TestCheckViolation:
    # --- température ---
    def test_temperature_normal(self):
        assert _check_violation("temperature", 25.0) is None

    def test_temperature_warning(self):
        result = _check_violation("temperature", 36.0)
        assert result is not None
        severity, threshold = result
        assert severity == "warning"
        assert threshold == 35.0

    def test_temperature_critical(self):
        result = _check_violation("temperature", 41.0)
        assert result is not None
        severity, threshold = result
        assert severity == "critical"
        assert threshold == 40.0

    def test_temperature_exactly_at_warning(self):
        result = _check_violation("temperature", 35.0)
        assert result is not None
        assert result[0] == "warning"

    def test_temperature_exactly_at_critical(self):
        result = _check_violation("temperature", 40.0)
        assert result is not None
        assert result[0] == "critical"

    # --- air_quality ---
    def test_air_quality_normal(self):
        assert _check_violation("air_quality", 1.5) is None

    def test_air_quality_warning(self):
        result = _check_violation("air_quality", 3.5)
        assert result is not None
        assert result[0] == "warning"

    def test_air_quality_critical(self):
        result = _check_violation("air_quality", 4.5)
        assert result is not None
        assert result[0] == "critical"

    # --- type inconnu ---
    def test_unknown_type_returns_none(self):
        assert _check_violation("unknown_sensor_type", 9999.0) is None

    def test_empty_type_returns_none(self):
        assert _check_violation("", 100.0) is None

    # --- autres types ---
    def test_traffic_flow_critical(self):
        result = _check_violation("traffic_flow", 96.0)
        assert result is not None
        assert result[0] == "critical"

    def test_noise_level_warning(self):
        result = _check_violation("noise_level", 72.0)
        assert result is not None
        assert result[0] == "warning"

    def test_humidity_normal(self):
        assert _check_violation("humidity", 60.0) is None


# ---- THRESHOLDS cohérence ----

class TestThresholds:
    def test_all_types_have_two_thresholds(self):
        for sensor_type, thresholds in THRESHOLDS.items():
            assert len(thresholds) == 2, f"{sensor_type} doit avoir (warning, critical)"

    def test_warning_below_critical(self):
        for sensor_type, (warn, crit) in THRESHOLDS.items():
            assert warn < crit, f"{sensor_type}: warning doit être < critical"

    def test_expected_types_present(self):
        expected = {"temperature", "air_quality", "traffic_flow", "humidity", "noise_level"}
        assert expected.issubset(THRESHOLDS.keys())


# ---- Logique detect_thresholds (simulation) ----

class TestDetectThresholdsLogic:
    def _run_detect(self, measurements: list[dict]) -> list[dict]:
        """Simule la logique de detect_thresholds."""
        violations = []
        for m in measurements:
            result = _check_violation(m.get("type", "unknown"), m["value"])
            if result is None:
                continue
            severity, threshold = result
            violations.append({
                "ts":        m["ts"],
                "sensor_id": m["sensor_id"],
                "severity":  severity,
                "value":     m["value"],
                "threshold": threshold,
            })
        return violations

    def test_no_violations(self):
        measurements = [
            {"ts": "2025-01-01T00:00:00", "sensor_id": "1", "type": "temperature", "value": 20.0, "unit": "celsius"},
        ]
        assert self._run_detect(measurements) == []

    def test_one_warning(self):
        measurements = [
            {"ts": "2025-01-01T00:00:00", "sensor_id": "1", "type": "temperature", "value": 37.0, "unit": "celsius"},
        ]
        violations = self._run_detect(measurements)
        assert len(violations) == 1
        assert violations[0]["severity"] == "warning"
        assert violations[0]["sensor_id"] == "1"

    def test_one_critical(self):
        measurements = [
            {"ts": "2025-01-01T00:00:00", "sensor_id": "2", "type": "air_quality", "value": 4.5, "unit": "index"},
        ]
        violations = self._run_detect(measurements)
        assert len(violations) == 1
        assert violations[0]["severity"] == "critical"

    def test_mixed_measurements(self):
        measurements = [
            {"ts": "T1", "sensor_id": "1", "type": "temperature", "value": 20.0, "unit": "c"},   # OK
            {"ts": "T2", "sensor_id": "2", "type": "temperature", "value": 38.0, "unit": "c"},   # warning
            {"ts": "T3", "sensor_id": "3", "type": "air_quality",  "value": 4.2, "unit": "idx"}, # critical
            {"ts": "T4", "sensor_id": "4", "type": "humidity",     "value": 50.0, "unit": "%"},  # OK
        ]
        violations = self._run_detect(measurements)
        assert len(violations) == 2
        severities = {v["severity"] for v in violations}
        assert "warning" in severities
        assert "critical" in severities

    def test_unknown_type_ignored(self):
        measurements = [
            {"ts": "T1", "sensor_id": "1", "type": "unknown_type", "value": 9999.0, "unit": "x"},
        ]
        assert self._run_detect(measurements) == []

