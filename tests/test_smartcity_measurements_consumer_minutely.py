"""Tests unitaires — DAG consumer micro-batch minutely (P5 — Gills).

12 tests couvrant les trois fonctions métier :
  _poll_api_logic  → collecte API + stockage MinIO brut
  _transform_logic → validation, déduplication, contrôle latence
  _flush_logic     → insertion TimescaleDB (ON CONFLICT DO NOTHING)

Les dépendances externes (SensorAPIHook, boto3, psycopg2, BaseHook) sont
toutes mockées : les tests s'exécutent sans Airflow, MinIO ni base de données.
"""
from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Stubs pour les dépendances non-Airflow absentes en local
# ─────────────────────────────────────────────────────────────────────────────
if "boto3" not in sys.modules:
    sys.modules["boto3"] = MagicMock()
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()

# ─────────────────────────────────────────────────────────────────────────────
# Import des fonctions testables (après injection des stubs)
# ─────────────────────────────────────────────────────────────────────────────
from smartcity_measurements_consumer_minutely import (  # noqa: E402
    MINIO_BUCKET,
    _flush_logic,
    _poll_api_logic,
    _transform_logic,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s3_body(data: list | dict) -> SimpleNamespace:
    """Fabrique un objet Body MinIO minimal."""
    body = SimpleNamespace()
    body.read = lambda: json.dumps(data).encode("utf-8")
    return body


def _make_s3_mock(stored: list | dict | None = None):
    """Retourne un mock boto3 S3 client avec put_object et get_object préconfigurés."""
    s3 = MagicMock()
    s3.head_bucket.return_value = {}
    if stored is not None:
        s3.get_object.return_value = {"Body": _s3_body(stored)}
    return s3


# ─────────────────────────────────────────────────────────────────────────────
# _poll_api_logic
# ─────────────────────────────────────────────────────────────────────────────

class TestPollApiLogic:

    def test_stores_raw_json_in_minio_and_returns_key(self):
        sensors = [
            {"sensor_id": "S-001", "is_active": True},
            {"sensor_id": "S-002", "is_active": True},
        ]
        readings_s001 = [{"ts": "2025-01-01T00:00:00Z", "value": 22.5, "unit": "°C"}]
        readings_s002 = [{"ts": "2025-01-01T00:00:00Z", "value": 45.0, "unit": "%"}]

        s3 = _make_s3_mock()

        with patch(
            "smartcity_measurements_consumer_minutely._s3_client", return_value=s3
        ), patch(
            "smartcity_measurements_consumer_minutely._ensure_bucket"
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.side_effect = [readings_s001, readings_s002]

            key = _poll_api_logic("2025-01-01T00-00-00")

        assert key == "raw/2025-01-01T00-00-00.json"
        s3.put_object.assert_called_once()
        call_kwargs = s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == MINIO_BUCKET
        assert call_kwargs["Key"] == "raw/2025-01-01T00-00-00.json"
        body = json.loads(call_kwargs["Body"].decode("utf-8"))
        assert len(body) == 2

    def test_skips_inactive_sensors(self):
        sensors = [
            {"sensor_id": "S-001", "is_active": True},
            {"sensor_id": "S-002", "is_active": False},
        ]
        readings = [{"ts": "2025-01-01T00:00:00Z", "value": 10.0, "unit": "ppm"}]

        s3 = _make_s3_mock()

        with patch(
            "smartcity_measurements_consumer_minutely._s3_client", return_value=s3
        ), patch(
            "smartcity_measurements_consumer_minutely._ensure_bucket"
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.return_value = readings

            _poll_api_logic("2025-01-01T00-00-00")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1  # seulement S-001

    def test_continues_on_individual_sensor_error(self):
        sensors = [
            {"sensor_id": "S-001", "is_active": True},
            {"sensor_id": "S-002", "is_active": True},
        ]
        readings_ok = [{"ts": "2025-01-01T00:00:00Z", "value": 10.0, "unit": "ppm"}]

        s3 = _make_s3_mock()

        with patch(
            "smartcity_measurements_consumer_minutely._s3_client", return_value=s3
        ), patch(
            "smartcity_measurements_consumer_minutely._ensure_bucket"
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.side_effect = [Exception("timeout"), readings_ok]

            key = _poll_api_logic("2025-01-01T00-00-00")

        # Pas d'exception levée + les lectures valides sont stockées
        assert key == "raw/2025-01-01T00-00-00.json"
        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1


# ─────────────────────────────────────────────────────────────────────────────
# _transform_logic
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformLogic:

    def test_valid_record_passes_through(self):
        raw = [{"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 22.5, "unit": "°C"}]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            key = _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1
        assert body[0]["value"] == 22.5
        assert key == "clean/run.json"

    def test_filters_missing_fields(self):
        raw = [
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 22.5, "unit": "°C"},
            {"sensor_id": "S-002", "value": 10.0, "unit": "ppm"},          # ts manquant
            {"ts": "2025-01-01T00:00:00+00:00", "value": 10.0, "unit": "ppm"},  # sensor_id manquant
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-003"},     # value et unit manquants
        ]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1

    def test_filters_non_numeric_value(self):
        raw = [{"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": "N/A", "unit": "°C"}]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 0

    def test_deduplicates_same_ts_sensor(self):
        raw = [
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 22.5, "unit": "°C"},
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 23.0, "unit": "°C"},
        ]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1
        assert body[0]["value"] == 22.5  # premier gardé

    def test_warns_on_high_latency(self, capsys):
        # Timestamp volontairement vieux (2020 → latence >> 300s)
        raw = [{"ts": "2020-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 5.0, "unit": "µg/m³"}]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        captured = capsys.readouterr()
        assert "[WARN]" in captured.out

    def test_keeps_record_with_unparseable_ts(self):
        raw = [{"ts": "not-a-date", "sensor_id": "S-001", "value": 5.0, "unit": "µg/m³"}]
        s3 = _make_s3_mock(stored=raw)

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert len(body) == 1  # conservé malgré le ts non parseable

    def test_empty_raw_produces_empty_clean(self):
        s3 = _make_s3_mock(stored=[])

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            _transform_logic("raw/run.json", "run")

        body = json.loads(s3.put_object.call_args[1]["Body"].decode("utf-8"))
        assert body == []


# ─────────────────────────────────────────────────────────────────────────────
# _flush_logic
# ─────────────────────────────────────────────────────────────────────────────

class TestFlushLogic:

    def _make_db_mocks(self):
        """Retourne (mock_conn_info, mock_psycopg2_conn, mock_cursor) préconfigurés."""
        conn_info = SimpleNamespace(
            host="timescaledb", port=5432,
            schema="smartcity", login="user", password="pw",
        )
        pg_conn = MagicMock()
        pg_cursor = MagicMock()
        pg_conn.__enter__ = lambda s: s
        pg_conn.__exit__ = MagicMock(return_value=False)
        pg_conn.cursor.return_value.__enter__ = lambda s: pg_cursor
        pg_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return conn_info, pg_conn, pg_cursor

    def _airflow_hooks_mock(self, conn_info):
        """Crée un module airflow.hooks.base avec BaseHook.get_connection configuré."""
        mock_base = MagicMock()
        mock_base.BaseHook.get_connection.return_value = conn_info
        return mock_base

    def test_inserts_records_and_returns_count(self):
        records = [
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 22.5, "unit": "°C"},
            {"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-002", "value": 45.0, "unit": "%"},
        ]
        s3 = _make_s3_mock(stored=records)
        conn_info, pg_conn, pg_cursor = self._make_db_mocks()

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3), \
             patch.dict("sys.modules", {"airflow.hooks.base": self._airflow_hooks_mock(conn_info)}), \
             patch("psycopg2.connect", return_value=pg_conn):
            count = _flush_logic("clean/run.json")

        assert count == 2
        pg_cursor.executemany.assert_called_once()
        sql, params = pg_cursor.executemany.call_args[0]
        assert "INSERT INTO fact_measurement" in sql
        assert "ON CONFLICT" in sql
        assert len(params) == 2

    def test_returns_zero_on_empty_records(self):
        s3 = _make_s3_mock(stored=[])

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3):
            count = _flush_logic("clean/run.json")

        assert count == 0

    def test_closes_connection_on_success(self):
        records = [{"ts": "2025-01-01T00:00:00+00:00", "sensor_id": "S-001", "value": 1.0, "unit": "x"}]
        s3 = _make_s3_mock(stored=records)
        conn_info, pg_conn, _ = self._make_db_mocks()

        with patch("smartcity_measurements_consumer_minutely._s3_client", return_value=s3), \
             patch.dict("sys.modules", {"airflow.hooks.base": self._airflow_hooks_mock(conn_info)}), \
             patch("psycopg2.connect", return_value=pg_conn):
            _flush_logic("clean/run.json")

        pg_conn.close.assert_called_once()

