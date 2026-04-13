"""Tests unitaires — DAG producteur Kafka (J2 — Gills).

7 tests couvrant les deux fonctions publiees :
  _get_bootstrap_servers  -> lecture env ou valeur par defaut
  _produce_readings_logic -> poll API, serialisation JSON, produce Kafka, count

Les dependances externes (confluent_kafka, SensorAPIHook) sont toutes mockees :
les tests s'executent sans Airflow, sans Kafka ni API simulateur en ligne.
"""
from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# -----------------------------------------------------------------------------
# Stubs pour les dependances absentes en local
# -----------------------------------------------------------------------------
if "confluent_kafka" not in sys.modules:
    sys.modules["confluent_kafka"] = MagicMock()

# -----------------------------------------------------------------------------
# Import des fonctions testables (apres injection des stubs)
# -----------------------------------------------------------------------------
from smartcity_kafka_measurements_producer import (  # noqa: E402
    KAFKA_TOPIC,
    PRODUCER_FLUSH_TIMEOUT_S,
    _get_bootstrap_servers,
    _produce_readings_logic,
)


# =============================================================================
# _get_bootstrap_servers
# =============================================================================

class TestGetBootstrapServers:

    def test_returns_env_variable_when_set(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "mybroker:9093")
        assert _get_bootstrap_servers() == "mybroker:9093"

    def test_returns_default_when_env_absent(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert _get_bootstrap_servers() == "kafka:9092"


# =============================================================================
# _produce_readings_logic
# =============================================================================

class TestProduceReadingsLogic:

    def _make_producer_mock(self):
        producer = MagicMock()
        producer.flush.return_value = 0  # 0 messages en attente
        return producer

    def test_returns_0_when_no_active_sensors(self):
        sensors = [{"sensor_id": "S-001", "is_active": False}]

        with patch(
            "confluent_kafka.Producer"
        ) as MockProducer, patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            MockHook.return_value.get_sensors.return_value = sensors
            result = _produce_readings_logic("2025-01-01T00-00-00")

        assert result == 0
        MockProducer.return_value.produce.assert_not_called()

    def test_returns_count_for_single_sensor(self):
        sensors = [{"sensor_id": "S-001", "is_active": True}]
        readings = [
            {"ts": "2025-01-01T00:00:00Z", "value": 22.5, "unit": "C"},
            {"ts": "2025-01-01T00:01:00Z", "value": 23.0, "unit": "C"},
        ]
        producer_mock = self._make_producer_mock()

        with patch(
            "confluent_kafka.Producer",
            return_value=producer_mock,
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.return_value = readings

            result = _produce_readings_logic("2025-01-01T00-00-00")

        assert result == 2
        assert producer_mock.produce.call_count == 2

    def test_message_contains_sensor_id_and_run_ts(self):
        sensors = [{"sensor_id": "S-042", "is_active": True}]
        readings = [{"ts": "2025-01-01T00:00:00Z", "value": 1.0, "unit": "ppm"}]
        producer_mock = self._make_producer_mock()

        with patch(
            "confluent_kafka.Producer",
            return_value=producer_mock,
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.return_value = readings

            _produce_readings_logic("2025-run-ts")

        call_kwargs = producer_mock.produce.call_args[1]
        assert call_kwargs["topic"] == KAFKA_TOPIC
        assert call_kwargs["key"] == b"S-042"
        payload = json.loads(call_kwargs["value"].decode("utf-8"))
        assert payload["sensor_id"] == "S-042"
        assert payload["run_ts"] == "2025-run-ts"

    def test_skips_sensor_on_api_error_continues(self):
        sensors = [
            {"sensor_id": "S-BAD", "is_active": True},
            {"sensor_id": "S-OK",  "is_active": True},
        ]
        readings_ok = [{"ts": "2025-01-01T00:00:00Z", "value": 5.0, "unit": "bar"}]
        producer_mock = self._make_producer_mock()

        with patch(
            "confluent_kafka.Producer",
            return_value=producer_mock,
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.side_effect = [
                RuntimeError("timeout"),
                readings_ok,
            ]

            result = _produce_readings_logic("2025-01-01T00-00-00")

        # Seul S-OK a produit un message malgre l'erreur sur S-BAD
        assert result == 1

    def test_calls_flush_after_produce(self):
        sensors = [{"sensor_id": "S-001", "is_active": True}]
        readings = [{"ts": "2025-01-01T00:00:00Z", "value": 9.0, "unit": "V"}]
        producer_mock = self._make_producer_mock()

        with patch(
            "confluent_kafka.Producer",
            return_value=producer_mock,
        ), patch(
            "hooks.sensor_api_hook.SensorAPIHook"
        ) as MockHook:
            inst = MockHook.return_value
            inst.get_sensors.return_value = sensors
            inst.get_readings.return_value = readings

            _produce_readings_logic("2025-01-01T00-00-00")

        producer_mock.flush.assert_called_once_with(timeout=PRODUCER_FLUSH_TIMEOUT_S)
