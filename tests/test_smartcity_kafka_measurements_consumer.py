"""Tests unitaires — DAG consommateur Kafka (J2 — Gills).

8 tests couvrant les deux fonctions publiees :
  _get_bootstrap_servers   -> lecture env ou valeur par defaut
  _consume_and_flush_logic -> consommation Kafka, validation,
                              deduplication, INSERT psycopg2

Les dependances externes (confluent_kafka, psycopg2, BaseHook) sont toutes
mockees : les tests s'executent sans Airflow, sans Kafka ni base de donnees.
"""
from __future__ import annotations

import json
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

# -----------------------------------------------------------------------------
# Stubs pour les dependances absentes en local
# -----------------------------------------------------------------------------
if "confluent_kafka" not in sys.modules:
    mock_confluent = MagicMock()
    sys.modules["confluent_kafka"] = mock_confluent

if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = MagicMock()

# -----------------------------------------------------------------------------
# Import des fonctions testables (apres injection des stubs)
# -----------------------------------------------------------------------------
from smartcity_kafka_measurements_consumer import (  # noqa: E402
    CONSUMER_GROUP,
    KAFKA_TOPIC,
    MAX_RECORDS,
    _consume_and_flush_logic,
    _get_bootstrap_servers,
)


# =============================================================================
# Helpers
# =============================================================================

def _kafka_message(data: dict, error=None):
    """Fabrique un faux message Kafka conforme a l'interface confluent_kafka."""
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = json.dumps(data).encode("utf-8")
    return msg


def _empty_poll():
    """Simule un poll() qui ne retourne aucun message (timeout)."""
    return None


def _make_consumer_side_effects(*messages, empty_tail: int = 6):
    """Retourne une liste d'effets de bord pour consumer.poll().

    Apres les messages fournis, on ajoute ``empty_tail`` polls vides pour
    simuler la fin du topic et declencher la sortie de la boucle.
    """
    effects = list(messages) + [None] * empty_tail
    return effects


# =============================================================================
# _get_bootstrap_servers
# =============================================================================

class TestGetBootstrapServers:

    def test_returns_env_variable_when_set(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "mybroker:9094")
        assert _get_bootstrap_servers() == "mybroker:9094"

    def test_returns_default_when_env_absent(self, monkeypatch):
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        assert _get_bootstrap_servers() == "kafka:9092"


# =============================================================================
# _consume_and_flush_logic
# =============================================================================

class TestConsumeAndFlushLogic:

    def _base_patches(self, consumer_mock, conn_mock=None):
        """Retourne le contexte de patch minimal pour tous les tests."""
        from airflow.hooks.base import BaseHook

        patches = [
            patch(
                "confluent_kafka.Consumer",
                return_value=consumer_mock,
            ),
            patch(
                "airflow.hooks.base.BaseHook.get_connection",
                return_value=SimpleNamespace(
                    host="localhost",
                    port=5432,
                    schema="smartcity",
                    login="airflow",
                    password="airflow",
                ),
            ),
        ]
        if conn_mock is not None:
            patches.append(
                patch("psycopg2.connect", return_value=conn_mock)
            )
        return patches

    def test_returns_0_when_topic_empty(self):
        consumer_mock = MagicMock()
        consumer_mock.poll.return_value = None  # topic vide, MAX_EMPTY_POLLS fois

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ):
            result = _consume_and_flush_logic("2025-01-01T00-00-00")

        assert result == 0
        consumer_mock.commit.assert_not_called()

    def test_inserts_valid_messages_and_returns_count(self):
        msgs = [
            _kafka_message(
                {"ts": "2025-01-01T00:00:00Z", "sensor_id": "S-01", "value": 22.5, "unit": "C"}
            ),
            _kafka_message(
                {"ts": "2025-01-01T00:01:00Z", "sensor_id": "S-01", "value": 23.0, "unit": "C"}
            ),
        ]
        consumer_mock = MagicMock()
        consumer_mock.poll.side_effect = _make_consumer_side_effects(*msgs)

        cur_mock = MagicMock()
        conn_mock = MagicMock()
        conn_mock.__enter__ = lambda s: s
        conn_mock.__exit__ = MagicMock(return_value=False)
        conn_mock.cursor.return_value.__enter__ = lambda s: cur_mock
        conn_mock.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ), patch("airflow.hooks.base.BaseHook") as MockBaseHookClass, patch(
            "psycopg2.connect", return_value=conn_mock
        ):
            MockBaseHookClass.get_connection.return_value = SimpleNamespace(
                host="localhost", port=5432,
                schema="smartcity", login="airflow", password="airflow",
            )
            result = _consume_and_flush_logic("2025-01-01T00-00-00")

        assert result == 2
        consumer_mock.commit.assert_called_once_with(asynchronous=False)

    def test_skips_malformed_json(self):
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.value.return_value = b"NOT-JSON{{{}"

        consumer_mock = MagicMock()
        consumer_mock.poll.side_effect = _make_consumer_side_effects(bad_msg)

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ):
            result = _consume_and_flush_logic("2025-01-01T00-00-00")

        assert result == 0

    def test_deduplication_same_ts_sensor_id(self):
        same_payload = {"ts": "2025-01-01T00:00:00Z", "sensor_id": "S-DUP", "value": 5.0, "unit": "bar"}
        msgs = [
            _kafka_message(same_payload),
            _kafka_message(same_payload),  # doublon exact
        ]
        consumer_mock = MagicMock()
        consumer_mock.poll.side_effect = _make_consumer_side_effects(*msgs)

        cur_mock = MagicMock()
        conn_mock = MagicMock()
        conn_mock.__enter__ = lambda s: s
        conn_mock.__exit__ = MagicMock(return_value=False)
        conn_mock.cursor.return_value.__enter__ = lambda s: cur_mock
        conn_mock.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ), patch("airflow.hooks.base.BaseHook") as MockBaseHookClass, patch(
            "psycopg2.connect", return_value=conn_mock
        ):
            MockBaseHookClass.get_connection.return_value = SimpleNamespace(
                host="localhost", port=5432,
                schema="smartcity", login="airflow", password="airflow",
            )
            result = _consume_and_flush_logic("2025-01-01T00-00-00")

        # Apres deduplication en memoire, un seul enregistrement doit etre insere
        assert result == 1

    def test_skips_message_with_missing_fields(self):
        incomplete = {"ts": "2025-01-01T00:00:00Z", "sensor_id": "S-INC"}
        # 'value' et 'unit' manquants
        consumer_mock = MagicMock()
        consumer_mock.poll.side_effect = _make_consumer_side_effects(
            _kafka_message(incomplete)
        )

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ):
            result = _consume_and_flush_logic("2025-01-01T00-00-00")

        assert result == 0
        consumer_mock.commit.assert_not_called()

    def test_consumer_closed_even_on_exception(self):
        consumer_mock = MagicMock()
        consumer_mock.poll.side_effect = RuntimeError("Kafka down")

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ):
            with pytest.raises(RuntimeError):
                _consume_and_flush_logic("2025-01-01T00-00-00")

        consumer_mock.close.assert_called_once()

    def test_consumer_subscribes_to_correct_topic(self):
        consumer_mock = MagicMock()
        consumer_mock.poll.return_value = None  # topic vide, sort proprement

        with patch(
            "confluent_kafka.Consumer",
            return_value=consumer_mock,
        ):
            _consume_and_flush_logic("2025-01-01T00-00-00")

        consumer_mock.subscribe.assert_called_once_with([KAFKA_TOPIC])
