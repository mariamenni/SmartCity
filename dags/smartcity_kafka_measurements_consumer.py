# =============================================================================
# DAG  — Consommateur Kafka des mesures capteurs (J2 — Gills)
#
# Ce DAG consomme les messages du topic Kafka "sensor-measurements" et les
# insere dans la table fact_measurement de TimescaleDB.
#
# Flux :
#   consume_and_flush : Consumer Kafka -> validation -> INSERT fact_measurement
#                       ON CONFLICT (ts, sensor_id) DO NOTHING (idempotence)
#                       -> XCom : nombre d'enregistrements inseres (int)
#
# Garanties :
#   - Idempotence : ON CONFLICT DO NOTHING, cle (ts, sensor_id)
#   - Deduplication en memoire sur la fenetre de consommation courante
#   - auto.commit desactive : commit des offsets uniquement apres flush reussi
#   - group.id fixe : permet la reprise depuis le dernier offset commite
#
# Prerequis :
#   - Stack kafka active :
#       docker compose -f docker-compose.yaml \
#                      -f docker-compose.override.yaml \
#                      -f docker-compose.kafka.yaml up -d
#   - confluent-kafka==2.14.0 installe dans l'image Airflow
#   - DAG smartcity_kafka_measurements_producer actif (producteur de messages)
#
# Connexions Airflow requises :
#   smartcity_timescaledb  (AIRFLOW_CONN_SMARTCITY_TIMESCALEDB)
#
# Topic Kafka consomme :
#   sensor-measurements
#
# Schedule : */1 * * * *  (une fois par minute, en complement du producteur)
# =============================================================================
from __future__ import annotations

import json
import os
from datetime import datetime

from airflow.sdk import dag, task

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_TOPIC = "sensor-measurements"
CONSUMER_GROUP = "smartcity-airflow-consumer"
MAX_RECORDS = 500           # limite par run pour ne pas bloquer la task
POLL_TIMEOUT_S = 1.0        # duree max d'un poll() en secondes
MAX_EMPTY_POLLS = 5         # arret apres N polls consecutifs vides


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (testables sans Airflow)
# ─────────────────────────────────────────────────────────────────────────────

def _get_bootstrap_servers() -> str:
    """Retourne l'adresse du broker Kafka depuis l'env (jamais en dur)."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _consume_and_flush_logic(run_ts: str) -> int:
    """Consomme les messages Kafka et les insere dans fact_measurement.

    Le consommateur s'arrete des que l'une des conditions suivantes est atteinte :
      - MAX_RECORDS messages valides collectes
      - MAX_EMPTY_POLLS polls consecutifs vides (topic courant a jour)
    Les offsets sont commites APRES l'insertion reussie en base (at-least-once).

    Args:
        run_ts: horodatage de l'execution (utilise pour les logs).

    Returns:
        Nombre d'enregistrements soumis a l'insertion (avant deduplication SQL).
    """
    from confluent_kafka import Consumer, KafkaError
    import psycopg2
    from airflow.hooks.base import BaseHook

    consumer = Consumer({
        "bootstrap.servers": _get_bootstrap_servers(),
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "client.id": "smartcity-airflow-consumer",
        "session.timeout.ms": 45000,
    })
    consumer.subscribe([KAFKA_TOPIC])

    records: list[dict] = []
    seen: set[tuple[str, str]] = set()
    empty_polls = 0

    try:
        while len(records) < MAX_RECORDS and empty_polls < MAX_EMPTY_POLLS:
            msg = consumer.poll(POLL_TIMEOUT_S)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_polls += 1
                    continue
                raise RuntimeError(f"Erreur Kafka: {msg.error()}")

            empty_polls = 0
            try:
                data = json.loads(msg.value().decode("utf-8"))
                ts        = data.get("ts") or data.get("timestamp") or data.get("time")
                sensor_id = data.get("sensor_id")
                value     = data.get("value")
                unit      = data.get("unit")

                if not (ts and sensor_id and value is not None and unit):
                    continue

                # Deduplication en memoire (ts, sensor_id)
                dedup_key = (str(ts), str(sensor_id))
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                records.append({
                    "ts":        str(ts),
                    "sensor_id": str(sensor_id),
                    "value":     float(value),
                    "unit":      str(unit),
                })
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                print(f"[WARN] Message malformed ignore: {exc}")

        if not records:
            print(f"consume [{run_ts}]: aucun message a inserer (topic '{KAFKA_TOPIC}')")
            return 0

        # Insertion en base AVANT commit des offsets (at-least-once)
        conn_info = BaseHook.get_connection("smartcity_timescaledb")
        conn = psycopg2.connect(
            host=conn_info.host,
            port=int(conn_info.port or 5432),
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO fact_measurement (ts, sensor_id, value, unit)
                        VALUES (%(ts)s, %(sensor_id)s, %(value)s, %(unit)s)
                        ON CONFLICT (ts, sensor_id) DO NOTHING
                        """,
                        records,
                    )
        finally:
            conn.close()

        # Commit des offsets seulement apres insertion reussie
        consumer.commit(asynchronous=False)
        print(
            f"consume [{run_ts}]: {len(records)} enregistrements inseres "
            f"dans fact_measurement depuis '{KAFKA_TOPIC}'"
        )
        return len(records)

    finally:
        consumer.close()


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="smartcity_kafka_measurements_consumer",
    description="J2 — Consommateur Kafka : topic sensor-measurements -> fact_measurement",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "kafka", "consumer", "j2"],
)
def smartcity_kafka_measurements_consumer():

    @task()
    def consume_and_flush() -> int:
        """Consomme les messages Kafka et les insere dans fact_measurement."""
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        run_ts = ctx["logical_date"].strftime("%Y-%m-%dT%H-%M-%S")
        return _consume_and_flush_logic(run_ts)

    consume_and_flush()


smartcity_kafka_measurements_consumer()
