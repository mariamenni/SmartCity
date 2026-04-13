# =============================================================================
# DAG  — Producteur Kafka des mesures capteurs (J2 — Gills)
#
# Ce DAG lit les mesures depuis l'API du simulateur et les publie dans le
# topic Kafka "sensor-measurements" pour que le DAG consommateur puisse
# les traiter de maniere decouple.
#
# Flux :
#   poll_and_produce  : GET /api/v1/sensors + GET /api/v1/readings/{id}
#                       pour chaque capteur actif, puis produce() vers Kafka
#                       -> XCom : nombre de messages produits (int)
#
# Prerequis :
#   - Stack kafka active :
#       docker compose -f docker-compose.yaml \
#                      -f docker-compose.override.yaml \
#                      -f docker-compose.kafka.yaml up -d
#   - confluent-kafka==2.14.0 installe dans l'image Airflow
#     (fourni via _PIP_ADDITIONAL_REQUIREMENTS dans docker-compose.override.yaml)
#   - Variable d'env KAFKA_BOOTSTRAP_SERVERS=kafka:9092
#     (injectee par docker-compose.kafka.yaml dans les services Airflow)
#
# Connexions Airflow requises :
#   sensor_api  (AIRFLOW_CONN_SENSOR_API)
#
# Topic Kafka :
#   sensor-measurements (cree automatiquement via KAFKA_AUTO_CREATE_TOPICS_ENABLE)
#
# Schedule : */1 * * * *  (une fois par minute, en complement du consumer)
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
PRODUCER_FLUSH_TIMEOUT_S = 30


# ─────────────────────────────────────────────────────────────────────────────
# Helpers (testables sans Airflow)
# ─────────────────────────────────────────────────────────────────────────────

def _get_bootstrap_servers() -> str:
    """Retourne l'adresse du broker Kafka depuis l'env (jamais en dur)."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _produce_readings_logic(run_ts: str) -> int:
    """Poll l'API capteurs et produit chaque lecture dans le topic Kafka.

    Chaque message est serialise en JSON. La cle du message est le sensor_id
    (bytes) pour assurer que les messages d'un meme capteur atterrissent
    dans la meme partition (ordre garanti par capteur).

    Args:
        run_ts: horodatage de l'execution (ex. ``"2026-04-13T12-00-00"``),
                ajoute dans chaque message pour traçabilite.

    Returns:
        Nombre de messages produits (avant confirmation de livraison).

    Raises:
        RuntimeError: si aucun capteur n'est disponible ou si trop d'erreurs
                      de livraison sont detectees.
    """
    from confluent_kafka import Producer
    from hooks.sensor_api_hook import SensorAPIHook

    hook = SensorAPIHook()
    sensors = hook.get_sensors()
    active = [s for s in sensors if s.get("is_active", True)]

    if not active:
        print("produce: aucun capteur actif, rien a produire")
        return 0

    delivery_errors: list[str] = []

    def _on_delivery(err, msg):
        if err:
            delivery_errors.append(str(err))

    producer = Producer({
        "bootstrap.servers": _get_bootstrap_servers(),
        "client.id": "smartcity-airflow-producer",
        "acks": "all",
        "delivery.timeout.ms": PRODUCER_FLUSH_TIMEOUT_S * 1000,
    })

    count = 0
    for sensor in active:
        sensor_id = sensor.get("sensor_id") or sensor.get("id")
        if not sensor_id:
            continue
        try:
            readings = hook.get_readings(sensor_id)
            for r in readings:
                r.setdefault("sensor_id", str(sensor_id))
                r["run_ts"] = run_ts
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=str(sensor_id).encode("utf-8"),
                    value=json.dumps(r).encode("utf-8"),
                    on_delivery=_on_delivery,
                )
                count += 1
                producer.poll(0)
        except Exception as exc:
            print(f"[WARN] Erreur lecture capteur {sensor_id}: {exc}")

    remaining = producer.flush(timeout=PRODUCER_FLUSH_TIMEOUT_S)

    if delivery_errors:
        print(f"[WARN] {len(delivery_errors)} erreur(s) de livraison: {delivery_errors[:3]}")
    if remaining > 0:
        print(f"[WARN] {remaining} message(s) non confirmes apres flush")

    print(
        f"produce: {count} messages envoyes au topic '{KAFKA_TOPIC}' "
        f"({len(active)} capteurs actifs)"
    )
    return count


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="smartcity_kafka_measurements_producer",
    description="J2 — Producteur Kafka : sensor API -> topic sensor-measurements",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "kafka", "producer", "j2"],
)
def smartcity_kafka_measurements_producer():

    @task()
    def poll_and_produce() -> int:
        """Lit l'API capteurs et publie chaque lecture dans Kafka."""
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        run_ts = ctx["logical_date"].strftime("%Y-%m-%dT%H-%M-%S")
        return _produce_readings_logic(run_ts)

    poll_and_produce()


smartcity_kafka_measurements_producer()
