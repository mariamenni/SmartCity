# =============================================================================
# DAG  — Batch ingest des mesures par fenêtre 15 min (P3 — Narcisse)
#
# Ce DAG collecte toutes les mesures sur une fenêtre glissante de 15 minutes,
# les stocke brutes dans MinIO, puis les charge dans TimescaleDB.
#
#   1. extract_measurements → poll API (tous capteurs actifs), range min/max ts
#   2. stage_raw            → sérialise dans MinIO batch/{run_ts}.json
#   3. load_timescaledb     → INSERT … ON CONFLICT DO NOTHING (idempotent)
#
# Différence avec P5 (consumer minutely) :
#   P5 s'exécute chaque minute et insère les lectures en temps réel.
#   P3 s'exécute toutes les 15 minutes et joue le rôle de filet de sécurité :
#   si P5 manque un cycle la mesure sera tout de même chargée par P3.
#   ON CONFLICT DO NOTHING garantit l'absence de doublons.
#
# Schedule : */15 * * * *
# Connexions : sensor_api, minio_local, smartcity_timescaledb
# =============================================================================
from __future__ import annotations

import json
from datetime import datetime, timezone

from airflow.sdk import dag, task

MINIO_BUCKET = "smartcity"


def _s3_client():
    import boto3
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection("minio_local")
    extra = json.loads(conn.extra) if conn.extra else {}
    return boto3.client(
        "s3",
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        endpoint_url=extra.get("endpoint_url", "http://minio:9000"),
        region_name=extra.get("region_name", "us-east-1"),
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def _filter_valid_records(records: list[dict]) -> list[dict]:
    """Filtre les enregistrements ayant les champs obligatoires ts, sensor_id, value, unit."""
    return [
        r for r in records
        if r.get("ts") and r.get("sensor_id") and r.get("value") is not None and r.get("unit")
    ]


@dag(
    dag_id="smartcity_measurements_batch_ingest",
    description="P3 — Batch ingest fenêtre 15 min : poll API → MinIO → TimescaleDB",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "batch", "j1"],
)
def smartcity_measurements_batch_ingest():

    @task()
    def extract_measurements() -> list[dict]:
        """Poll l'API pour récupérer les lectures de tous les capteurs actifs."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()
        active = [s for s in sensors if s.get("status", "active") == "active"]

        all_readings: list[dict] = []
        for sensor in active:
            sensor_id = sensor["id"]
            try:
                readings = hook.get_readings(sensor_id)
                for r in readings:
                    all_readings.append({
                        "ts":        r.get("timestamp") or r.get("ts") or r.get("time"),
                        "sensor_id": str(r.get("sensor_id", sensor_id)),
                        "value":     float(r["value"]),
                        "unit":      str(r.get("unit", "")),
                    })
            except Exception as exc:
                print(f"[WARN] Readings introuvables pour sensor_id={sensor_id}: {exc}")

        print(f"extract_measurements: {len(all_readings)} lectures ({len(active)} capteurs actifs)")
        return all_readings

    @task()
    def stage_raw(readings: list[dict]) -> str:
        """Sérialise le batch brut dans MinIO sous batch/{run_ts}.json."""
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        run_ts = ctx["logical_date"].strftime("%Y-%m-%dT%H-%M-%S")

        s3 = _s3_client()
        _ensure_bucket(s3, MINIO_BUCKET)
        key = f"batch/{run_ts}.json"
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=json.dumps(readings).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"stage_raw: {len(readings)} lectures → MinIO {key}")
        return key

    @task()
    def load_timescaledb(batch_key: str) -> int:
        """Charge le batch depuis MinIO vers fact_measurement (idempotent)."""
        import psycopg2
        from airflow.hooks.base import BaseHook

        s3 = _s3_client()
        obj = s3.get_object(Bucket=MINIO_BUCKET, Key=batch_key)
        records: list[dict] = json.loads(obj["Body"].read().decode("utf-8"))

        # Filtre les enregistrements incomplets
        valid = _filter_valid_records(records)

        if not valid:
            print("load_timescaledb: aucune mesure valide dans le batch")
            return 0

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
                        valid,
                    )
            print(f"load_timescaledb: {len(valid)} mesures insérées (ON CONFLICT DO NOTHING)")
            return len(valid)
        finally:
            conn.close()

    readings   = extract_measurements()
    batch_key  = stage_raw(readings)
    load_timescaledb(batch_key)


smartcity_measurements_batch_ingest()

