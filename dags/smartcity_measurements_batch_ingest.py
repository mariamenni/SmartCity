"""
DAG : smartcity_measurements_batch_ingest
Responsable : Narcisse Cabrel TSAFACK FOUEGAP

Rôle : Ingestion batch des mesures IoT toutes les 15 minutes.

CE QUE CE DAG FAIT (dans l'ordre)
  1. extract_measurements
       -> appelle SensorAPIHook.get_measurements(since=now-15min)
       -> récupère toutes les mesures de la fenêtre glissante
       -> retourne une list[dict] via XCom

  2a. stage_raw  (en parallèle avec 2b)
       -> archive le JSON brut dans MinIO
       -> bucket: smartcity-raw / clé: measurements/YYYY/MM/DD/HHMMSS.json

  2b. load_timescaledb  (en parallèle avec 2a)
       -> INSERT dans fact_measurement avec ON CONFLICT DO NOTHING
       -> idempotence garantie par PRIMARY KEY (ts, sensor_id)
       -> retourne le nombre de lignes réellement insérées

IDEMPOTENCE
  Rejouer le même run donne le même résultat en base.
  Mécanisme : PRIMARY KEY (ts, sensor_id) sur fact_measurement
              + ON CONFLICT (ts, sensor_id) DO NOTHING

CONNEXIONS AIRFLOW REQUISES
  sensor_api             -> http://sensor-simulator:5000
  smartcity_timescaledb  -> postgresql://smartcity_user:...@timescaledb:5432/smartcity
  minio_local            -> aws://minio_admin:...@?endpoint_url=http://minio:9000
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

SENSOR_API_CONN_ID = "sensor_api"
TIMESCALE_CONN_ID  = "smartcity_timescaledb"
MINIO_CONN_ID      = "minio_local"
MINIO_BUCKET       = "smartcity-raw"
WINDOW_MINUTES     = 15


@dag(
    dag_id="smartcity_measurements_batch_ingest",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "batch", "j1"],
    doc_md=__doc__,
)
def smartcity_measurements_batch_ingest():

    #Task 1 : Extraire les mesures depuis l'API 
    @task()
    def extract_measurements() -> list[dict]:
        from hooks.sensor_api_hook import SensorAPIHook

        now   = datetime.now(tz=timezone.utc)
        since = (now - timedelta(minutes=WINDOW_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")

        log.info("Fenêtre d'extraction : [%s → maintenant]", since)

        hook         = SensorAPIHook(sensor_api_conn_id=SENSOR_API_CONN_ID)
        measurements = hook.get_measurements(since=since)

        log.info("Mesures extraites : %d", len(measurements))
        return measurements

    #Task 2a : Archiver le brut dans MinIO 
    @task()
    def stage_raw(measurements: list[dict]) -> str:
        if not measurements:
            log.info("Aucune mesure → rien à archiver dans MinIO.")
            return ""

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3  = S3Hook(aws_conn_id=MINIO_CONN_ID)
        now = datetime.now(tz=timezone.utc)
        key = f"measurements/{now.strftime('%Y/%m/%d/%H%M%S')}.json"

        s3.load_string(
            string_data=json.dumps(measurements, ensure_ascii=False),
            key=key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )

        path = f"s3://{MINIO_BUCKET}/{key}"
        log.info("Brut archivé → %s (%d mesures)", path, len(measurements))
        return path

    #Task 2b : Charger dans TimescaleDB 
    @task()
    def load_timescaledb(measurements: list[dict]) -> int:
        if not measurements:
            log.info("Aucune mesure → rien à charger dans TimescaleDB.")
            return 0

        hook = PostgresHook(postgres_conn_id=TIMESCALE_CONN_ID)

        rows = [
            (m["ts"], m["sensor_id"], m["value"], m["unit"])
            for m in measurements
            if "ts" in m and "sensor_id" in m
        ]

        if not rows:
            log.warning("Toutes les mesures sont malformées (ts ou sensor_id manquants).")
            return 0

        sql = """
            INSERT INTO fact_measurement (ts, sensor_id, value, unit)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ts, sensor_id) DO NOTHING
        """

        conn   = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, rows)
            conn.commit()
            inserted = cursor.rowcount
        finally:
            cursor.close()
            conn.close()

        log.info(
            "TimescaleDB : %d/%d lignes insérées (reste = conflits ignorés)",
            inserted, len(rows),
        )
        return inserted

    #Pipeline : extract -> (stage_raw ∥ load_timescaledb) 
    measurements = extract_measurements()
    stage_raw(measurements)
    load_timescaledb(measurements)


smartcity_measurements_batch_ingest()
