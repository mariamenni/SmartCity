# =============================================================================
# DAG  — Consumer micro-batch minutely (P5 — Gills)
#
# Flux : poll_api → transform → flush_to_timescaledb
#
#   poll_api              : interroge SensorAPIHook (get_sensors + get_readings
#                           par capteur), stocke le brut dans MinIO raw/
#                           → XCom : chemin S3 (str)
#   transform             : lit le brut depuis MinIO, valide les champs
#                           obligatoires, déduplique (ts, sensor_id), contrôle
#                           la latence (>5 min → warning), stocke le nettoyé
#                           dans MinIO clean/
#                           → XCom : chemin S3 (str)
#   flush_to_timescaledb  : lit le nettoyé depuis MinIO, INSERT INTO
#                           fact_measurement … ON CONFLICT DO NOTHING
#                           → XCom : nombre d'enregistrements insérés (int)
#
# Connexions Airflow requises :
#   sensor_api            (AIRFLOW_CONN_SENSOR_API)
#   minio_local           (AIRFLOW_CONN_MINIO_LOCAL)
#   smartcity_timescaledb (AIRFLOW_CONN_SMARTCITY_TIMESCALEDB)
# =============================================================================
from __future__ import annotations

import json
from datetime import datetime, timezone

from airflow.sdk import dag, task

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────
MINIO_BUCKET = "smartcity"
LATENCY_WARN_SECONDS = 300  # 5 minutes


# ─────────────────────────────────────────────────────────────────────────────
# Helpers S3 / MinIO (testables via injection du client en tests)
# ─────────────────────────────────────────────────────────────────────────────

def _s3_client():
    """Construit un client boto3 S3 configuré pour MinIO depuis `minio_local`."""
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
    """Crée le bucket si absent (idempotent)."""
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


# ─────────────────────────────────────────────────────────────────────────────
# Logique métier — fonctions testables indépendamment du framework Airflow
# ─────────────────────────────────────────────────────────────────────────────

def _poll_api_logic(run_ts: str) -> str:
    """Interroge le SensorAPIHook pour chaque capteur actif.

    Récupère la liste des capteurs via ``get_sensors()``, puis les lectures
    de chaque capteur actif via ``get_readings(sensor_id)``.  Le résultat brut
    est sérialisé en JSON et stocké dans MinIO sous la clé ``raw/<run_ts>.json``.

    Args:
        run_ts: horodatage de l'exécution (format lisible, ex. ``2025-01-01T00-00-00``)
                utilisé comme identifiant de la fenêtre dans MinIO.

    Returns:
        Chemin S3 du fichier brut (ex. ``"raw/2025-01-01T00-00-00.json"``).
    """
    from hooks.sensor_api_hook import SensorAPIHook

    hook = SensorAPIHook()
    sensors: list[dict] = hook.get_sensors()
    active = [s for s in sensors if s.get("is_active", True)]

    all_readings: list[dict] = []
    for sensor in active:
        sensor_id = sensor.get("sensor_id") or sensor.get("id")
        if not sensor_id:
            continue
        try:
            readings: list[dict] = hook.get_readings(sensor_id)
            for r in readings:
                r.setdefault("sensor_id", str(sensor_id))
            all_readings.extend(readings)
        except Exception as exc:
            print(f"[WARN] lectures impossibles pour {sensor_id}: {exc}")

    print(
        f"poll_api: {len(all_readings)} lectures brutes "
        f"({len(active)} capteurs actifs)"
    )

    s3 = _s3_client()
    _ensure_bucket(s3, MINIO_BUCKET)
    key = f"raw/{run_ts}.json"
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(all_readings).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _transform_logic(raw_key: str, run_ts: str) -> str:
    """Valide, déduplique et contrôle la latence des lectures brutes.

    Règles appliquées :
    - Les champs ``ts``, ``sensor_id``, ``value`` et ``unit`` sont obligatoires.
    - ``value`` doit être convertible en ``float``.
    - Les doublons ``(ts, sensor_id)`` sont écartés.
    - Une latence > 5 min déclenche un avertissement (l'enregistrement est conservé).

    Args:
        raw_key: clé S3 du fichier brut produit par ``_poll_api_logic``.
        run_ts:  horodatage utilisé pour la clé de sortie dans MinIO.

    Returns:
        Chemin S3 du fichier nettoyé (ex. ``"clean/2025-01-01T00-00-00.json"``).
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=raw_key)
    raw_readings: list[dict] = json.loads(obj["Body"].read().decode("utf-8"))

    now = datetime.now(timezone.utc)
    seen: set[tuple[str, str]] = set()
    valid: list[dict] = []
    skipped = 0
    late = 0

    for r in raw_readings:
        ts        = r.get("ts") or r.get("timestamp") or r.get("time")
        sensor_id = r.get("sensor_id")
        value     = r.get("value")
        unit      = r.get("unit")

        # Champs obligatoires
        if not all([ts, sensor_id, value is not None, unit]):
            skipped += 1
            continue

        # Valeur numérique
        try:
            float_val = float(value)
        except (TypeError, ValueError):
            skipped += 1
            continue

        # Dédoublonnage (ts, sensor_id)
        dedup_key = (str(ts), str(sensor_id))
        if dedup_key in seen:
            skipped += 1
            continue
        seen.add(dedup_key)

        # Contrôle de latence
        try:
            ts_dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            latency_s = (now - ts_dt).total_seconds()
            if latency_s > LATENCY_WARN_SECONDS:
                print(
                    f"[WARN] Latence {latency_s:.0f}s pour capteur {sensor_id}"
                    f" (ts={ts})"
                )
                late += 1
        except (ValueError, TypeError):
            pass  # timestamp non parseable → on conserve quand même

        valid.append({
            "ts":        str(ts),
            "sensor_id": str(sensor_id),
            "value":     float_val,
            "unit":      str(unit),
        })

    print(
        f"transform: {len(valid)} valides, {skipped} écartés, "
        f"{late} en retard (>{LATENCY_WARN_SECONDS // 60} min)"
    )

    clean_key = f"clean/{run_ts}.json"
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=clean_key,
        Body=json.dumps(valid).encode("utf-8"),
        ContentType="application/json",
    )
    return clean_key


def _flush_logic(clean_key: str) -> int:
    """Insère les enregistrements nettoyés dans ``fact_measurement``.

    Utilise ``ON CONFLICT (ts, sensor_id) DO NOTHING`` pour l'idempotence :
    rejouer le DAG n'entraîne pas de doublons en base.

    Args:
        clean_key: clé S3 du fichier nettoyé produit par ``_transform_logic``.

    Returns:
        Nombre d'enregistrements soumis à l'insertion.
    """
    s3 = _s3_client()
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=clean_key)
    records: list[dict] = json.loads(obj["Body"].read().decode("utf-8"))

    if not records:
        print("flush: aucune mesure à insérer")
        return 0

    import psycopg2
    from airflow.hooks.base import BaseHook

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
        print(f"flush: {len(records)} mesures insérées dans fact_measurement")
        return len(records)
    finally:
        conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

@dag(
    dag_id="smartcity_measurements_consumer_minutely",
    description=(
        "P5 — Consumer micro-batch minutely : "
        "poll API → transform → flush TimescaleDB"
    ),
    schedule="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "consumer", "j1"],
)
def smartcity_measurements_consumer_minutely():

    @task()
    def poll_api() -> str:
        """Récupère les lectures brutes de tous les capteurs actifs → MinIO raw/."""
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        run_ts = ctx["logical_date"].strftime("%Y-%m-%dT%H-%M-%S")
        return _poll_api_logic(run_ts)

    @task()
    def transform(raw_key: str) -> str:
        """Valide, déduplique et contrôle la latence → MinIO clean/."""
        from airflow.sdk import get_current_context

        ctx = get_current_context()
        run_ts = ctx["logical_date"].strftime("%Y-%m-%dT%H-%M-%S")
        return _transform_logic(raw_key, run_ts)

    @task()
    def flush_to_timescaledb(clean_key: str) -> int:
        """Insère les mesures nettoyées dans fact_measurement."""
        return _flush_logic(clean_key)

    raw_key   = poll_api()
    clean_key = transform(raw_key)
    flush_to_timescaledb(clean_key)


smartcity_measurements_consumer_minutely()
