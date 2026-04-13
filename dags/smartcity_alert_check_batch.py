# =============================================================================
# DAG  — Détection d'alertes par seuils (P4 — Maria)
#
# Ce DAG analyse les mesures récentes et génère des alertes quand des seuils
# sont dépassés. Il s'exécute toutes les 15 minutes.
#
#   1. read_recent_measurements → requête TimescaleDB (fenêtre 15 min)
#   2. detect_thresholds        → compare chaque mesure aux seuils
#   3. write_alerts             → INSERT dans fact_alert (idempotent via UUID)
#
# Seuils (configurables via la constante THRESHOLDS) :
#   temperature   : warning > 35 °C   | critical > 40 °C
#   air_quality   : warning > 3       | critical > 4   (échelle 0-5)
#   traffic_flow  : warning > 80      | critical > 95  (% occupation)
#   humidity      : warning > 85 %    | critical > 95 %
#   noise_level   : warning > 70 dB   | critical > 85 dB
#
# Schedule : */15 * * * *
# Connexions : smartcity_timescaledb
# =============================================================================
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task

# Seuils par type de capteur : (warning, critical)
THRESHOLDS: dict[str, tuple[float, float]] = {
    "temperature":  (35.0, 40.0),
    "air_quality":  (3.0,  4.0),
    "traffic_flow": (80.0, 95.0),
    "humidity":     (85.0, 95.0),
    "noise_level":  (70.0, 85.0),
}


def _check_violation(sensor_type: str, value: float) -> tuple[str, float] | None:
    """Teste si une valeur dépasse un seuil.

    Returns:
        (severity, threshold) si violation, None sinon.
    """
    if sensor_type not in THRESHOLDS:
        return None
    warn_thresh, crit_thresh = THRESHOLDS[sensor_type]
    if value >= crit_thresh:
        return ("critical", crit_thresh)
    if value >= warn_thresh:
        return ("warning", warn_thresh)
    return None


def _get_pg_conn():
    """Retourne une connexion psycopg2 via la connexion Airflow timescaledb."""
    import psycopg2
    from airflow.hooks.base import BaseHook

    conn_info = BaseHook.get_connection("smartcity_timescaledb")
    return psycopg2.connect(
        host=conn_info.host,
        port=int(conn_info.port or 5432),
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
    )


@dag(
    dag_id="smartcity_alert_check_batch",
    description="P4 — Détection d'alertes par seuils sur les 15 dernières minutes",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "alerting", "j1"],
)
def smartcity_alert_check_batch():

    @task()
    def read_recent_measurements() -> list[dict]:
        """Récupère les mesures des 15 dernières minutes depuis fact_measurement."""
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT fm.ts, fm.sensor_id, ds.type, fm.value, fm.unit
                    FROM fact_measurement fm
                    LEFT JOIN dim_sensor ds ON ds.sensor_id = fm.sensor_id
                    WHERE fm.ts >= NOW() - INTERVAL '15 minutes'
                    ORDER BY fm.ts DESC
                    """,
                )
                rows = cur.fetchall()
            measurements = [
                {
                    "ts":        row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]),
                    "sensor_id": row[1],
                    "type":      row[2] or "unknown",
                    "value":     float(row[3]),
                    "unit":      row[4],
                }
                for row in rows
            ]
            print(f"read_recent_measurements: {len(measurements)} mesures (15 min)")
            return measurements
        finally:
            conn.close()

    @task()
    def detect_thresholds(measurements: list[dict]) -> list[dict]:
        """Compare chaque mesure aux seuils et retourne la liste des violations."""
        violations: list[dict] = []

        for m in measurements:
            sensor_type = m.get("type", "unknown")
            result = _check_violation(sensor_type, m["value"])
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
            print(
                f"[ALERT {severity.upper()}] sensor={m['sensor_id']} "
                f"type={sensor_type} value={m['value']} >= {threshold}"
            )

        print(f"detect_thresholds: {len(violations)} violation(s) détectée(s)")
        return violations

    @task()
    def write_alerts(violations: list[dict]) -> int:
        """Insère les violations dans fact_alert (UUID auto-généré côté DB)."""
        if not violations:
            print("write_alerts: aucune alerte à écrire")
            return 0

        conn = _get_pg_conn()
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO fact_alert (ts, sensor_id, severity, value, threshold)
                        VALUES (%(ts)s, %(sensor_id)s, %(severity)s, %(value)s, %(threshold)s)
                        """,
                        violations,
                    )
            print(f"write_alerts: {len(violations)} alerte(s) insérée(s) dans fact_alert")
            return len(violations)
        finally:
            conn.close()

    measurements = read_recent_measurements()
    violations   = detect_thresholds(measurements)
    write_alerts(violations)


smartcity_alert_check_batch()

