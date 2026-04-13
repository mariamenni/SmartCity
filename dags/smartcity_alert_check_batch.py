from __future__ import annotations

from datetime import datetime , timezone
from airflow.sdk import dag, task
from operators.threshold_alert_operator import ThresholdAlertOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


@dag(
    dag_id="smartcity_alert_check_batch",
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "alerting", "final"],
)
def smartcity_alert_check_batch():

    # -------------------------
    # EXTRACT
    # -------------------------
    @task()
    def extract_measurements() -> list[dict]:
        pg = PostgresHook(postgres_conn_id="smartcity_timescaledb")

        sql = """
            SELECT fm.sensor_id, ds.type, fm.value, fm.ts
            FROM fact_measurement fm
            JOIN dim_sensor ds ON fm.sensor_id = ds.sensor_id
            WHERE fm.ts > NOW() - INTERVAL '15 minutes'
        """

        records = pg.get_records(sql)

        return [
            {
                "sensor_id": r[0],
                "type": r[1],
                "value": float(r[2]),
                "ts": r[3],
            }
            for r in records
        ]

    # -------------------------
    # THRESHOLD RULES
    # -------------------------
    thresholds = {
        "air_quality_pm25": {"warning": 25, "critical": 50},
        "air_quality_no2": {"warning": 100, "critical": 200},
        "traffic_density": {"critical": 2000},
        "noise_level": {"warning": 65, "critical": 80},
    }

    threshold_task = ThresholdAlertOperator(
        task_id="detect_threshold_alerts",
        thresholds=thresholds,
    )

    # -------------------------
    # OFFLINE DETECTION 
    # -------------------------
    @task()
    def detect_offline_sensors() -> list[tuple]:
        pg = PostgresHook(postgres_conn_id="smartcity_timescaledb")

        sql = """
            SELECT ds.sensor_id, MAX(fm.ts) as last_seen
            FROM dim_sensor ds
            LEFT JOIN fact_measurement fm
                ON ds.sensor_id = fm.sensor_id
            WHERE ds.is_active = true
            GROUP BY ds.sensor_id
            HAVING MAX(fm.ts) IS NULL
                OR MAX(fm.ts) < NOW() - INTERVAL '15 minutes'
        """

        sensors = pg.get_records(sql)

        return [
            (
                datetime.now(timezone.utc),
                s[0],
                "critical",
                0,   
                0,   
            )
            for s in sensors
        ]


    # -------------------------
    # LOAD ALERTS (IDEMPOTENT)
    # -------------------------
    @task()
    def load_alerts(alerts: list[tuple]) -> int:
        if not alerts:
            return 0

        pg = PostgresHook(postgres_conn_id="smartcity_timescaledb")
        conn = pg.get_conn()
        cur = conn.cursor()

        sql = """
            INSERT INTO fact_alert (ts, sensor_id, severity, value, threshold)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ts, sensor_id, severity) DO NOTHING
        """

        cur.executemany(sql, alerts)
        conn.commit()

        cur.close()
        conn.close()

        return len(alerts)

    # -------------------------
    # REPORT
    # -------------------------
    @task()
    def report(nb_threshold: int, nb_offline: int):
        print("=" * 60)
        print("RAPPORT ALERTING FINAL")
        print("=" * 60)
        print(f"Alertes seuil    : {nb_threshold}")
        print(f"Capteurs offline : {nb_offline}")
        print("=" * 60)

    # PIPELINE
    data = extract_measurements()
    offline_alerts = detect_offline_sensors()

    nb_threshold = load_alerts(threshold_task.output)
    nb_offline = load_alerts(offline_alerts)

    report(nb_threshold, nb_offline)

    data >> threshold_task


smartcity_alert_check_batch()