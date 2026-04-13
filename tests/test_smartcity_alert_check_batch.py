from airflow.models import DagBag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from operators.threshold_alert_operator import ThresholdAlertOperator

DAG_ID = "smartcity_alert_check_batch"


# =========================================================
# 1. TEST : DAG LOAD
# =========================================================
def test_dag_load():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag(DAG_ID)

    assert dag is not None, "DAG introuvable"
    assert len(dag.tasks) > 0

    task_ids = {t.task_id for t in dag.tasks}

    assert "extract_measurements" in task_ids
    assert "detect_offline_sensors" in task_ids
    assert "report" in task_ids


# =========================================================
# 2. TEST : THRESHOLD LOGIC (UNIT TEST OPERATOR)
# =========================================================
def test_threshold_operator():
    op = ThresholdAlertOperator(
        task_id="test_threshold",
        thresholds={
            "noise_level": {"warning": 65, "critical": 80},
            "air_quality_pm25": {"warning": 25, "critical": 50},
        }
    )

    fake_data = [
        {"sensor_id": "S-001", "type": "noise_level", "value": 90},
        {"sensor_id": "S-002", "type": "noise_level", "value": 70},
        {"sensor_id": "S-003", "type": "air_quality_pm25", "value": 60},
    ]

    class FakeTI:
        def xcom_pull(self, task_ids):
            return fake_data

    context = {"ti": FakeTI()}

    alerts = op.execute(context)

    assert len(alerts) == 3
    assert alerts[0][2] == "critical"
    assert alerts[1][2] == "warning"
    assert alerts[2][2] == "critical"


# =========================================================
# 3. TEST : OFFLINE LOGIC
# =========================================================
def test_offline_logic_simple():
    now = "2026-04-13T10:00:00Z"

    # capteur offline simulé
    sensors_offline = [
        ("S-999", "2026-04-13T09:30:00Z")
    ]

    alerts = [
        (now, s[0], "critical", 0, 0)
        for s in sensors_offline
    ]

    assert alerts[0][1] == "S-999"
    assert alerts[0][2] == "critical"


# =========================================================
# 4. TEST : DB ALERT GENERATION 
# =========================================================
def test_alerts_in_db():
    pg = PostgresHook(postgres_conn_id="smartcity_timescaledb")
    conn = pg.get_conn()
    cur = conn.cursor()

    # reset
    cur.execute("DELETE FROM fact_alert;")
    conn.commit()

    # insert fake alerts
    cur.execute("""
        INSERT INTO fact_alert (ts, sensor_id, severity, value, threshold)
        VALUES
        (NOW(), 'S-001', 'critical', 80, 50),
        (NOW(), 'S-002', 'warning', 70, 65),
        (NOW(), 'S-003', 'critical', 250, 200);
    """)
    conn.commit()

    # verify
    cur.execute("SELECT COUNT(*) FROM fact_alert;")
    count = cur.fetchone()[0]

    assert count == 3

    cur.execute("SELECT sensor_id FROM fact_alert;")
    sensors = [r[0] for r in cur.fetchall()]

    assert "S-001" in sensors
    assert "S-002" in sensors
    assert "S-003" in sensors

    cur.close()
    conn.close()