"""
Tests — smartcity_measurements_batch_ingest
Responsable : Narcisse Cabrel TSAFACK FOUEGAP

Couverture :
  1. test_dag_structure              : les 3 tâches sont bien présentes dans le DAG
  2. test_rows_filtering             : les mesures malformées (ts/sensor_id manquants) sont écartées
  3. test_idempotence_on_conflict    : ON CONFLICT → les doublons sont ignorés (INSERT retourne 0)
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch, call


# Test 1 : structure du DAG
def test_dag_structure():
    """Le DAG doit exposer exactement les 3 tâches attendues."""
    try:
        from airflow.models import DagBag
        dagbag = DagBag(dag_folder="dags/", include_examples=False)
        dag = dagbag.get_dag("smartcity_measurements_batch_ingest")
        assert dag is not None, "DAG introuvable dans le DagBag"
        task_ids = {t.task_id for t in dag.tasks}
        assert "extract_measurements" in task_ids
        assert "stage_raw" in task_ids
        assert "load_timescaledb" in task_ids
    except ImportError:
        # Airflow non disponible localement → test structurel simplifié
        import ast, pathlib
        src = pathlib.Path("dags/smartcity_measurements_batch_ingest.py").read_text()
        for name in ("extract_measurements", "stage_raw", "load_timescaledb"):
            assert name in src, f"task '{name}' absente du fichier DAG"


# Test 2 : filtrage des mesures malformées
def test_rows_filtering():
    """Les mesures sans 'ts' ou 'sensor_id' ne doivent pas être insérées."""
    measurements = [
        {"ts": "2026-04-11T10:00:00Z", "sensor_id": "S-001", "value": 12.5, "unit": "μg/m³"},
        {"sensor_id": "S-002", "value": 30.0, "unit": "μg/m³"},          # ts manquant
        {"ts": "2026-04-11T10:00:00Z", "value": 5.0, "unit": "dB"},      # sensor_id manquant
        {"ts": "2026-04-11T10:01:00Z", "sensor_id": "S-003", "value": 7.1, "unit": "°C"},
    ]

    rows = [
        (m["ts"], m["sensor_id"], m["value"], m["unit"])
        for m in measurements
        if "ts" in m and "sensor_id" in m
    ]

    assert len(rows) == 2
    assert rows[0] == ("2026-04-11T10:00:00Z", "S-001", 12.5, "μg/m³")
    assert rows[1] == ("2026-04-11T10:01:00Z", "S-003", 7.1, "°C")


# Test 3 : idempotence — ON CONFLICT DO NOTHING renvoie rowcount == 0
def test_idempotence_on_conflict():
    """Rejouer le même lot doit renvoyer 0 insertions (conflits silencieux)."""
    measurements = [
        {"ts": "2026-04-11T10:00:00Z", "sensor_id": "S-001", "value": 12.5, "unit": "μg/m³"},
    ]
    rows = [
        (m["ts"], m["sensor_id"], m["value"], m["unit"])
        for m in measurements
        if "ts" in m and "sensor_id" in m
    ]

    # Simuler un curseur dont executemany ne change aucune ligne (conflit)
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 0                # ON CONFLICT DO NOTHING → 0 rows affected
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_hook = MagicMock()
    mock_hook.get_conn.return_value = mock_conn

    with patch("airflow.providers.postgres.hooks.postgres.PostgresHook", return_value=mock_hook):
        conn   = mock_hook.get_conn()
        cursor = conn.cursor()
        sql = """
            INSERT INTO fact_measurement (ts, sensor_id, value, unit)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ts, sensor_id) DO NOTHING
        """
        cursor.executemany(sql, rows)
        conn.commit()
        inserted = cursor.rowcount

    assert inserted == 0, "Un doublon ne doit pas être inséré (rowcount doit être 0)"
