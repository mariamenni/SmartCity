from dags.smartcity_alert_check_batch import smartcity_alert_check_batch


def test_dag_import():
    dag = smartcity_alert_check_batch()
    assert dag.dag_id == "smartcity_alert_check_batch"


def test_tasks_exist():
    dag = smartcity_alert_check_batch()
    task_ids = {t.task_id for t in dag.tasks}

    assert "extract_measurements" in task_ids
    assert "detect_threshold_alerts" in task_ids
    assert "detect_offline_sensors" in task_ids
    assert "load_alerts" in task_ids
    assert "report" in task_ids