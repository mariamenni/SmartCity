from __future__ import annotations

from airflow.models import BaseOperator
from airflow.utils.context import Context
from datetime import datetime


class ThresholdAlertOperator(BaseOperator):

    def __init__(self, thresholds: dict, **kwargs):
        super().__init__(**kwargs)
        self.thresholds = thresholds

    def execute(self, context: Context):
        ti = context["ti"]

        data = ti.xcom_pull(task_ids="extract_measurements")

        if not data:
            self.log.info("Aucune donnée à analyser")
            return []

        alerts = []

        for row in data:
            sensor_type = row["type"]
            value = row["value"]

            if sensor_type not in self.thresholds:
                continue

            rules = self.thresholds[sensor_type]

            severity = None
            threshold_value = None

            if "critical" in rules and value >= rules["critical"]:
                severity = "critical"
                threshold_value = rules["critical"]

            elif "warning" in rules and value >= rules["warning"]:
                severity = "warning"
                threshold_value = rules["warning"]

            if severity:
                alerts.append(
                    (
                        datetime.utcnow(),
                        row["sensor_id"],
                        severity,
                        value,
                        threshold_value,
                    )
                )

        self.log.info(f"{len(alerts)} alertes seuil détectées")

        return alerts