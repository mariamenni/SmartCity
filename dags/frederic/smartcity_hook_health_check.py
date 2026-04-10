# dags/frederic/smartcity_hook_health_check.py
# =============================================================================
# DAG  — Validation du Hook + Connexions Airflow
#
# Ce DAG prouve que le SensorAPIHook fonctionne de bout en bout :
#   1. health_check()              → vérifie que l'API est joignable
#   2. fetch_sensors()             → récupère la liste des capteurs
#   3. fetch_readings()            → récupère les readings d'un capteur
#   4. fetch_metrics()             → récupère les métriques de la ville
#   5. report()                    → log un résumé pour validation en UI
#
# Schedule : @daily (ou déclenchement manuel pour la démo)
# Connexion requise : sensor_api (AIRFLOW_CONN_SENSOR_API)
# =============================================================================
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="smartcity_hook_health_check",
    description="P1 — Validation du SensorAPIHook (health, sensors, readings, metrics)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["smartcity", "hook", "validation", "j1"],
)
def smartcity_hook_health_check():

    @task()
    def health_check() -> bool:
        """Vérifie que la smart-cities-api est joignable via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        is_healthy = hook.health_check()
        if not is_healthy:
            raise RuntimeError(
                "smart-cities-api ne répond pas ! "
                "Vérifier que le service est démarré et que la connexion "
                "sensor_api est bien configurée."
            )
        print("smart-cities-api est UP et répond healthy")
        return is_healthy

    @task()
    def fetch_sensors() -> list[dict]:
        """Récupère la liste complète des capteurs via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()
        print(f"{len(sensors)} capteurs récupérés via /api/v1/sensors")
        for s in sensors:
            print(f"   id={s['id']} | {s['name']:40s} | {s['type']:20s} | {s['status']}")
        return sensors

    @task()
    def fetch_readings(sensors: list[dict]) -> int:
        """Récupère les readings du premier capteur actif."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        active = [s for s in sensors if s.get("status") == "active"]
        if not active:
            print("Aucun capteur actif, skip readings")
            return 0
        sensor_id = active[0]["id"]
        readings = hook.get_readings(sensor_id, limit=10)
        print(f"{len(readings)} readings récupérés pour sensor {sensor_id}")
        for r in readings[:3]:
            print(f"   value={r['value']} {r['unit']}  ts={r.get('timestamp', 'N/A')}")
        return len(readings)

    @task()
    def fetch_metrics() -> dict:
        """Récupère les métriques de la ville via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        metrics = hook.get_metrics()
        print(f"Métriques ville : {metrics.get('city', 'N/A')}")
        print(f"   Total capteurs : {metrics.get('total_sensors')}")
        print(f"   Actifs         : {metrics.get('active_sensors')}")
        print(f"   Temp moyenne   : {metrics.get('average_temperature')}")
        print(f"   Qualité air    : {metrics.get('average_air_quality')}")
        return metrics

    @task()
    def report(
        is_healthy: bool,
        sensors: list[dict],
        nb_readings: int,
        metrics: dict,
    ) -> None:
        """Log un résumé de validation pour la démo soutenance."""
        active = sum(1 for s in sensors if s.get("status") == "active")
        inactive = len(sensors) - active
        print("=" * 60)
        print("  RAPPORT DE VALIDATION — SensorAPIHook (P1 Frédéric)")
        print("=" * 60)
        print(f"  API Health       : {'OK' if is_healthy else 'KO'}")
        print(f"  Ville            : {metrics.get('city', 'N/A')}")
        print(f"  Capteurs total   : {len(sensors)}")
        print(f"  Capteurs actifs  : {active}")
        print(f"  Capteurs inactifs: {inactive}")
        print(f"  Readings testés  : {nb_readings}")
        print(f"  Temp moyenne     : {metrics.get('average_temperature')}")
        print("=" * 60)
        print("  -> Hook validé, prêt pour P2/P3/P4/P5")
        print("=" * 60)

    # Orchestration
    ok = health_check()
    sensors_data = fetch_sensors()
    nb = fetch_readings(sensors_data)
    city_metrics = fetch_metrics()
    report(
        is_healthy=ok,
        sensors=sensors_data,
        nb_readings=nb,
        metrics=city_metrics,
    )


smartcity_hook_health_check()
