# dags/smartcity_hook_health_check.py
# =============================================================================
# DAG  — Validation du Hook + Connexions Airflow
#
# Ce DAG prouve que le SensorAPIHook fonctionne de bout en bout :
#   1. health_check()       → vérifie que l'API est joignable
#   2. fetch_sensors()      → récupère la liste des capteurs
#   3. fetch_metrics()      → récupère les métriques agrégées
#   4. report()             → log un résumé pour validation en UI
#
# Schedule : @daily (ou déclenchement manuel pour la démo)
# Connexion requise : sensor_api (AIRFLOW_CONN_SENSOR_API)
# =============================================================================
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="smartcity_hook_health_check",
    description="P1 — Validation du SensorAPIHook (health, sensors, metrics)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "hook", "validation", "j1"],
)
def smartcity_hook_health_check():

    @task()
    def health_check() -> bool:
        """Vérifie que le sensor-simulator est joignable via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        is_healthy = hook.health_check()
        if not is_healthy:
            raise RuntimeError(
                "sensor-simulator ne répond pas ! "
                "Vérifier que le service est démarré et que la connexion "
                "sensor_api est bien configurée."
            )
        print("sensor-simulator est UP et répond OK")
        return is_healthy

    @task()
    def fetch_sensors() -> list[dict]:
        """Récupère la liste complète des capteurs via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()
        print(f"{len(sensors)} capteurs récupérés via SensorAPIHook.get_sensors()")
        for s in sensors:
            status = s.get("status", "unknown")
            print(f"   id={s['id']} | {s['type']:25s} | {status}")
        return sensors

    @task()
    def fetch_metrics() -> dict:
        """Récupère les métriques agrégées de la ville via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        metrics = hook.get_metrics()
        print(f"Métriques récupérées : city={metrics.get('city')} "
              f"total_sensors={metrics.get('total_sensors')} "
              f"active_sensors={metrics.get('active_sensors')}")
        return metrics

    @task()
    def report(
        is_healthy: bool,
        sensors: list[dict],
        metrics: dict,
    ) -> None:
        """Log un résumé de validation pour la démo soutenance."""
        active = sum(1 for s in sensors if s.get("status") == "active")
        inactive = len(sensors) - active
        print("=" * 60)
        print("  RAPPORT DE VALIDATION — SensorAPIHook (P1 Frédéric)")
        print("=" * 60)
        print(f"  API Health       : {'OK' if is_healthy else 'KO'}")
        print(f"  Capteurs total   : {len(sensors)}")
        print(f"  Capteurs actifs  : {active}")
        print(f"  Capteurs inactifs: {inactive}")
        print(f"  Ville            : {metrics.get('city', 'N/A')}")
        print(f"  Temp. moy.       : {metrics.get('average_temperature', 'N/A')} °C")
        print("=" * 60)
        print("  -> Hook validé, prêt pour P2/P3/P4/P5")
        print("=" * 60)

    # Orchestration
    ok = health_check()
    sensors_data = fetch_sensors()
    metrics_data = fetch_metrics()
    report(
        is_healthy=ok,
        sensors=sensors_data,
        metrics=metrics_data,
    )


smartcity_hook_health_check()


    @task()
    def health_check() -> bool:
        """Vérifie que le sensor-simulator est joignable via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        is_healthy = hook.health_check()
        if not is_healthy:
            raise RuntimeError(
                "sensor-simulator ne répond pas ! "
                "Vérifier que le service est démarré et que la connexion "
                "sensor_api est bien configurée."
            )
        print("sensor-simulator est UP et répond OK")
        return is_healthy

    @task()
    def fetch_sensors() -> list[dict]:
        """Récupère la liste complète des capteurs via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()
        print(f"{len(sensors)} capteurs récupérés via SensorAPIHook.get_sensors()")
        for s in sensors:
            status = "actif" if s.get("is_active") else "inactif"
            print(f"   {s['sensor_id']} | {s['type']:25s} | {status}")
        return sensors

    @task()
    def fetch_locations() -> list[dict]:
        """Récupère la liste des locations via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        locations = hook.get_locations()
        print(f"{len(locations)} locations récupérées via SensorAPIHook.get_locations()")
        for loc in locations:
            print(f"   {loc['location_id']} | {loc['district']:15s} | {loc['zone_type']}")
        return locations

    @task()
    def fetch_sample_measurements() -> int:
        """Récupère les mesures des 15 dernières minutes via le Hook."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        now = datetime.utcnow()
        start = (now - timedelta(minutes=15)).isoformat()
        end = now.isoformat()
        measurements = hook.get_measurements(start_ts=start, end_ts=end)
        print(f"{len(measurements)} mesures récupérées (fenêtre 15 min)")
        if measurements:
            sample = measurements[0]
            print(
                f"   Exemple : sensor={sample['sensor_id']} "
                f"type={sample['type']} value={sample['value']} {sample['unit']}"
            )
        return len(measurements)

    @task()
    def report(
        is_healthy: bool,
        sensors: list[dict],
        locations: list[dict],
        nb_measures: int,
    ) -> None:
        """Log un résumé de validation pour la démo soutenance."""
        active = sum(1 for s in sensors if s.get("is_active"))
        inactive = len(sensors) - active
        print("=" * 60)
        print("  RAPPORT DE VALIDATION — SensorAPIHook (P1 Frédéric)")
        print("=" * 60)
        print(f"  API Health       : {'OK' if is_healthy else 'KO'}")
        print(f"  Locations        : {len(locations)}")
        print(f"  Capteurs total   : {len(sensors)}")
        print(f"  Capteurs actifs  : {active}")
        print(f"  Capteurs inactifs: {inactive}")
        print(f"  Mesures (15 min) : {nb_measures}")
        print("=" * 60)
        print("  -> Hook validé, prêt pour P2/P3/P4/P5")
        print("=" * 60)

    # Orchestration
    ok = health_check()
    sensors_data = fetch_sensors()
    locations_data = fetch_locations()
    nb = fetch_sample_measurements()
    report(
        is_healthy=ok,
        sensors=sensors_data,
        locations=locations_data,
        nb_measures=nb,
    )


smartcity_hook_health_check()
