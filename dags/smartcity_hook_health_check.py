from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="smartcity_hook_health_check",
    description="Validation du SensorAPIHook : health, sensors, locations, measurements",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "hook", "validation", "j1"],
)
def smartcity_hook_health_check():

    @task()
    def health_check() -> bool:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        is_healthy = hook.health_check()

        if not is_healthy:
            raise RuntimeError(
                "sensor-simulator ne répond pas. Vérifie le service et la connexion Airflow sensor_api."
            )

        print("sensor-simulator est UP et répond correctement.")
        return is_healthy

    @task()
    def fetch_sensors() -> list[dict]:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()

        print(f"{len(sensors)} capteurs récupérés")
        for s in sensors:
            status = "actif" if s.get("is_active") else "inactif"
            print(f"{s['sensor_id']} | {s['type']} | {status}")

        return sensors

    @task()
    def fetch_locations() -> list[dict]:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        locations = hook.get_locations()

        print(f"{len(locations)} locations récupérées")
        for loc in locations:
            print(f"{loc['location_id']} | {loc['district']} | {loc['zone_type']}")

        return locations

    @task()
    def fetch_sample_measurements() -> int:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        since = (datetime.utcnow() - timedelta(minutes=15)).isoformat()
        measurements = hook.get_measurements(since=since, limit=100)

        print(f"{len(measurements)} mesures récupérées")
        if measurements:
            sample = measurements[0]
            print(
                f"Exemple : sensor={sample['sensor_id']} "
                f"type={sample['type']} value={sample['value']} {sample['unit']} "
                f"ts={sample['ts']}"
            )

        return len(measurements)

    @task()
    def report(
        is_healthy: bool,
        sensors: list[dict],
        locations: list[dict],
        nb_measures: int,
    ) -> None:
        active = sum(1 for s in sensors if s.get("is_active"))
        inactive = len(sensors) - active

        print("=" * 60)
        print("RAPPORT DE VALIDATION — SensorAPIHook")
        print("=" * 60)
        print(f"API Health        : {'OK' if is_healthy else 'KO'}")
        print(f"Locations         : {len(locations)}")
        print(f"Capteurs total    : {len(sensors)}")
        print(f"Capteurs actifs   : {active}")
        print(f"Capteurs inactifs : {inactive}")
        print(f"Mesures 15 min    : {nb_measures}")
        print("=" * 60)
        print("Hook validé.")
        print("=" * 60)

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