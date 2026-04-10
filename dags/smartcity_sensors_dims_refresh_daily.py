from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="smartcity_sensors_dims_refresh_daily",
    description="Refresh quotidien des dimensions dim_location et dim_sensor depuis l'API SmartCity",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "dimensions", "batch", "j1"],
)
def smartcity_sensors_dims_refresh_daily():
    @task()
    def extract_locations() -> list[dict]:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        locations = hook.get_locations()

        if not isinstance(locations, list):
            raise ValueError("Format inattendu pour /locations")

        print(f"{len(locations)} locations récupérées depuis l'API")
        return locations

    @task()
    def load_locations(locations: list[dict]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not locations:
            print("Aucune location à charger")
            return 0

        pg_hook = PostgresHook(postgres_conn_id="smartcity_timescaledb")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO dim_location (
                location_id,
                district,
                latitude,
                longitude,
                zone_type
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (location_id)
            DO UPDATE SET
                district = EXCLUDED.district,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                zone_type = EXCLUDED.zone_type
        """

        rows = []
        for loc in locations:
            rows.append(
                (
                    int(loc["location_id"]),
                    loc["district"],
                    float(loc["latitude"]),
                    float(loc["longitude"]),
                    loc["zone_type"],
                )
            )

        cur.executemany(upsert_sql, rows)
        conn.commit()

        cur.close()
        conn.close()

        print(f"{len(rows)} locations upsertées dans dim_location")
        return len(rows)

    @task()
    def extract_sensors() -> list[dict]:
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()

        if not isinstance(sensors, list):
            raise ValueError("Format inattendu pour /sensors")

        print(f"{len(sensors)} capteurs récupérés depuis l'API")
        return sensors

    @task()
    def load_sensors(sensors: list[dict]) -> int:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        if not sensors:
            print("Aucun capteur à charger")
            return 0

        pg_hook = PostgresHook(postgres_conn_id="smartcity_timescaledb")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        upsert_sql = """
            INSERT INTO dim_sensor (
                sensor_id,
                type,
                location_id,
                installed_date,
                is_active
            )
            VALUES (%s, %s, %s, CURRENT_DATE, %s)
            ON CONFLICT (sensor_id)
            DO UPDATE SET
                type = EXCLUDED.type,
                location_id = EXCLUDED.location_id,
                is_active = EXCLUDED.is_active
        """

        rows = []
        for sensor in sensors:
            rows.append(
                (
                    sensor["sensor_id"],
                    sensor["type"],
                    int(sensor["location_id"]),
                    bool(sensor["is_active"]),
                )
            )

        cur.executemany(upsert_sql, rows)
        conn.commit()

        cur.close()
        conn.close()

        print(f"{len(rows)} capteurs upsertés dans dim_sensor")
        return len(rows)

    @task()
    def report(nb_locations: int, nb_sensors: int) -> None:
        print("=" * 60)
        print("RAPPORT — Refresh des dimensions SmartCity")
        print("=" * 60)
        print(f"dim_location : {nb_locations} lignes upsertées")
        print(f"dim_sensor   : {nb_sensors} lignes upsertées")
        print("Refresh terminé avec succès")
        print("=" * 60)

    locations = extract_locations()
    sensors = extract_sensors()

    nb_locations = load_locations(locations)
    nb_sensors = load_sensors(sensors)

    report(nb_locations, nb_sensors)


smartcity_sensors_dims_refresh_daily()