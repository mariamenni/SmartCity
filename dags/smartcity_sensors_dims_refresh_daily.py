# =============================================================================
# DAG  — Refresh des dimensions capteurs (P2 — Ikhlas)
#
# Ce DAG synchronise quotidiennement les dimensions depuis l'API :
#   1. extract_from_api     → GET /api/v1/sensors, retourne la liste brute
#   2. upsert_dimensions    → UPSERT dans dim_location + dim_sensor
#
# dim_location : une entrée par capteur (localisation GPS + nom extrait)
# dim_sensor   : sensor_id = str(api_id) pour correspondre aux fact_measurement
#                insérés par P5 (consumer minutely)
#
# Schedule : @daily
# Connexions : sensor_api, smartcity_timescaledb
# =============================================================================
from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, task


def _extract_district(sensor_name: str) -> str:
    """Extrait le nom du district depuis le nom du capteur ('Type - District')."""
    parts = sensor_name.split(" - ", 1)
    return parts[1].strip() if len(parts) == 2 else sensor_name.strip()


@dag(
    dag_id="smartcity_sensors_dims_refresh_daily",
    description="P2 — Refresh quotidien des dimensions (dim_location + dim_sensor)",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smartcity", "dimensions", "j1"],
)
def smartcity_sensors_dims_refresh_daily():

    @task()
    def extract_from_api() -> list[dict]:
        """Récupère la liste complète des capteurs depuis l'API."""
        from hooks.sensor_api_hook import SensorAPIHook

        hook = SensorAPIHook()
        sensors = hook.get_sensors()
        print(f"extract_from_api: {len(sensors)} capteurs récupérés")
        return sensors

    @task()
    def upsert_dimensions(sensors: list[dict]) -> dict:
        """Upsert dans dim_location et dim_sensor depuis les données API.

        dim_location → une entrée par capteur (location_id = 'LOC-{id}')
        dim_sensor   → sensor_id = str(id), correspondant aux enregistrements
                       insérés dans fact_measurement par le consumer P5.
        """
        import psycopg2
        from airflow.hooks.base import BaseHook

        conn_info = BaseHook.get_connection("smartcity_timescaledb")
        conn = psycopg2.connect(
            host=conn_info.host,
            port=int(conn_info.port or 5432),
            dbname=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )

        nb_loc = 0
        nb_sensor = 0

        try:
            with conn:
                with conn.cursor() as cur:
                    for s in sensors:
                        sensor_id   = str(s["id"])
                        location_id = f"LOC-{sensor_id}"
                        district    = _extract_district(s.get("name", sensor_id))
                        lat         = float(s.get("latitude", 0))
                        lon         = float(s.get("longitude", 0))
                        is_active   = s.get("status", "active") == "active"

                        # Upsert dim_location
                        cur.execute(
                            """
                            INSERT INTO dim_location
                                (location_id, district, latitude, longitude, zone_type)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (location_id) DO UPDATE
                                SET district  = EXCLUDED.district,
                                    latitude  = EXCLUDED.latitude,
                                    longitude = EXCLUDED.longitude
                            """,
                            (location_id, district, lat, lon, "urban"),
                        )
                        nb_loc += 1

                        # Upsert dim_sensor
                        cur.execute(
                            """
                            INSERT INTO dim_sensor
                                (sensor_id, type, location_id, installed_date, is_active)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (sensor_id) DO UPDATE
                                SET type      = EXCLUDED.type,
                                    is_active = EXCLUDED.is_active
                            """,
                            (
                                sensor_id,
                                s.get("type", "unknown"),
                                location_id,
                                s.get("created_at", datetime.utcnow().date()),
                                is_active,
                            ),
                        )
                        nb_sensor += 1

            print(f"upsert_dimensions: {nb_loc} locations, {nb_sensor} capteurs upsertés")
            return {"nb_locations": nb_loc, "nb_sensors": nb_sensor}
        finally:
            conn.close()

    sensors_data = extract_from_api()
    upsert_dimensions(sensors_data)


smartcity_sensors_dims_refresh_daily()

