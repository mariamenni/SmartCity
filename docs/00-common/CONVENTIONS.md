# Conventions communes

## Airflow
- Naming DAG : `smartcity_<domaine>_<action>_<frequence>`
- Tags obligatoires : `smartcity`, domaine, jalon (`j1` ou `j2`)
- `catchup=False` sur tous les DAGs sans exception
- `max_active_runs=1` sur tous les DAGs batch et consumer
- Aucun appel réseau, accès DB ou import lourd au top-level des fichiers DAG — tout dans les `@task`
- Python 3.12 pour tous les DAGs
- TimescaleDB 2.26.2 (PostgreSQL 18) — conteneur `timescaledb`, port interne 5432, exposé 5433
- MinIO RELEASE.2025-09-07 pour le stockage objet
- Grafana 13.0.0 pour les dashboards
- XCom = référence uniquement (chemin S3, compteur, statut) — jamais des données volumineuses

## Connexions Airflow

| Conn Id | Type | Host | Port | Détails |
|---------|------|------|------|---------|
| `sensor_api` | HTTP | `sensor-simulator` | `8000` | Interne Docker |
| `smartcity_timescaledb` | Postgres | `timescaledb` | `5432` | schema=`smartcity` |
| `minio_local` | Amazon S3 | — | — | extra: `{"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}` |

> **Note** : le sensor-simulator est accessible en externe sur le port `8100` (mapping `8100:8000`).

## API réelle (sensor-simulator)

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/health` | GET | `{"status": "healthy", "sensors_count": N}` |
| `/api/v1/sensors` | GET | Liste de tous les capteurs (`id` int, `status`, `type`, lat/lon) |
| `/api/v1/readings/{sensor_id}` | GET | Lectures d'un capteur (`sensor_id`, `value`, `unit`, `timestamp`) |
| `/api/v1/readings` | POST | Créer une lecture |
| `/api/v1/metrics` | GET | Métriques agrégées ville |
| `/api/v1/metrics/summary` | GET | Résumé par type de capteur |

## Idempotence
- `fact_measurement` : `ON CONFLICT DO NOTHING` (clé : `ts`, `sensor_id`)
- `dim_location` / `dim_sensor` : `ON CONFLICT DO UPDATE` (upsert)
- `fact_alert` : UUID généré par la DB, pas de duplication de clé

## Commits
- `feat: <description>`
- `fix: <description>`
- `docs: <description>`
- `test: <description>`
- `refactor: <description>`

## Git
- Branche base : `main` (squelette commun, protégée)
- Intégration : `dev`
- Une branche de feature par personne
- Pas de push force sur `main`

## Organisation des fichiers
- Frederic : `plugins/hooks/`, `tests/frederic/`, `docs/01-frederic-hook/`
- Ikhlas : `dags/smartcity_sensors_dims_refresh_daily.py`, `sql/ikhlas/`, `tests/ikhlas/`, `docs/02-ikhlas-dimensions/`
- Narcisse : `dags/smartcity_measurements_batch_ingest.py`, `sql/narcisse/`, `tests/narcisse/`, `docs/03-narcisse-batch-ingest/`
- Maria : `dags/smartcity_alert_check_batch.py`, `sql/maria/`, `tests/maria/`, `docs/04-maria-alerting/`
- Gills : `dags/smartcity_measurements_consumer_minutely.py`, `sql/gills/`, `tests/gills/`, `docs/05-gills-consumer/`
- Infrastructure commune : `sql/init/`, `docker-compose.override.yaml`
