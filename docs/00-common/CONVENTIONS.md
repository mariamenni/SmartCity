# Conventions communes

## Airflow
- naming DAG: `smartcity_<domaine>_<action>_<frequence>`
- tags obligatoires: `smartcity`, domaine, jalon (`j1` ou `j2`)
- `catchup=False` sur tous les DAGs sans exception
- `max_active_runs=1` sur tous les DAGs batch et consumer
- aucun appel reseau, acces DB ou import lourd au top-level des fichiers DAG
- Python 3.12 pour tous les DAGs
- TimescaleDB 2.26.2 (PostgreSQL 18) pour le DWH time-series (conteneur `timescaledb`, port interne 5432, expose 5433)
- MinIO RELEASE.2025-09-07 pour le stockage objet
- Grafana 13.0.0 pour les dashboards
- Kafka (Confluent Platform 8.1.2, mode KRaft) reserve a la partie J2 (streaming consumer)
- XCom = reference uniquement (chemin S3, compteur, statut) - jamais des donnees

## Connexions Airflow
- `sensor_api` : HTTP vers `sensor-simulator:8000`
- `smartcity_timescaledb` : PostgreSQL vers `timescaledb:5432` (port interne Docker)
- `minio_local` : S3 compatible vers `minio:9000`

## Commits
- `feat: <description>`
- `fix: <description>`
- `docs: <description>`
- `test: <description>`
- `refactor: <description>`

## Git
- branche base: `main` (squelette commun, protegee)
- integration: `dev`
- une branche de feature par personne
- pas de push force sur `main`

## Organisation des fichiers
- Frederic: `plugins/hooks/`, `tests/frederic/`, `docs/01-frederic-hook/`
- Ikhlas: `dags/ikhlas/`, `sql/ikhlas/`, `tests/ikhlas/`, `docs/02-ikhlas-dimensions/`
- Narcisse: `dags/narcisse/`, `sql/narcisse/`, `tests/narcisse/`, `docs/03-narcisse-batch-ingest/`
- Maria: `dags/maria/`, `sql/maria/`, `tests/maria/`, `docs/04-maria-alerting/`
- Gills: `dags/gills/`, `sql/gills/`, `tests/gills/`, `docs/05-gills-consumer/`
- Infrastructure commune: `sql/init/`, `sensor-simulator/`, `docker-compose.override.yaml`
