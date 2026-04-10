# Conventions communes

## Airflow
- naming DAG: `smartcity_<domaine>_<action>_<frequence>`
- tags obligatoires: `smartcity`, domaine, jalon
- `catchup=False`
- `max_active_runs=1` pour batch et consumer
- pas de gros appels reseau au top-level des DAGs
- Python 3.12 pour tous les DAGs
- PostgreSQL 16 pour le DWH
- MinIO pour le stockage objet
- pas de Kafka dans cette version du projet

## Git
- branche base: `main`
- integration: `dev`
- une branche par personne
- pas de push force sur `main`

## Organisation des fichiers
- Frederic: `plugins/hooks/`, `tests/frederic/`, `docs/01-frederic-hook/`
- Ikhlas: `dags/ikhlas/`, `sql/ikhlas/`, `tests/ikhlas/`, `docs/02-ikhlas-dimensions/`
- Narcisse: `dags/narcisse/`, `sql/narcisse/`, `tests/narcisse/`, `docs/03-narcisse-batch-ingest/`
- Maria: `dags/maria/`, `sql/maria/`, `tests/maria/`, `docs/04-maria-alerting/`
- Gills: `dags/gills/`, `sql/gills/`, `tests/gills/`, `docs/05-gills-consumer/`
