# Répartition finale

| # | Nom | Responsabilité |
|---|-----|----------------|
| 1 | Frederic FERNANDES DA COSTA | Hook API commun |
| 2 | Ikhlas LAGHMICH | DAG dimensions quotidien |
| 3 | Narcisse Cabrel TSAFACK FOUEGAP | DAG batch ingestion 15 min |
| 4 | Maria MENNI | DAG alerting 15 min |
| 5 | Gills Daryl KETCHA NZOUNDJI JIEPMOU | DAG consumer 1 min |

## Fichiers réservés

| Membre | Fichiers |
|--------|----------|
| Frederic (P1) | `plugins/hooks/sensor_api_hook.py`, `tests/frederic/`, `docs/01-frederic-hook/` |
| Ikhlas (P2) | `dags/smartcity_sensors_dims_refresh_daily.py`, `sql/ikhlas/`, `tests/ikhlas/`, `docs/02-ikhlas-dimensions/` |
| Narcisse (P3) | `dags/smartcity_measurements_batch_ingest.py`, `sql/narcisse/`, `tests/narcisse/`, `docs/03-narcisse-batch-ingest/` |
| Maria (P4) | `dags/smartcity_alert_check_batch.py`, `sql/maria/`, `tests/maria/`, `docs/04-maria-alerting/` |
| Gills (P5) | `dags/smartcity_measurements_consumer_minutely.py`, `sql/gills/`, `tests/gills/`, `docs/05-gills-consumer/` |

## Infrastructure commune

Responsabilité partagée du groupe :
- `docker-compose.yaml` (base officielle)
- `docker-compose.override.yaml`
- `sql/init/` (schéma, seed, hypertable, migrations)
- `config/airflow.cfg`
