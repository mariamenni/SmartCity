# P5 — DAG Consumer micro-batch 1 min

**Responsable** : Gills Daryl KETCHA NZOUNDJI JIEPMOU  
**Fichier** : `dags/smartcity_measurements_consumer_minutely.py`  
**Tests** : `tests/gills/test_smartcity_measurements_consumer_minutely.py` (13 tests)

---

## Description

Ce DAG simule un consumer temps réel (sans Kafka) via un micro-batch exécuté chaque minute.  
Il poll l'API, valide et transforme les mesures, puis les insère dans TimescaleDB avec archivage dans MinIO.

## Schedule

`*/1 * * * *` — chaque minute.

## Flux des tâches

```
poll_api  →  transform  →  flush_to_timescaledb
```

| Tâche | XCom out | Description |
|-------|----------|-------------|
| `poll_api` | chemin S3 `raw/` | Interroge SensorAPIHook (`get_sensors` + `get_readings` par capteur), stocke le brut dans MinIO `raw/` |
| `transform` | chemin S3 `clean/` | Lit le brut MinIO, valide les champs, déduplique `(ts, sensor_id)`, contrôle la latence (> 5 min → warning), stocke nettoyé dans `clean/` |
| `flush_to_timescaledb` | nb insérés (int) | Lit le nettoyé MinIO, `INSERT ON CONFLICT DO NOTHING` dans `fact_measurement` |

## Stockage MinIO

| Bucket | Préfixe | Contenu |
|--------|---------|---------|
| `smartcity` | `raw/` | Données brutes API avant validation |
| `smartcity` | `clean/` | Données validées et dédupliquées |

## Logique de validation (`transform`)

Un enregistrement est conservé si :
- Champs obligatoires présents : `ts` (ou `timestamp`), `sensor_id`, `value`, `unit`
- `value` est numérique (`int` ou `float`)
- Pas de doublon `(ts, sensor_id)` dans le batch courant

Un avertissement est loggué si la latence dépasse **5 minutes** (`LATENCY_WARN_SECONDS = 300`).

## Constantes

```python
MINIO_BUCKET = "smartcity"
LATENCY_WARN_SECONDS = 300  # 5 minutes
```

## Idempotence

`INSERT INTO fact_measurement ... ON CONFLICT DO NOTHING`  
La déduplication dans `transform` évite les doublons au sein du même batch.

## Connexions requises

- `sensor_api`
- `minio_local`
- `smartcity_timescaledb`

## Tests

```bash
python -m pytest tests/gills/ -v
```

| Classe | # | Scope |
|--------|---|-------|
| `TestPollApiLogic` | 3 | Stockage MinIO, skip capteur inactif, continue sur erreur individuelle |
| `TestTransformLogic` | 7 | Enregistrement valide, champs manquants, valeur non-numérique, déduplication, latence, ts non parseable, batch vide |
| `TestFlushLogic` | 3 | Insertion + compteur, batch vide → 0, fermeture connexion |

## Résultats de validation — 13 avril 2026

| Élément | Statut | Détail |
|--------|--------|--------|
| Tests unitaires | OK 13 passés | `pytest tests/gills/ -v` |
| DAG `smartcity_measurements_consumer_minutely` | OK Succès | Dernière exécution : 2026-04-13 12:29:00 |
| Fichiers MinIO `raw/` et `clean/` | OK créés | Archivage brut + nettoyé visible dans MinIO |
| Courbe température Grafana | OK | Capteur `"1"` trace la courbe (~19–27 °C sur 24 h) |
| Déduplication `(ts, sensor_id)` | OK | `ON CONFLICT DO NOTHING` vérifié sur re-run |
