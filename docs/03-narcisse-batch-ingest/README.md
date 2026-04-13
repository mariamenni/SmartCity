# P3 — DAG Batch Ingest 15 min

**Responsable** : Narcisse Cabrel TSAFACK FOUEGAP  
**Fichier** : `dags/smartcity_measurements_batch_ingest.py`  
**Tests** : `tests/narcisse/test_smartcity_measurements_batch_ingest.py` (11 tests)

---

## Description

Ce DAG collecte toutes les mesures sur une fenêtre glissante de 15 minutes, les archive dans MinIO, puis les charge dans TimescaleDB.  
Il joue le rôle de **filet de sécurité** par rapport au consumer P5 : si P5 manque un cycle, P3 récupère les mesures. Le `ON CONFLICT DO NOTHING` garantit l'absence de doublons.

## Schedule

`*/15 * * * *` — toutes les 15 minutes.

## Flux des tâches

```
extract_measurements  →  stage_raw  →  load_timescaledb
```

| Tâche | Description |
|-------|-------------|
| `extract_measurements` | Poll l'API pour tous les capteurs `status=active`, normalise en `{ts, sensor_id, value, unit}` |
| `stage_raw` | Sérialise le batch JSON dans MinIO : `batch/{run_ts}.json` |
| `load_timescaledb` | Lit depuis MinIO, filtre via `_filter_valid_records()`, INSERT `ON CONFLICT DO NOTHING` |

## Normalisation des champs

L'API retourne `timestamp` — renommé `ts` pour correspondre au schéma `fact_measurement`.  
Le `sensor_id` est converti en `str` pour correspondre aux insertions de P2/P5.

```python
{
    "ts":        reading["timestamp"],   # clé renommée
    "sensor_id": str(reading["sensor_id"]),
    "value":     reading["value"],
    "unit":      reading["unit"],
}
```

## Stockage MinIO

| Bucket | Préfixe | Contenu |
|--------|---------|---------|
| `smartcity` | `batch/` | Un fichier JSON par exécution du DAG (`batch/2026-04-13T12:00:00.json`) |

## Helper testable

```python
def _filter_valid_records(records: list[dict]) -> list[dict]:
    """Garde uniquement les enregistrements avec ts, sensor_id, value et unit."""
```

Un enregistrement est valide si `ts`, `sensor_id`, `unit` sont non-vides **et** `value` n'est pas `None` (la valeur `0.0` est valide).

## Idempotence

`INSERT INTO fact_measurement ... ON CONFLICT DO NOTHING`

## Connexions requises

- `sensor_api`
- `minio_local`
- `smartcity_timescaledb`

## Différence avec P5 (consumer minutely)

| | P3 — Batch | P5 — Consumer |
|-|-----------|---------------|
| Fréquence | 15 min | 1 min |
| Rôle | Filet de sécurité | Temps réel |
| Archivage | `batch/` | `raw/` + `clean/` |
| Transformation | Filtre basique | Déduplication + contrôle latence |

## Tests

```bash
python -m pytest tests/narcisse/ -v
```

| Classe | # | Scope |
|--------|---|-------|
| `TestFilterValidRecords` | 9 | Champs manquants, value=0 valide, chaîne vide invalide, liste vide, mixte |
| `TestSensorIdNormalization` | 2 | int→str, alias clé `timestamp`→`ts` |
