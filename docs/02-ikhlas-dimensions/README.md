# P2 — DAG Dimensions quotidien

**Responsable** : Ikhlas LAGHMICH  
**Fichier** : `dags/smartcity_sensors_dims_refresh_daily.py`  
**Tests** : `tests/ikhlas/test_smartcity_sensors_dims_refresh_daily.py` (12 tests)

---

## Description

Ce DAG synchronise quotidiennement les tables de dimensions du DWH depuis l'API.  
Il maintient `dim_location` et `dim_sensor` à jour en faisant un **upsert** (ON CONFLICT DO UPDATE).

## Schedule

`@daily` — se déclenche une fois par jour à minuit.

## Flux des tâches

```
extract_from_api  →  upsert_dimensions
```

| Tâche | Description |
|-------|-------------|
| `extract_from_api` | Appelle `SensorAPIHook.get_sensors()` → retourne la liste brute |
| `upsert_dimensions` | Upsert dans `dim_location` puis `dim_sensor` pour chaque capteur |

## Schéma cible

### `dim_location`

| Colonne | Valeur calculée |
|---------|----------------|
| `location_id` | `"LOC-{id}"` (ex: `LOC-1`) |
| `district` | Extrait du nom du capteur via `_extract_district()` : `"Temperature Sensor - Sagrada Familia"` → `"Sagrada Familia"` |
| `latitude` | `float(s.get("latitude", 0))` |
| `longitude` | `float(s.get("longitude", 0))` |
| `zone_type` | `"urban"` (valeur fixe pour les capteurs API) |

### `dim_sensor`

| Colonne | Valeur calculée |
|---------|----------------|
| `sensor_id` | `str(s["id"])` — chaîne pour correspondre aux insertions de P5 |
| `type` | `s.get("type", "unknown")` |
| `location_id` | `"LOC-{id}"` |
| `installed_date` | `s.get("created_at", today)` |
| `is_active` | `True` si `status == "active"`, `False` sinon |

## Helper testable

```python
def _extract_district(sensor_name: str) -> str:
    """Ex: 'Temperature Sensor - Sagrada Familia' → 'Sagrada Familia'"""
    parts = sensor_name.split(" - ", 1)
    return parts[1].strip() if len(parts) == 2 else sensor_name.strip()
```

## Idempotence

Upsert via `ON CONFLICT (sensor_id) DO UPDATE SET ...` — une relance du DAG ne crée jamais de doublon.

## Connexions requises

- `sensor_api`
- `smartcity_timescaledb`

## Tests

```bash
python -m pytest tests/ikhlas/ -v
```

| Classe | # | Scope |
|--------|---|-------|
| `TestExtractDistrict` | 5 | Format standard, sans séparateur, tirets multiples, espaces, chaîne vide |
| `TestExtractFromApi` | 2 | Retour de la liste, réponse vide |
| `TestUpsertDimensionsFieldLogic` | 5 | sensor_id en string, location_id dérivé, is_active vrai/faux, district extrait |

## Résultats de validation — 13 avril 2026

| Élément | Statut | Détail |
|--------|--------|--------|
| Tests unitaires | OK 12 passés | `pytest tests/ikhlas/ -v` |
| DAG `smartcity_sensors_dims_refresh_daily` | OK Succès | Dernière exécution : 2026-04-13 12:27:17 |
| `dim_location` après P2 | OK 3 lignes | LOC-1 Sagrada Familia, LOC-2 Eixample, LOC-3 Las Ramblas |
| `dim_sensor` après P2 | OK 3 lignes | sensor_id `"1"` température, `"2"` air_quality, `"3"` traffic_flow |
