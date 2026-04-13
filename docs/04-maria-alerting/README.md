# P4 — DAG Alerting 15 min

**Responsable** : Maria MENNI  
**Fichier** : `dags/smartcity_alert_check_batch.py`  
**Tests** : `tests/maria/test_smartcity_alert_check_batch.py` (21 tests)

---

## Description

Ce DAG analyse les mesures récentes de `fact_measurement` et génère des alertes dans `fact_alert` lorsque des seuils sont dépassés.  
Il s'exécute toutes les 15 minutes.

## Schedule

`*/15 * * * *` — toutes les 15 minutes.

## Flux des tâches

```
read_recent_measurements  →  detect_thresholds  →  write_alerts
```

| Tâche | Description |
|-------|-------------|
| `read_recent_measurements` | SQL : `fact_measurement LEFT JOIN dim_sensor` sur les 15 dernières minutes |
| `detect_thresholds` | Itère les mesures via `_check_violation()`, produit la liste des violations |
| `write_alerts` | INSERT bulk dans `fact_alert` (UUID auto-généré par la DB) |

## Seuils de détection

Définis dans la constante `THRESHOLDS` :

| Type de capteur | Seuil warning | Seuil critical | Unité |
|----------------|---------------|----------------|-------|
| `temperature` | > 35 °C | > 40 °C | celsius |
| `air_quality` | > 3 | > 4 | indice 0-5 |
| `traffic_flow` | > 80 % | > 95 % | % occupation |
| `humidity` | > 85 % | > 95 % | % |
| `noise_level` | > 70 dB | > 85 dB | dB |

### Logique de détection

```python
critical  si valeur >= seuil_critical
warning   si seuil_warning <= valeur < seuil_critical
aucune    si valeur < seuil_warning ou type inconnu
```

## Helper testable

```python
def _check_violation(
    sensor_type: str, value: float
) -> tuple[str, float] | None:
    """
    Retourne (severity, threshold) si violation, None sinon.
    severity: 'warning' | 'critical'
    """
```

## Schéma cible — `fact_alert`

| Colonne | Source |
|---------|--------|
| `alert_id` | UUID généré automatiquement par la DB |
| `ts` | `ts` de la mesure |
| `sensor_id` | `sensor_id` de la mesure |
| `severity` | `"warning"` ou `"critical"` |
| `value` | valeur mesurée |
| `threshold` | seuil franchi |

## Idempotence

Chaque INSERT dans `fact_alert` génère un UUID distinct — une relance produit des doublons d'alerte.  
Pour éviter cela, la fenêtre de lecture est strictement bornée à 15 min (`NOW() - INTERVAL '15 minutes'`).

## Connexions requises

- `smartcity_timescaledb`

## Tests

```bash
python -m pytest tests/maria/ -v
```

| Classe | # | Scope |
|--------|---|-------|
| `TestCheckViolation` | 13 | Normal / warning / critical pour temperature, air_quality, traffic_flow, noise_level, humidity, type inconnu |
| `TestThresholds` | 3 | Structure THRESHOLDS : 2 valeurs par type, warning < critical, types attendus présents |
| `TestDetectThresholdsLogic` | 5 | Aucune violation, 1 warning, 1 critical, mixte, type inconnu ignoré |
