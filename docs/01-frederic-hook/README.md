# P1 — Hook API commun

**Responsable** : Frederic FERNANDES DA COSTA  
**Fichier** : `plugins/hooks/sensor_api_hook.py`  
**Tests** : `tests/frederic/test_sensor_api_hook.py` (13 tests)

---

## Description

`SensorAPIHook` est le point d'entrée unique vers l'API sensor-simulator pour tous les DAGs du projet. Il étend `HttpHook` d'Airflow et utilise la connexion `sensor_api`.

## API réelle

L'API est fournie par l'image Docker `arnauropero/smart-cities-api` (FastAPI, données de Barcelone).  
Accès externe : `http://localhost:8100`  
Accès interne Docker : `http://sensor-simulator:8000`

### Capteurs disponibles

| id | name | type | status |
|----|------|------|--------|
| 1 | Temperature Sensor - Sagrada Familia | temperature | active |
| 2 | Air Quality Sensor - Eixample | air_quality | active |
| 3 | Traffic Flow Sensor - Las Ramblas | traffic_flow | maintenance |

### Format des réponses

**`GET /api/v1/sensors`** :
```json
[
  {
    "id": 1,
    "name": "Temperature Sensor - Sagrada Familia",
    "type": "temperature",
    "latitude": 41.4036,
    "longitude": 2.1744,
    "status": "active",
    "description": "...",
    "created_at": "...",
    "last_reading": "..."
  }
]
```

**`GET /api/v1/readings/{sensor_id}`** :
```json
[
  {
    "sensor_id": 1,
    "value": 22.5,
    "unit": "celsius",
    "timestamp": "2026-04-13T08:54:03.039009"
  }
]
```

**`GET /health`** :
```json
{"status": "healthy", "timestamp": "...", "sensors_count": 3}
```

## Méthodes du hook

| Méthode | Endpoint appelé | Retour | Description |
|---------|----------------|--------|-------------|
| `health_check()` | `GET /health` | `bool` | `True` si `status == "healthy"` |
| `get_sensors()` | `GET /api/v1/sensors` | `list[dict]` | Tous les capteurs |
| `get_readings(sensor_id)` | `GET /api/v1/readings/{sensor_id}` | `list[dict]` | Lectures d'un capteur |
| `get_metrics()` | `GET /api/v1/metrics` | `dict` | Métriques agrégées ville |
| `get_metrics_summary()` | `GET /api/v1/metrics/summary` | `dict` | Résumé par type |

## Connexion Airflow

| Champ | Valeur |
|-------|--------|
| Conn Id | `sensor_api` |
| Conn Type | `HTTP` |
| Host | `sensor-simulator` |
| Port | `8000` |

## Tests

```bash
python -m pytest tests/frederic/ -v
```

| Classe | # | Description |
|--------|---|-------------|
| `TestHealthCheck` | 3 | OK / KO / exception |
| `TestGetSensors` | 3 | liste, vide, endpoint correct `/api/v1/sensors` |
| `TestGetReadings` | 4 | liste, vide, endpoint correct, sensor_id en string |
| `TestGetMetrics` | 1 | retourne un dict |
| `TestGetMetricsSummary` | 1 | retourne un dict |
| `TestApiGetError` | 1 | exception propagée |

## Utilisation dans les DAGs

```python
# Import lazy dans une @task Airflow
from hooks.sensor_api_hook import SensorAPIHook

hook = SensorAPIHook()                          # conn_id=sensor_api
sensors = hook.get_sensors()                    # list[dict]
readings = hook.get_readings(sensor_id=1)       # list[dict]
ok = hook.health_check()                        # bool
```
