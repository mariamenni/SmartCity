"""
sensor-simulator — API Flask IoT SmartCity

Endpoints :
  GET  /health                                → état du service
  GET  /sensors?active=true|false             → liste des capteurs
  GET  /sensors/<sensor_id>                   → un capteur par ID
  GET  /locations                             → liste des locations
  GET  /measurements?since=&type=&limit=      → mesures sur fenêtre
  GET  /measurements/latest                   → dernière mesure par capteur actif
  POST /inject  body: {sensor_id, type, value}→ injection manuelle (test latence)

Types de mesures (selon cahier des charges §5) :
  air_quality_pm25  μg/m³    seuil warning:25  critique:50
  air_quality_no2   μg/m³    seuil warning:100 critique:200
  traffic_density   véh/h    seuil critique:2000
  noise_level       dB       seuil warning:65  critique:80
  temperature       °C       (référence climatique, sans seuil réglementaire)
"""
from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import random

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Données de référence — alignées avec le seed SQL (init-db/03-seed.sql)
# ---------------------------------------------------------------------------
LOCATIONS = [
    {"location_id": 1, "district": "Batignolles", "latitude": 48.8862, "longitude": 2.3195, "zone_type": "residential"},
    {"location_id": 2, "district": "Nation",      "latitude": 48.8484, "longitude": 2.3965, "zone_type": "commercial"},
    {"location_id": 3, "district": "Belleville",  "latitude": 48.8700, "longitude": 2.3800, "zone_type": "residential"},
    {"location_id": 4, "district": "La Défense",  "latitude": 48.8924, "longitude": 2.2384, "zone_type": "industrial"},
    {"location_id": 5, "district": "Châtelet",    "latitude": 48.8584, "longitude": 2.3470, "zone_type": "commercial"},
]

SENSORS = [
    {"sensor_id": "S-001", "type": "air_quality_pm25", "unit": "μg/m³", "location_id": 1, "is_active": True},
    {"sensor_id": "S-002", "type": "air_quality_no2",  "unit": "μg/m³", "location_id": 1, "is_active": True},
    {"sensor_id": "S-003", "type": "traffic_density",  "unit": "véh/h", "location_id": 1, "is_active": True},
    {"sensor_id": "S-004", "type": "noise_level",      "unit": "dB",    "location_id": 2, "is_active": True},
    {"sensor_id": "S-005", "type": "temperature",      "unit": "°C",    "location_id": 2, "is_active": True},
    {"sensor_id": "S-006", "type": "air_quality_pm25", "unit": "μg/m³", "location_id": 3, "is_active": True},
    {"sensor_id": "S-007", "type": "air_quality_no2",  "unit": "μg/m³", "location_id": 3, "is_active": True},
    {"sensor_id": "S-008", "type": "traffic_density",  "unit": "véh/h", "location_id": 4, "is_active": True},
    {"sensor_id": "S-009", "type": "noise_level",      "unit": "dB",    "location_id": 4, "is_active": True},
    {"sensor_id": "S-010", "type": "temperature",      "unit": "°C",    "location_id": 5, "is_active": True},
    {"sensor_id": "S-011", "type": "air_quality_pm25", "unit": "μg/m³", "location_id": 5, "is_active": False},
    {"sensor_id": "S-012", "type": "noise_level",      "unit": "dB",    "location_id": 2, "is_active": False},
]

# Plages normales de génération aléatoire (en-dessous des seuils critiques)
_NORMAL_RANGES = {
    "air_quality_pm25": (2.0,  20.0),
    "air_quality_no2":  (10.0, 80.0),
    "traffic_density":  (200,  1500),
    "noise_level":      (40.0, 62.0),
    "temperature":      (5.0,  35.0),
}

# Mémoire des injections manuelles (endpoint /inject)
_injected: list[dict] = []


def _sim_value(sensor_type: str) -> float:
    lo, hi = _NORMAL_RANGES.get(sensor_type, (0.0, 100.0))
    return round(random.uniform(lo, hi), 3)


def _unit(sensor_type: str) -> str:
    unit_map = {
        "air_quality_pm25": "μg/m³",
        "air_quality_no2":  "μg/m³",
        "traffic_density":  "véh/h",
        "noise_level":      "dB",
        "temperature":      "°C",
    }
    return unit_map.get(sensor_type, "")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "sensor-simulator",
        "timestamp": datetime.utcnow().isoformat(),
    })


@app.route("/locations")
def get_locations():
    return jsonify(LOCATIONS)


@app.route("/sensors")
def get_sensors():
    active_param = request.args.get("active", "").lower()
    if active_param == "true":
        result = [s for s in SENSORS if s["is_active"]]
    elif active_param == "false":
        result = [s for s in SENSORS if not s["is_active"]]
    else:
        result = SENSORS
    return jsonify(result)


@app.route("/sensors/<sensor_id>")
def get_sensor(sensor_id: str):
    sensor = next((s for s in SENSORS if s["sensor_id"] == sensor_id), None)
    if not sensor:
        return jsonify({"error": f"Sensor '{sensor_id}' not found"}), 404
    return jsonify(sensor)


@app.route("/measurements")
def get_measurements():
    """
    Paramètres :
      since  (ISO datetime) — fenêtre de début  ex: 2026-04-10T14:00:00
      type   (str)          — filtre par type   ex: air_quality_pm25
      limit  (int)          — nb max de mesures (défaut 100)
    """
    since_str = request.args.get("since")
    sensor_type = request.args.get("type")
    limit = request.args.get("limit", 100, type=int)

    # Décoder la fenêtre temporelle
    if since_str:
        try:
            since_dt = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
        except ValueError:
            return jsonify({"error": "Invalid 'since' format, use ISO 8601"}), 400
    else:
        since_dt = datetime.utcnow() - timedelta(minutes=15)

    # Filtrer les capteurs actifs (+ filtre par type si spécifié)
    active_sensors = [
        s for s in SENSORS
        if s["is_active"] and (sensor_type is None or s["type"] == sensor_type)
    ]

    now = datetime.utcnow()
    window_minutes = max(1, int((now - since_dt.replace(tzinfo=None)).total_seconds() / 60))

    measurements = []
    for sensor in active_sensors:
        # 1 mesure par minute dans la fenêtre (max `limit` au total)
        for i in range(min(window_minutes, limit)):
            measurements.append({
                "sensor_id": sensor["sensor_id"],
                "type":      sensor["type"],
                "value":     _sim_value(sensor["type"]),
                "unit":      sensor["unit"],
                "ts":        (now - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            })

    # Inclure aussi les injections manuelles dans la fenêtre
    for inj in _injected:
        try:
            inj_dt = datetime.fromisoformat(inj["ts"].replace("Z", ""))
        except (KeyError, ValueError):
            continue
        if inj_dt >= since_dt.replace(tzinfo=None):
            if sensor_type is None or inj.get("type") == sensor_type:
                measurements.append(inj)

    return jsonify(measurements[:limit])


@app.route("/measurements/latest")
def get_latest_measurements():
    """Retourne la dernière valeur simulée pour chaque capteur actif."""
    now = datetime.utcnow()
    return jsonify([
        {
            "sensor_id": s["sensor_id"],
            "type":      s["type"],
            "value":     _sim_value(s["type"]),
            "unit":      s["unit"],
            "ts":        now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for s in SENSORS if s["is_active"]
    ])


@app.route("/inject", methods=["POST"])
def inject():
    """
    Injection manuelle d'une mesure pour tester la latence alerte.
    Body JSON : {"sensor_id": "S-001", "type": "air_quality_pm25", "value": 180}

    Utilisé par le formateur pour le test de latence (cahier des charges §12.2) :
      curl -X POST http://localhost:5001/inject \\
           -H "Content-Type: application/json" \\
           -d '{"sensor_id":"S-001","type":"air_quality_pm25","value":180}'
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "JSON body required"}), 400

    sensor_id = data.get("sensor_id")
    sensor_type = data.get("type")
    value = data.get("value")

    if not all([sensor_id, sensor_type, value is not None]):
        return jsonify({"error": "sensor_id, type and value are required"}), 400

    record = {
        "sensor_id": sensor_id,
        "type":      sensor_type,
        "value":     float(value),
        "unit":      _unit(sensor_type),
        "ts":        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "injected":  True,
    }
    _injected.append(record)
    return jsonify({"status": "injected", "record": record}), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
