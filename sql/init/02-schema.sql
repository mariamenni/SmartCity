-- Schema SmartCity : dimensions, faits et alertes.
-- Idempotent : toutes les instructions utilisent IF NOT EXISTS ou ON CONFLICT.
-- Ordre obligatoire : dim_location -> dim_sensor -> fact_measurement -> fact_alert.

-- -----------------------------------------------------------------------
-- Dimensions
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_location (
    location_id  VARCHAR(20)  PRIMARY KEY,
    district     VARCHAR(100) NOT NULL,
    latitude     DECIMAL(9,6) NOT NULL,
    longitude    DECIMAL(9,6) NOT NULL,
    zone_type    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_sensor (
    sensor_id      VARCHAR(50) PRIMARY KEY,
    type           VARCHAR(50) NOT NULL,
    location_id    VARCHAR(20) REFERENCES dim_location(location_id),
    installed_date DATE,
    is_active      BOOLEAN NOT NULL DEFAULT TRUE
);

-- -----------------------------------------------------------------------
-- Faits - mesures (hypertable TimescaleDB)
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_measurement (
    ts         TIMESTAMPTZ  NOT NULL,
    sensor_id  VARCHAR(50)  NOT NULL REFERENCES dim_sensor(sensor_id),
    value      DECIMAL(10,3) NOT NULL,
    unit       VARCHAR(20)  NOT NULL,
    PRIMARY KEY (ts, sensor_id)
);

-- Conversion en hypertable partitionnee par jour.
-- if_not_exists=TRUE rend l'appel idempotent.
SELECT create_hypertable(
    'fact_measurement',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Index optimise pour les requetes Grafana et le DAG d'alerting.
CREATE INDEX IF NOT EXISTS idx_fact_measurement_sensor_ts
    ON fact_measurement (sensor_id, ts DESC);

-- -----------------------------------------------------------------------
-- Alertes
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_alert (
    alert_id   UUID          DEFAULT gen_random_uuid() PRIMARY KEY,
    ts         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    sensor_id  VARCHAR(50)   REFERENCES dim_sensor(sensor_id),
    severity   VARCHAR(20)   NOT NULL CHECK (severity IN ('warning', 'critical')),
    value      DECIMAL(10,3) NOT NULL,
    threshold  DECIMAL(10,3) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_alert_ts
    ON fact_alert (ts DESC);
