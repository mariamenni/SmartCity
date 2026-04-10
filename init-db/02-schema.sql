-- =============================================================================
-- 02-schema.sql — Schéma SmartCity
--
-- Tables :
--   dim_location    — lieux géographiques (quartiers, zones)
--   dim_sensor      — référentiel capteurs (320 capteurs actifs)
--   fact_measurement — mesures IoT [HYPERTABLE TimescaleDB, chunk = 1 jour]
--   fact_alert      — alertes sur dépassements de seuils (table PG classique)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- dim_location — Localisation géographique des capteurs
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_location (
    location_id   SERIAL       PRIMARY KEY,
    district      VARCHAR(100) NOT NULL,
    latitude      DECIMAL(9,6) NOT NULL,
    longitude     DECIMAL(9,6) NOT NULL,
    zone_type     VARCHAR(50)  NOT NULL    -- ex: 'residential', 'commercial', 'industrial'
);

-- ---------------------------------------------------------------------------
-- dim_sensor — Référentiel des capteurs IoT
-- Types : air_quality_pm25, air_quality_no2, traffic_density, noise_level, temperature
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_sensor (
    sensor_id      VARCHAR(50)  PRIMARY KEY,   -- ex: 'S-001', 'S-042'
    type           VARCHAR(50)  NOT NULL,
    location_id    INTEGER      REFERENCES dim_location(location_id),
    installed_date DATE         NOT NULL DEFAULT CURRENT_DATE,
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- fact_measurement — Mesures en provenance des capteurs
-- Clé primaire composite (ts, sensor_id) garantit l'idempotence.
-- [HYPERTABLE TimescaleDB — partitionné par jour sur la colonne ts]
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_measurement (
    ts          TIMESTAMPTZ  NOT NULL,
    sensor_id   VARCHAR(50)  NOT NULL,
    value       DECIMAL(10,3) NOT NULL,
    unit        VARCHAR(20)  NOT NULL,
    PRIMARY KEY (ts, sensor_id)
);

-- Conversion en hypertable (chunk = 1 jour pour ~5M mesures/jour)
SELECT create_hypertable(
    'fact_measurement',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Index compound pour les requêtes Grafana (filtre par capteur + tri temporel)
CREATE INDEX IF NOT EXISTS idx_fact_measurement_sensor_ts
    ON fact_measurement (sensor_id, ts DESC);

-- ---------------------------------------------------------------------------
-- fact_alert — Alertes sur dépassements réglementaires
-- Table PostgreSQL classique (pas de hypertable — volume ≪ fact_measurement)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_alert (
    alert_id    SERIAL       PRIMARY KEY,
    ts          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    sensor_id   VARCHAR(50)  NOT NULL,
    severity    VARCHAR(20)  NOT NULL CHECK (severity IN ('warning', 'critical')),
    value       DECIMAL(10,3) NOT NULL,
    threshold   DECIMAL(10,3) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_alert_ts      ON fact_alert (ts DESC);
CREATE INDEX IF NOT EXISTS idx_fact_alert_sensor  ON fact_alert (sensor_id, ts DESC);
