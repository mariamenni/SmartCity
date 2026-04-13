CREATE TABLE IF NOT EXISTS dim_location (
    location_id   INTEGER      PRIMARY KEY,
    district      VARCHAR(100) NOT NULL,
    latitude      DECIMAL(9,6) NOT NULL,
    longitude     DECIMAL(9,6) NOT NULL,
    zone_type     VARCHAR(50)  NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sensor (
    sensor_id      VARCHAR(50)  PRIMARY KEY,
    type           VARCHAR(50)  NOT NULL,
    location_id    INTEGER      REFERENCES dim_location(location_id),
    installed_date DATE         NOT NULL DEFAULT CURRENT_DATE,
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS fact_measurement (
    ts         TIMESTAMPTZ   NOT NULL,
    sensor_id  VARCHAR(50)   NOT NULL REFERENCES dim_sensor(sensor_id),
    value      DECIMAL(10,3) NOT NULL,
    unit       VARCHAR(20)   NOT NULL,
    PRIMARY KEY (ts, sensor_id)
);

SELECT create_hypertable(
    'fact_measurement',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS idx_fact_measurement_sensor_ts
    ON fact_measurement (sensor_id, ts DESC);

CREATE TABLE IF NOT EXISTS fact_alert (
    alert_id   SERIAL        PRIMARY KEY,
    ts         TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    sensor_id  VARCHAR(50)   NOT NULL REFERENCES dim_sensor(sensor_id),
    severity   VARCHAR(20)   NOT NULL CHECK (severity IN ('warning', 'critical')),
    value      DECIMAL(10,3) NOT NULL,
    threshold  DECIMAL(10,3) NOT NULL,
    UNIQUE (ts, sensor_id, severity)
);

CREATE INDEX IF NOT EXISTS idx_fact_alert_ts
    ON fact_alert (ts DESC);

CREATE INDEX IF NOT EXISTS idx_fact_alert_sensor
    ON fact_alert (sensor_id, ts DESC);