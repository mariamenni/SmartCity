-- =========================
-- CONTRAINTE D'IDEMPOTENCE
-- =========================
ALTER TABLE fact_alert
ADD CONSTRAINT unique_alert UNIQUE (ts, sensor_id, severity);

-- =========================
-- VALIDATION : détection doublons
-- =========================
SELECT
    ts,
    sensor_id,
    severity,
    COUNT(*) as occurrences
FROM fact_alert
GROUP BY ts, sensor_id, severity
HAVING COUNT(*) > 1;

-- =========================
-- KPI ALERTING
-- =========================
SELECT
    sensor_id,
    COUNT(*) AS nb_alerts
FROM fact_alert
WHERE ts > NOW() - INTERVAL '1 hour'
GROUP BY sensor_id
ORDER BY nb_alerts DESC;