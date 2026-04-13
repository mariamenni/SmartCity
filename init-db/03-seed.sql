-- =============================================================================
-- 03-seed.sql — Données de référence SmartCity
--
-- 5 locations (districts parisiens) + 10 capteurs actifs (+ 2 inactifs)
-- Seuils réglementaires source : cahier des charges §5
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Locations — 5 districts
-- ---------------------------------------------------------------------------
INSERT INTO dim_location (district, latitude, longitude, zone_type) VALUES
    ('Batignolles',    48.8862, 2.3195, 'residential'),
    ('Nation',         48.8484, 2.3965, 'commercial'),
    ('Belleville',     48.8700, 2.3800, 'residential'),
    ('La Défense',     48.8924, 2.2384, 'industrial'),
    ('Châtelet',       48.8584, 2.3470, 'commercial')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- Capteurs — 10 actifs, 2 inactifs
-- Types : air_quality_pm25, air_quality_no2, traffic_density, noise_level, temperature
-- ---------------------------------------------------------------------------
INSERT INTO dim_sensor (sensor_id, type, location_id, installed_date, is_active) VALUES
    ('S-001', 'air_quality_pm25', 1, '2024-01-15', TRUE),
    ('S-002', 'air_quality_no2',  1, '2024-01-15', TRUE),
    ('S-003', 'traffic_density',  1, '2024-02-01', TRUE),
    ('S-004', 'noise_level',      2, '2024-02-01', TRUE),
    ('S-005', 'temperature',      2, '2024-02-15', TRUE),
    ('S-006', 'air_quality_pm25', 3, '2024-03-01', TRUE),
    ('S-007', 'air_quality_no2',  3, '2024-03-01', TRUE),
    ('S-008', 'traffic_density',  4, '2024-03-15', TRUE),
    ('S-009', 'noise_level',      4, '2024-03-15', TRUE),
    ('S-010', 'temperature',      5, '2024-04-01', TRUE),
    ('S-011', 'air_quality_pm25', 5, '2024-04-01', FALSE),  -- hors service
    ('S-012', 'noise_level',      2, '2024-04-15', FALSE)   -- maintenance
ON CONFLICT DO NOTHING;
