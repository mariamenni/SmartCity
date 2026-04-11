INSERT INTO dim_location (location_id, district, latitude, longitude, zone_type) VALUES
    (1, 'Batignolles', 48.8862, 2.3195, 'residential'),
    (2, 'Nation',      48.8484, 2.3965, 'commercial'),
    (3, 'Belleville',  48.8700, 2.3800, 'residential'),
    (4, 'La Défense',  48.8924, 2.2384, 'industrial'),
    (5, 'Châtelet',    48.8584, 2.3470, 'commercial')
ON CONFLICT (location_id) DO NOTHING;

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
    ('S-011', 'air_quality_pm25', 5, '2024-04-01', FALSE),
    ('S-012', 'noise_level',      2, '2024-04-15', FALSE)
ON CONFLICT (sensor_id) DO NOTHING;