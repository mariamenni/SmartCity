-- Donnees de reference : localisations et capteurs.
-- Idempotent via ON CONFLICT DO NOTHING.
-- Les IDs doivent rester coherents avec le sensor-simulator (app.py).

INSERT INTO dim_location (location_id, district, latitude, longitude, zone_type) VALUES
    ('LOC-01', 'Part-Dieu',      45.760500, 4.859700, 'commercial'),
    ('LOC-02', 'Confluence',     45.742200, 4.818300, 'mixte'),
    ('LOC-03', 'Croix-Rousse',   45.775600, 4.828200, 'residentiel'),
    ('LOC-04', 'Gerland',        45.723800, 4.833400, 'industriel'),
    ('LOC-05', 'Vieux-Lyon',     45.760100, 4.827200, 'touristique'),
    ('LOC-06', 'Bron',           45.732100, 4.911800, 'residentiel'),
    ('LOC-07', 'Villeurbanne',   45.766900, 4.880000, 'mixte'),
    ('LOC-08', 'Caluire',        45.798100, 4.844300, 'residentiel'),
    ('LOC-09', 'La Guillotiere', 45.749300, 4.848100, 'commercial'),
    ('LOC-10', 'Perrache',       45.749100, 4.825700, 'transport')
ON CONFLICT (location_id) DO NOTHING;

INSERT INTO dim_sensor (sensor_id, type, location_id, installed_date, is_active) VALUES
    ('S-001', 'air_quality_pm25', 'LOC-01', '2024-01-15', TRUE),
    ('S-002', 'air_quality_no2',  'LOC-01', '2024-01-15', TRUE),
    ('S-003', 'traffic_density',  'LOC-01', '2024-02-01', TRUE),
    ('S-004', 'noise_level',      'LOC-01', '2024-02-01', TRUE),
    ('S-005', 'temperature',      'LOC-01', '2024-03-01', TRUE),
    ('S-006', 'air_quality_pm25', 'LOC-02', '2024-01-20', TRUE),
    ('S-007', 'air_quality_no2',  'LOC-02', '2024-01-20', TRUE),
    ('S-008', 'traffic_density',  'LOC-02', '2024-02-05', TRUE),
    ('S-009', 'noise_level',      'LOC-02', '2024-02-05', TRUE),
    ('S-010', 'temperature',      'LOC-02', '2024-03-05', TRUE),
    ('S-011', 'air_quality_pm25', 'LOC-03', '2024-01-25', TRUE),
    ('S-012', 'traffic_density',  'LOC-03', '2024-02-10', TRUE),
    ('S-013', 'noise_level',      'LOC-03', '2024-02-10', TRUE),
    ('S-014', 'air_quality_pm25', 'LOC-04', '2024-01-28', TRUE),
    ('S-015', 'air_quality_no2',  'LOC-04', '2024-01-28', TRUE),
    ('S-016', 'traffic_density',  'LOC-04', '2024-02-12', TRUE),
    ('S-017', 'air_quality_pm25', 'LOC-05', '2024-01-30', TRUE),
    ('S-018', 'noise_level',      'LOC-05', '2024-02-15', TRUE),
    ('S-019', 'air_quality_pm25', 'LOC-06', '2024-02-01', TRUE),
    ('S-020', 'traffic_density',  'LOC-06', '2024-02-20', TRUE)
ON CONFLICT (sensor_id) DO NOTHING;
