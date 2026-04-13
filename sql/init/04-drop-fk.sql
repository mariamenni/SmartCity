-- 04-drop-fk.sql
-- =============================================================================
-- Suppression des contraintes FK sur fact_measurement et fact_alert.
--
-- Pourquoi ?
--   Le sensor-simulator (arnauropero/smart-cities-api) génère des IDs entiers
--   (1, 2, 3…) qui ne correspondent PAS aux sensor_id du seed Lyon (S-001…).
--   Le DAG P2 (dims refresh) resynchronise dim_sensor depuis l'API, mais il
--   tourne en @daily alors que P5 (consumer) tourne toutes les minutes.
--
--   Sans cette migration, les premiers INSERT dans fact_measurement échouent
--   avec une violation de FK jusqu'à ce que P2 ait tourné une première fois.
--
--   En supprimant les FK :
--   - fact_measurement accepte n'importe quel sensor_id (ex. "1", "2", "3")
--   - fact_alert idem
--   - L'intégrité référentielle est assurée applicativement par P2
--     (qui maintient dim_sensor à jour) et par les requêtes Grafana.
--
-- Idempotent : IF EXISTS garantit qu'aucune erreur n'est levée si les
-- contraintes n'existent pas (ex. ré-exécution ou environnement déjà migré).
-- =============================================================================

ALTER TABLE fact_measurement
    DROP CONSTRAINT IF EXISTS fact_measurement_sensor_id_fkey;

ALTER TABLE fact_alert
    DROP CONSTRAINT IF EXISTS fact_alert_sensor_id_fkey;
