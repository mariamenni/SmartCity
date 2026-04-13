-- =============================================================================
-- 01-extension.sql — Activation TimescaleDB
--
-- DOIT être exécuté EN PREMIER dans docker-entrypoint-initdb.d/
-- Sans cette extension, create_hypertable() ne existe pas → erreur garantie.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;
