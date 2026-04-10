-- Activation de l'extension TimescaleDB.
-- Ce script est execute automatiquement au premier demarrage du conteneur timescaledb
-- grace au volume monte dans /docker-entrypoint-initdb.d/.
-- PREREQUIS : image timescale/timescaledb:2.26.2-pg18.

CREATE EXTENSION IF NOT EXISTS timescaledb;
