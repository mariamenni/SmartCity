# Partie 4 — Alerting SmartCity (Batch Monitoring)

## Objectif

Mettre en place un système d’alerting batch avec Airflow permettant de :

- détecter les dépassements de seuils (pollution, bruit, trafic)
- identifier les capteurs offline
- enregistrer les alertes dans TimescaleDB
- garantir l’idempotence des insertions

---

## Architecture du DAG

Le DAG `smartcity_alert_check_batch` est composé de 4 étapes :

1. Extraction des mesures des 15 dernières minutes
2. Détection des alertes seuil (Custom Operator)
3. Détection des capteurs offline
4. Insertion des alertes en base
5. Rapport final

---

## Détection des seuils

Un opérateur custom `ThresholdAlertOperator` compare les valeurs des capteurs avec des règles métier :

- PM2.5 > 50 → critical
- NO2 > 200 → critical
- Traffic > 2000 → critical
- Noise > 80 → critical

Les alertes sont générées dynamiquement depuis les données extraites.

---

## Détection des capteurs offline

Un capteur est considéré offline si :

- il est actif (`is_active = true`)
- il possède une date d’installation valide
- aucune mesure n’a été reçue dans les 15 dernières minutes


## Idempotence des alertes

Les doublons sont évités grâce à :

`ALTER TABLE fact_alert`
`ADD CONSTRAINT unique_alert UNIQUE (ts, sensor_id, severity);`

Et côté DAG :

`ON CONFLICT (ts, sensor_id, severity) DO NOTHING`

## Tests de validation

Un test Airflow valide :

l’existence du DAG
la présence des tâches principales
la structure du pipeline

## Exemple Resultat

[2026-04-11 20:12:58] INFO - Done. Returned value was: None
[2026-04-11 20:12:58] INFO - ============================================================
[2026-04-11 20:12:58] INFO - RAPPORT ALERTING FINAL
[2026-04-11 20:12:58] INFO - ============================================================
[2026-04-11 20:12:58] INFO - Alertes seuil    : 2
[2026-04-11 20:12:58] INFO - Capteurs offline : 12
[2026-04-11 20:12:58] INFO - ============================================================

