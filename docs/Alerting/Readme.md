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

## DATA FLOW AIRFLOW

Le DAG repose sur `XCom` :

extract_measurements
        ↓
ThresholdAlertOperator (XCom pull)
        ↓
load_alerts (fact_alert)

et en parallèle :

detect_offline_sensors → load_alerts

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

`Tests automatisés (Pytest)` :

Les testS valident 4 niveaux :

`test_dag_load`

 Vérifie que le DAG existe et se charge correctement
 DAG présent dans Airflow
 au moins une tâche définie

`test_threshold_operator`

Vérifie la logique métier du ThresholdAlertOperator

détection warning/critical correcte
mapping type → seuils OK
génération correcte des tuples alertes

`test_offline_logic_simple`

Vérifie la logique offline 

simulate sensors
vérifie condition > 15 min
validation logique pure Python

`test_alerts_in_db (E2E test)`

Test complet d’intégration :

Étapes :
nettoyage tables (fact_alert, fact_measurement)
insertion données test
exécution DAG manuellement
exécution des tasks synchrones
vérification résultats en base



## Résultat attendu des tests

collected 4 items

test_dag_load PASSED
test_threshold_operator PASSED
test_offline_logic_simple PASSED
test_alerts_in_db PASSED

## Exemple de résultat DAG

[2026-04-13 11:30:36] INFO - ============================================================ 
[2026-04-13 11:30:36] INFO - RAPPORT ALERTING FINAL 
[2026-04-13 11:30:36] INFO - ============================================================ 
[2026-04-13 11:30:36] INFO - Alertes seuil    : 3
[2026-04-13 11:30:36] INFO - Capteurs offline : 1
[2026-04-13 11:30:36] INFO - ============================================================ 