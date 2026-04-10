"""Configuration pytest commune.

Ajoute le dossier plugins/ au sys.path pour que les imports du style
``from hooks.sensor_api_hook import SensorAPIHook`` fonctionnent dans
tous les tests, qu'ils s'executent localement ou dans le conteneur
airflow-worker.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Racine du projet (dossier parent de tests/)
_PROJECT_ROOT = Path(__file__).parent.parent

# Ajout de plugins/ pour l'import des hooks et operators
_PLUGINS = _PROJECT_ROOT / "plugins"
if str(_PLUGINS) not in sys.path:
    sys.path.insert(0, str(_PLUGINS))
