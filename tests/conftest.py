"""Configuration pytest commune.

Ajoute le dossier plugins/ au sys.path pour que les imports du style
``from hooks.sensor_api_hook import SensorAPIHook`` fonctionnent dans
tous les tests, qu'ils s'executent localement ou dans le conteneur
airflow-worker.

Fournit aussi des stubs pour les modules Airflow afin de pouvoir lancer
les tests **sans** avoir Airflow installé localement.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

# Racine du projet (dossier parent de tests/)
_PROJECT_ROOT = Path(__file__).parent.parent

# Ajout de plugins/ pour l'import des hooks et operators
_PLUGINS = _PROJECT_ROOT / "plugins"
if str(_PLUGINS) not in sys.path:
    sys.path.insert(0, str(_PLUGINS))


def _stub_airflow() -> None:
    """Injecte des faux modules airflow dans sys.modules si Airflow
    n'est pas installé (exécution locale sans virtualenv Airflow)."""
    try:
        import airflow  # noqa: F401
        return  # Airflow est installé, pas besoin de stubs
    except ImportError:
        pass

    # Module racine
    airflow_mod = types.ModuleType("airflow")
    sys.modules["airflow"] = airflow_mod

    # airflow.hooks.base  →  BaseHook
    hooks_pkg = types.ModuleType("airflow.hooks")
    base_mod = types.ModuleType("airflow.hooks.base")
    base_hook_cls = MagicMock
    base_mod.BaseHook = base_hook_cls
    sys.modules["airflow.hooks"] = hooks_pkg
    sys.modules["airflow.hooks.base"] = base_mod

    # airflow.providers.http.hooks.http  →  HttpHook (stub fonctionnel)
    providers = types.ModuleType("airflow.providers")
    providers_http = types.ModuleType("airflow.providers.http")
    providers_http_hooks = types.ModuleType("airflow.providers.http.hooks")
    http_mod = types.ModuleType("airflow.providers.http.hooks.http")

    class _HttpHookStub:
        """Stub minimal qui reproduit la surface de HttpHook."""
        def __init__(self, method="GET", http_conn_id="http_default", **kwargs):
            self.method = method
            self.http_conn_id = http_conn_id

        def run(self, endpoint, data=None, headers=None, extra_options=None):
            raise NotImplementedError("stub — use patch.object in tests")

        def get_conn(self):
            return MagicMock()

    http_mod.HttpHook = _HttpHookStub

    sys.modules["airflow.providers"] = providers
    sys.modules["airflow.providers.http"] = providers_http
    sys.modules["airflow.providers.http.hooks"] = providers_http_hooks
    sys.modules["airflow.providers.http.hooks.http"] = http_mod

    # airflow.sdk  →  dag / task stubs
    sdk_mod = types.ModuleType("airflow.sdk")
    sdk_mod.dag = MagicMock()
    sdk_mod.task = MagicMock()
    sys.modules["airflow.sdk"] = sdk_mod


_stub_airflow()
