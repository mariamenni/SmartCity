import pytest


pytestmark = pytest.mark.skip(reason="TODO equipe: remplacer par un vrai test DagBag dans le conteneur Airflow")


def test_placeholder_dag_import() -> None:
    assert True
