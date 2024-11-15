import pytest
from unittest.mock import patch
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from airflow.utils.dates import days_ago


@pytest.fixture()
def dagbag():
    return DagBag(dag_folder="dags")


def test_task_count(dagbag):
    dag = dagbag.get_dag(dag_id="ETL_data_from_api")
    assert dag is not None, "DAG ETL_data_from_api not found"
    assert dagbag.import_errors == {}, "There are import errors in the DAG"
    assert len(dag.tasks) == 4, "DAG ETL_data_from_api should have 4 tasks"


def test_dependencies(dagbag):
    """Test if the task dependencies are set up correctly."""
    dag = dagbag.get_dag(dag_id="ETL_data_from_api")

    # Check task dependencies
    extract = dag.get_task("extract")
    transform = dag.get_task("transform")
    load = dag.get_task("load")
    send_email = dag.get_task("send_email_on_success")

    assert extract.downstream_list == [transform], "Extract should lead to Transform"
    assert transform.downstream_list == [load], "Transform should lead to Load"
    assert load.downstream_list == [send_email], "Load should lead to Send Email"
