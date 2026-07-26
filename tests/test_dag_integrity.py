import pytest
from airflow.models import DagBag
from airflow.exceptions import AirflowDagCycleException

@pytest.fixture(scope="module")
def dagbag():
   
    return DagBag(dag_folder="./dags", include_examples=False)

def test_dag_loaded_with_no_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"DAG Import Errors: {dagbag.import_errors}"

def test_oms_transformation_dag_integrity(dagbag):
    dag = dagbag.get_dag(dag_id="oms_transformation")
    
    assert dag is not None, "DAG 'oms_transformation' was not found."
    assert len(dag.tasks) > 0, "DAG has no tasks."
    
    assert dag.default_args.get("owner") == "Mo Amer"
    assert dag.default_args.get("retries") == 2
    assert "on_failure_callback" in dag.default_args

def test_no_cyclical_dependencies(dagbag):
    dag = dagbag.get_dag(dag_id="oms_transformation")
    assert dag is not None
    
    try:
        dag.topological_sort()
    except AirflowDagCycleException:
        pytest.fail("Cyclical dependency detected in DAG!")