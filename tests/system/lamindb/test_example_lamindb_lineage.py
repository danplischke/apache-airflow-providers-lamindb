"""End-to-end test through Airflow's real task runner (``dag.test()``).

Covers what neither the mocked nor the integration tests can: XCom resolution between
tasks, setup/teardown semantics for the DAG run state, and the real
``PythonVirtualenvOperator`` creating a venv and running the shipped wrapper.

Requires a throwaway LaminDB instance and a migrated Airflow metadata DB::

    export HOME=/tmp/lamin-home  # keeps ~/.lamin away from your real settings
    lamin init --storage /tmp/lamin-home/store --name airflowtest
    export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/lamindb  # dag.test() needs the DAG serialized
    airflow db migrate
    LAMINDB_E2E_TEST=1 pytest tests/system
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("lamindb")

pytestmark = pytest.mark.skipif(
    os.environ.get("LAMINDB_E2E_TEST") != "1",
    reason="Set LAMINDB_E2E_TEST=1 (needs a lamindb instance and a migrated Airflow DB)",
)

DAG_FILE = Path(__file__).parent / "example_lamindb_lineage.py"


@pytest.fixture(scope="module")
def dags():
    # generated module name, as Airflow's DAG bundle loader does
    spec = importlib.util.spec_from_file_location("unusual_prefix_e2e_example_lamindb_lineage", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    # The throwaway instance is local and unknown to LaminHub, so the tasks use lamindb's own
    # configuration instead of the lamindb_default connection. The virtualenvs run in another
    # directory, so they get the instance explicitly. dag.test() runs these task objects.
    import lamindb_setup

    for dag in (module.dag_ok, module.dag_fail, module.dag_auto_flow):
        for task in dag.tasks:
            if hasattr(task, "lamindb_conn_id"):
                task.lamindb_conn_id = None
                task.lamindb_instance = lamindb_setup.settings.instance.slug
    return module


def flow_and_steps(dag_id: str, run_id: str):
    import lamindb as ln

    flow = ln.Run.filter(reference=f"{dag_id}/{run_id}", reference_type="airflow_dag_run").one()
    steps = {r.entrypoint: r for r in ln.Run.filter(initiated_by_run=flow)}
    return flow, steps


def test_dag_run_success(dags):
    dr = dags.dag_ok.test()
    assert str(dr.state) == "success"

    flow, steps = flow_and_steps("lamindb_example_ok", dr.run_id)
    assert flow.status == "completed"
    assert flow.transform.kind == "script"
    assert flow.transform.source_code is not None
    assert set(steps) == {"extract", "transform", "report"}  # load is a plain task
    for step in steps.values():
        assert step.status == "completed"
        assert step.transform.uid == flow.transform.uid
        assert step.reference_type == "airflow_task_instance"
    assert steps["extract"].params == {"count": 3}
    assert steps["transform"].params == {"data": {"count": 3}}  # XCom between virtualenvs
    assert steps["report"].params == {"value": 7}  # XCom from a plain task


def test_failed_step_fails_dag_run_and_flow(dags):
    dr = dags.dag_fail.test()
    assert str(dr.state) == "failed"  # finish is a teardown, so it does not mask the failure

    flow, steps = flow_and_steps("lamindb_example_fail", dr.run_id)
    assert flow.status == "errored"
    assert flow.finished_at is not None
    assert steps["boom"].status == "errored"


def test_auto_flow_dag_run(dags):
    assert dags.dag_auto_flow.get_task("lamindb_flow_init").system_site_packages is True
    dr = dags.dag_auto_flow.test()
    assert str(dr.state) == "success"

    flow, steps = flow_and_steps("lamindb_example_auto_flow", dr.run_id)
    assert flow.status == "completed"
    assert set(steps) == {"venv_extract", "venv_double"}
    assert all(step.status == "completed" for step in steps.values())
    assert steps["venv_double"].params == {"data": {"count": 4}}
