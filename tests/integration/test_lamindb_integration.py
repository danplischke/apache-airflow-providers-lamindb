"""Integration tests against a real LaminDB instance.

Run against a throwaway instance, never a shared one::

    export HOME=/tmp/lamin-home  # keeps ~/.lamin away from your real settings
    lamin init --storage /tmp/lamin-home/store --name airflowtest
    LAMINDB_INTEGRATION_TEST=1 pytest tests/integration

These exercise the paths the mocked unit tests cannot: ``@ln.step`` really attaching
to the flow run, transform resolution by hash, and the source shipped to remote
interpreters.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock

import pytest

pytest.importorskip("lamindb")

pytestmark = pytest.mark.skipif(
    os.environ.get("LAMINDB_INTEGRATION_TEST") != "1",
    reason="Set LAMINDB_INTEGRATION_TEST=1 to run integration tests",
)

DAG_SOURCE = """
import lamindb as ln
from airflow.sdk import DAG

dag = DAG("integration_dag")


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


def boom() -> None:
    raise ValueError("step failed")
"""


@pytest.fixture(scope="module")
def dag_module(tmp_path_factory):
    """Load a DAG file the way Airflow does: real file, generated module name."""
    path = tmp_path_factory.mktemp("dags") / "integration_dag.py"
    path.write_text(DAG_SOURCE)
    spec = importlib.util.spec_from_file_location("unusual_prefix_abc123_integration_dag", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def run_id():
    import secrets

    return f"manual__{secrets.token_hex(4)}"


def make_context(dag_module, run_id: str, task_id: str, task_states: dict | None = None) -> dict:
    ti = MagicMock(task_id=task_id, log_url=f"http://airflow/{run_id}/{task_id}")
    ti.get_task_states = MagicMock(return_value={run_id: task_states or {}})
    return {
        "dag": dag_module.dag,
        "dag_run": MagicMock(run_id=run_id, run_type="manual", logical_date=None, conf={"a": 1}),
        "run_id": run_id,
        "ti": ti,
    }


def run_script(script: str, tmp_path) -> str:
    """Run ``script`` in a fresh interpreter, as a virtualenv/pod would."""
    path = tmp_path / "script.py"
    path.write_text(script)
    proc = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, cwd=tmp_path, check=False)
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_is_lamindb_tracked_detects_ln_step() -> None:
    import lamindb as ln

    from airflow.providers.lamindb.utils.context import is_lamindb_tracked

    def plain() -> None: ...

    assert not is_lamindb_tracked(plain)
    assert is_lamindb_tracked(ln.step()(plain))
    assert is_lamindb_tracked(ln.flow()(plain))


def test_flow_and_steps_end_to_end(dag_module, run_id):
    import lamindb as ln

    from airflow.providers.lamindb import LaminDBFlowFinishOperator, LaminDBFlowInitOperator, LaminDBStepOperator

    init = LaminDBFlowInitOperator()
    flow_uid = init.execute(make_context(dag_module, run_id, init.task_id))
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.status == "started"
    assert flow_run.entrypoint == "integration_dag"
    assert flow_run.reference == f"integration_dag/{run_id}"
    assert flow_run.reference_type == "airflow_dag_run"
    assert flow_run.transform.key.endswith("integration_dag.py")
    assert flow_run.transform.kind == "script"
    assert flow_run.params["conf"] == {"a": 1}
    assert ln.context.run is None

    # plain callable: wrapped with ln.step() by the operator
    step = LaminDBStepOperator(task_id="extract", python_callable=dag_module.extract, op_kwargs={"count": 3})
    assert step.execute(make_context(dag_module, run_id, "extract")) == {"count": 3}
    # already @ln.step-decorated: used as is, no nested run
    step2 = LaminDBStepOperator(task_id="transform", python_callable=dag_module.transform, op_args=[{"count": 3}])
    assert step2.execute(make_context(dag_module, run_id, "transform")) == {"count": 6}
    assert ln.context.run is None

    steps = {r.entrypoint: r for r in ln.Run.filter(initiated_by_run=flow_run)}
    assert set(steps) == {"extract", "transform"}
    for r in steps.values():
        assert r.status == "completed"
        assert r.transform.uid == flow_run.transform.uid  # same DAG file transform, matched by hash
    assert steps["extract"].params == {"count": 3}

    # failing step: errored child run, exception propagates, context cleaned up
    step3 = LaminDBStepOperator(task_id="boom", python_callable=dag_module.boom)
    with pytest.raises(ValueError, match="step failed"):
        step3.execute(make_context(dag_module, run_id, "boom"))
    assert ln.context.run is None
    assert ln.Run.filter(initiated_by_run=flow_run, entrypoint="boom").one().status == "errored"

    finish = LaminDBFlowFinishOperator()
    finish.execute(make_context(dag_module, run_id, finish.task_id, {"extract": "success", "boom": "failed"}))
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.status == "errored"
    assert flow_run.finished_at is not None


def test_init_is_idempotent_per_dag_run(dag_module, run_id):
    import lamindb as ln

    from airflow.providers.lamindb import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator()
    first = init.execute(make_context(dag_module, run_id, init.task_id))
    second = init.execute(make_context(dag_module, run_id, init.task_id))  # retry
    assert first == second
    assert ln.Run.get(uid=first).status == "restarted"
    assert ln.Run.filter(reference=f"integration_dag/{run_id}").count() == 1

    finish = LaminDBFlowFinishOperator()
    finish.execute(make_context(dag_module, run_id, finish.task_id, {"x": "success"}))
    assert ln.Run.get(uid=first).status == "completed"


def test_step_without_init_raises(dag_module, run_id):
    from airflow.exceptions import AirflowException

    from airflow.providers.lamindb import LaminDBStepOperator

    step = LaminDBStepOperator(task_id="extract", python_callable=dag_module.extract)
    with pytest.raises(AirflowException, match="No LaminDB flow run"):
        step.execute(make_context(dag_module, run_id, "extract"))


def test_untracked_step_records_nothing(dag_module, run_id):
    """``track=False``: no flow run needed, nothing recorded, the real ``@ln.step()`` wrapper bypassed."""
    import lamindb as ln

    from airflow.providers.lamindb import LaminDBStepOperator

    runs_before = ln.Run.filter().count()
    # no flow init ran for this DAG run; transform is @ln.step()-decorated, which raises without a run
    step = LaminDBStepOperator(
        task_id="transform", python_callable=dag_module.transform, op_args=[{"count": 3}], track=False
    )
    assert step.execute(make_context(dag_module, run_id, "transform")) == {"count": 6}
    assert ln.context.run is None
    assert ln.Run.filter().count() == runs_before


def test_remote_sources_run_in_fresh_interpreter(dag_module, run_id, tmp_path):
    """The source shipped to a venv/pod must work with only lamindb installed."""
    import lamindb as ln

    from airflow.providers.lamindb.utils.dag_run import dag_source, flow_run_params
    from airflow.providers.lamindb.utils.remote import (
        build_remote_flow_source,
        build_remote_step_source,
        worker_instance_slug,
    )

    instance = worker_instance_slug()
    reference = f"integration_dag/{run_id}"
    context = make_context(dag_module, run_id, "lamindb_flow_init")

    def flow_script(runtime_function: str, config: dict) -> str:
        return (
            build_remote_flow_source(function_name="flow_fn", runtime_function=runtime_function)
            + f"\nimport json\nprint('RESULT=' + json.dumps(flow_fn(json.loads({json.dumps(config)!r}))))\n"
        )

    start_config = {
        "reference": reference,
        "path": dag_module.dag.fileloc,
        "entrypoint": "integration_dag",
        "params": flow_run_params(context),
        "instance": instance,
    }
    flow_uid = json.loads(run_script(flow_script("start_flow_run", start_config), tmp_path).split("RESULT=")[1])
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.reference == reference
    assert flow_run.status == "started"

    user_source = textwrap.dedent(
        """
        def extract(count: int = 10) -> dict:
            return {"count": count}
        """
    )
    step_script = build_remote_step_source(
        user_source=user_source,
        callable_name="extract",
        config={
            "flow_run_reference": reference,
            "source_code": dag_source(dag_module.dag),
            "step_reference": "http://airflow/ti",
            "instance": instance,
        },
    )
    # mimic Airflow's template: define, then call by name
    step_script += '\nimport json\nprint("RESULT=" + json.dumps(extract(count=7)))\n'
    assert 'RESULT={"count": 7}' in run_script(step_script, tmp_path)

    step_run = ln.Run.filter(initiated_by_run=flow_run).one()
    assert step_run.entrypoint == "extract"
    assert step_run.status == "completed"
    assert step_run.params == {"count": 7}
    assert step_run.reference == "http://airflow/ti"
    assert step_run.reference_type == "airflow_task_instance"
    assert step_run.transform.uid == flow_run.transform.uid

    finish_config = {"reference": reference, "success": True, "instance": instance}
    run_script(flow_script("finish_flow_run", finish_config), tmp_path)
    assert ln.Run.get(uid=flow_uid).status == "completed"
