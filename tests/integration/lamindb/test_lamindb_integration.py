"""Integration tests against a real LaminDB instance.

Run against a throwaway instance, never a shared one::

    export HOME=/tmp/lamin-home  # keeps ~/.lamin away from your real settings
    lamin init --storage /tmp/lamin-home/store --name airflowtest
    LAMINDB_INTEGRATION_TEST=1 pytest tests/integration

These exercise the paths the mocked unit tests cannot: the flow run of the DAG file's
transform, step runs resolving the same transform by hash, and the source shipped to
remote interpreters.
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
from airflow.sdk import DAG

dag = DAG("integration_dag")


def extract(count: int = 10) -> dict:
    return {"count": count}
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


@pytest.fixture(scope="module")
def instance() -> str:
    """The test instance; remote interpreters run elsewhere and can't find it by directory."""
    import lamindb_setup

    return lamindb_setup.settings.instance.slug


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


def run_script(script: str, tmp_path, *, succeed: bool = True) -> subprocess.CompletedProcess[str]:
    """Run ``script`` in a fresh interpreter, as a virtualenv/pod would."""
    path = tmp_path / "script.py"
    path.write_text(script)
    proc = subprocess.run(
        [sys.executable, str(path)], capture_output=True, text=True, cwd=tmp_path, check=False
    )
    assert (proc.returncode == 0) is succeed, proc.stderr
    return proc


def start_flow_run_kwargs(dag_module, run_id: str) -> dict:
    from airflow.providers.lamindb.utils.dag_run import flow_run_params

    return {
        "reference": f"integration_dag/{run_id}",
        "path": dag_module.dag.fileloc,
        "entrypoint": "integration_dag",
        "params": flow_run_params(make_context(dag_module, run_id, "lamindb_flow_init")),
    }


def step_script(user_source: str, callable_name: str, call: str, config: dict) -> str:
    """The step source a virtualenv/pod receives, then called by name like Airflow's template."""
    from airflow.providers.lamindb.utils.remote import build_remote_step_source

    script = build_remote_step_source(user_source=user_source, callable_name=callable_name, config=config)
    return script + f'\nimport json\nprint("RESULT=" + json.dumps({call}))\n'


def test_start_flow_run_is_idempotent_per_dag_run(dag_module, run_id):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs)
    assert flow_run.status == "started"
    assert flow_run.entrypoint == "integration_dag"
    assert flow_run.reference == f"integration_dag/{run_id}"
    assert flow_run.reference_type == "airflow_dag_run"
    assert flow_run.transform.key.endswith("integration_dag.py")
    assert flow_run.transform.kind == "script"
    assert flow_run.params["conf"] == {"a": 1}
    assert ln.context.run is None

    retried = runtime.start_flow_run(**kwargs)
    assert retried.uid == flow_run.uid
    assert ln.Run.get(uid=flow_run.uid).status == "restarted"
    assert ln.Run.filter(reference=kwargs["reference"]).count() == 1

    runtime.finish_flow_run(kwargs["reference"], True)
    assert ln.Run.get(uid=flow_run.uid).status == "completed"


def test_step_without_flow_run_raises(dag_module, run_id):
    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    with pytest.raises(LookupError, match="No LaminDB flow run"):
        runtime.run_step(dag_module.extract, (), {}, f"integration_dag/{run_id}", dag_source(dag_module.dag))


def test_failed_remote_step_marks_step_and_flow_errored(dag_module, run_id, instance, tmp_path):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs)

    script = step_script(
        'def boom():\n    raise ValueError("step failed")\n',
        "boom",
        "boom()",
        {
            "flow_run_reference": kwargs["reference"],
            "source_code": dag_source(dag_module.dag),
            "step_reference": "http://airflow/ti",
            "instance": instance,
        },
    )
    assert "ValueError: step failed" in run_script(script, tmp_path, succeed=False).stderr
    assert ln.Run.filter(initiated_by_run=flow_run, entrypoint="boom").one().status == "errored"

    runtime.finish_flow_run(kwargs["reference"], False)
    flow_run = ln.Run.get(uid=flow_run.uid)
    assert flow_run.status == "errored"
    assert flow_run.finished_at is not None


def test_remote_sources_run_in_fresh_interpreter(dag_module, run_id, instance, tmp_path):
    """The source shipped to a venv/pod must work with only lamindb installed."""
    import lamindb as ln

    from airflow.providers.lamindb.utils.dag_run import dag_source
    from airflow.providers.lamindb.utils.remote import build_remote_flow_source

    reference = f"integration_dag/{run_id}"

    def flow_script(runtime_function: str, config: dict) -> str:
        return (
            build_remote_flow_source(function_name="flow_fn", runtime_function=runtime_function)
            + f"\nimport json\nprint('RESULT=' + json.dumps(flow_fn(json.loads({json.dumps(config)!r}))))\n"
        )

    start_config = {**start_flow_run_kwargs(dag_module, run_id), "instance": instance}
    flow_uid = json.loads(
        run_script(flow_script("start_flow_run", start_config), tmp_path).stdout.split("RESULT=")[1]
    )
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.reference == reference
    assert flow_run.status == "started"

    user_source = textwrap.dedent(
        """
        def extract(count: int = 10) -> dict:
            return {"count": count}
        """
    )
    script = step_script(
        user_source,
        "extract",
        "extract(count=7)",
        {
            "flow_run_reference": reference,
            "source_code": dag_source(dag_module.dag),
            "step_reference": "http://airflow/ti",
            "instance": instance,
        },
    )
    assert 'RESULT={"count": 7}' in run_script(script, tmp_path).stdout

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
