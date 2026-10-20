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


def step_script(
    user_source: str,
    callable_name: str,
    call: str,
    config: dict,
    runtime_function: str = "run_step",
    bootstrap: str = "",
) -> str:
    """The step source a virtualenv/pod receives, then called by name like Airflow's template."""
    from airflow.providers.lamindb.utils.remote import build_remote_step_source

    script = build_remote_step_source(
        user_source=user_source,
        callable_name=callable_name,
        config=config,
        runtime_function=runtime_function,
        bootstrap=bootstrap,
    )
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


def test_step_records_only_params_lamindb_can_store(dag_module, run_id, caplog, capfd):
    """Values lamindb would skip with a warning that shows them are left out beforehand."""
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    class Client:
        def __repr__(self) -> str:
            return "Client(credentials='s3cret')"

    def load(count: int, name: str, client, credentials, options: dict, missing=None):
        return count

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs)
    result = runtime.run_step(
        load,
        (3, "x", Client(), "s3cret"),
        {"options": {"a": 1}},
        kwargs["reference"],
        dag_source(dag_module.dag),
        exclude_params=["credentials"],
    )

    assert result == 3
    step_run = ln.Run.filter(initiated_by_run=flow_run, entrypoint="load").one()
    assert step_run.params == {"count": 3, "name": "x", "options": {"a": 1}}
    output = capfd.readouterr()
    assert "Not recording parameter 'client' of type Client" in caplog.text
    assert "s3cret" not in caplog.text + output.out + output.err
    runtime.finish_flow_run(kwargs["reference"], True)


def test_connect_only_source_records_nothing(instance, tmp_path):
    import lamindb as ln

    runs_before = ln.Run.filter().count()
    transforms_before = ln.Transform.filter().count()
    user_source = textwrap.dedent(
        """
        def count_runs() -> int:
            import lamindb as ln

            return ln.Run.filter().count()
        """
    )
    script = step_script(user_source, "count_runs", "count_runs()", {"instance": instance}, "run_connected")

    result = json.loads(run_script(script, tmp_path).stdout.split("RESULT=")[1])

    assert result == runs_before  # connected to the instance
    assert ln.Run.filter().count() == runs_before
    assert ln.Transform.filter().count() == transforms_before


@pytest.fixture(scope="module")
def lab_space():
    import lamindb as ln

    return ln.Space.filter(name="lab").one_or_none() or ln.Space(name="lab").save()


def test_flow_steps_and_their_records_are_created_in_the_space(dag_module, run_id, lab_space):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    def save_record(name: str) -> str:
        return ln.Record(name=name).save().uid

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs, space="lab")
    assert flow_run.space == lab_space
    assert ln.context.space is None  # not left over for the next task

    # inherited from the flow run
    uid = runtime.run_step(
        save_record, (f"in-lab-{run_id}",), {}, kwargs["reference"], dag_source(dag_module.dag)
    )
    step_run = ln.Run.filter(initiated_by_run=flow_run, entrypoint="save_record").one()
    assert step_run.space == lab_space
    assert ln.Record.get(uid=uid).space == lab_space

    # connect-only, without a run
    uid = runtime.run_connected(save_record, (f"connected-{run_id}",), {}, space="lab")
    assert ln.Record.get(uid=uid).space == lab_space
    uid = runtime.run_connected(save_record, (f"default-{run_id}",), {})
    assert ln.Record.get(uid=uid).space_id == runtime.ALL_SPACE_ID

    with pytest.raises(LookupError, match="'missing' does not exist"):
        runtime.run_step(
            save_record, ("x",), {}, kwargs["reference"], dag_source(dag_module.dag), space="missing"
        )
    runtime.finish_flow_run(kwargs["reference"], True)


def test_bootstrap_runs_before_lamindb_is_imported(dag_module, run_id, instance, tmp_path):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs)
    bootstrap = "import os, sys\nos.environ['LAMINDB_IMPORTED_AT_BOOTSTRAP'] = str('lamindb' in sys.modules)"
    user_source = textwrap.dedent(
        """
        def probe() -> list:
            import sys

            import os

            return [os.environ["LAMINDB_IMPORTED_AT_BOOTSTRAP"] == "True", "lamindb" in sys.modules]
        """
    )
    config = {
        "flow_run_reference": kwargs["reference"],
        "source_code": dag_source(dag_module.dag),
        "instance": instance,
    }
    script = step_script(user_source, "probe", "probe()", config, bootstrap=bootstrap)

    result = json.loads(run_script(script, tmp_path).stdout.split("RESULT=")[1])

    assert result == [False, True]  # not imported at bootstrap; imported by the time the step ran
    assert ln.Run.filter(initiated_by_run=flow_run, entrypoint="probe").one().status == "completed"
    runtime.finish_flow_run(kwargs["reference"], True)


@pytest.fixture(scope="module")
def feature_branch():
    import lamindb as ln

    return ln.Branch.filter(name="feature-x").one_or_none() or ln.Branch(name="feature-x").save()


def test_flow_steps_and_their_records_are_on_the_branch(dag_module, run_id, feature_branch):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    def save_record(name: str) -> str:
        return ln.Record(name=name).save().uid

    kwargs = start_flow_run_kwargs(dag_module, run_id)
    flow_run = runtime.start_flow_run(**kwargs, branch="feature-x")
    assert flow_run.branch == feature_branch
    assert flow_run.transform.branch_id == runtime.MAIN_BRANCH_ID  # still found by source from main
    assert ln.context.branch is None

    # steps find the flow run although lamindb's default scope is main, and inherit its branch
    uid = runtime.run_step(
        save_record, (f"on-branch-{run_id}",), {}, kwargs["reference"], dag_source(dag_module.dag)
    )
    step_run = ln.Run.filter(initiated_by_run=flow_run, entrypoint="save_record", branch=feature_branch).one()
    assert step_run.transform == flow_run.transform
    assert ln.Record.filter(uid=uid).one().branch == feature_branch
    assert ln.context.branch is None

    # a retried init keeps the run and its branch
    assert runtime.start_flow_run(**kwargs).uid == flow_run.uid
    with pytest.raises(RuntimeError, match="on another branch than 'main'"):
        runtime.start_flow_run(**kwargs, branch="main")

    uid = runtime.run_connected(save_record, (f"connected-branch-{run_id}",), {}, branch="feature-x")
    assert ln.Record.filter(uid=uid).one().branch == feature_branch
    with pytest.raises(ValueError, match="can't be recorded on the LaminDB 'trash' branch"):
        runtime.run_connected(save_record, ("x",), {}, branch="trash")
    runtime.finish_flow_run(kwargs["reference"], True)
    assert ln.Run.filter(uid=flow_run.uid).one().status == "completed"


@pytest.fixture
def unique_dag_file(tmp_path, run_id):
    """A DAG file of its own (unique source and name), so tests don't share a transform."""
    from types import SimpleNamespace

    def write(body: str = "", *, newline: bytes = b"\n", name: str | None = None) -> SimpleNamespace:
        path = tmp_path / (name or f"standalone_{run_id}.py")
        lines = [f"# {run_id}", "from airflow.sdk import DAG", f'dag = DAG("standalone_{run_id}")', body]
        path.write_bytes(newline.join(line.encode() for line in lines))
        return SimpleNamespace(fileloc=str(path), dag_id=f"standalone_{run_id}", name=path.name)

    return write


def standalone_step(dag, fn, *args, step_reference="http://airflow/ti", **kwargs):
    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    return runtime.run_step(
        fn, args, kwargs, None, dag_source(dag), step_reference=step_reference, fileloc=dag.fileloc
    )


def test_standalone_step_is_recorded_without_a_parent(unique_dag_file):
    import lamindb as ln

    dag = unique_dag_file()

    def extract(count: int = 10) -> dict:
        return {"count": count}

    assert standalone_step(dag, extract, count=4, step_reference="http://airflow/solo") == {"count": 4}

    run = ln.Run.filter(reference="http://airflow/solo").one()
    assert run.initiated_by_run is None
    assert run.entrypoint == "extract"
    assert run.status == "completed"
    assert run.params == {"count": 4}
    assert run.reference_type == "airflow_task_instance"
    assert run.transform.key == dag.name  # the DAG file's, not a wrapper script's
    assert run.transform.kind == "script"
    assert run.transform.source_code is not None
    assert ln.context.run is None


def test_standalone_steps_and_the_flow_run_share_the_dag_files_transform(dag_module, run_id, unique_dag_file):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    dag = unique_dag_file()

    def solo():
        return 1

    # standalone first: the flow init then finds its transform by the file's hash
    standalone_step(dag, solo, step_reference=f"http://airflow/{run_id}/a")
    first = ln.Run.filter(reference=f"http://airflow/{run_id}/a").one()
    flow_run = runtime.start_flow_run(
        f"{dag.dag_id}/{run_id}", dag.fileloc, dag.dag_id, {"dag_id": dag.dag_id}
    )
    assert flow_run.transform.uid == first.transform.uid
    assert flow_run.transform.key == dag.name

    # and the other way around: a scoped step uses the flow run's transform and key
    runtime.run_step(solo, (), {}, f"{dag.dag_id}/{run_id}", dag_source(dag))
    child = ln.Run.filter(initiated_by_run=flow_run).one()
    assert child.transform.uid == flow_run.transform.uid
    runtime.finish_flow_run(f"{dag.dag_id}/{run_id}", True)
    assert ln.Transform.filter(key=dag.name).count() == 1


def test_standalone_step_of_an_edited_dag_is_a_new_version_of_the_same_transform(unique_dag_file, run_id):
    import lamindb as ln

    def solo():
        return 1

    v1 = unique_dag_file("# one")
    standalone_step(v1, solo, step_reference=f"http://airflow/{run_id}/v1")
    v2 = unique_dag_file("# two")  # same file name, edited
    standalone_step(v2, solo, step_reference=f"http://airflow/{run_id}/v2")
    standalone_step(v2, solo, step_reference=f"http://airflow/{run_id}/v2-again")

    t1 = ln.Run.filter(reference=f"http://airflow/{run_id}/v1").one().transform
    t2 = ln.Run.filter(reference=f"http://airflow/{run_id}/v2").one().transform
    again = ln.Run.filter(reference=f"http://airflow/{run_id}/v2-again").one().transform
    assert t1.key == t2.key == v1.name
    assert t1.uid[:-4] == t2.uid[:-4]  # versions of one transform
    assert t1.uid != t2.uid
    assert t2.is_latest
    assert not ln.Transform.get(uid=t1.uid).is_latest
    assert again.uid == t2.uid


def test_standalone_step_of_a_renamed_file_never_asks_whether_it_was_renamed(unique_dag_file, run_id):
    """Same source under another key makes lamindb prompt, which hangs a virtualenv or pod."""
    import lamindb as ln

    def solo():
        return 1

    original = unique_dag_file("# keep", name=f"original_{run_id}.py")
    standalone_step(original, solo, step_reference=f"http://airflow/{run_id}/o")
    renamed = unique_dag_file("# keep", name=f"renamed_{run_id}.py")
    standalone_step(renamed, solo, step_reference=f"http://airflow/{run_id}/r")

    runs = ln.Run.filter(reference__startswith=f"http://airflow/{run_id}/")
    assert {run.transform.uid for run in runs} == {runs[0].transform.uid}  # one transform, original key
    assert runs[0].transform.key == original.name


def test_crlf_dag_files_hash_like_what_the_flow_init_stores(unique_dag_file, run_id):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime

    dag = unique_dag_file(newline=b"\r\n")
    flow_run = runtime.start_flow_run(
        f"{dag.dag_id}/{run_id}", dag.fileloc, dag.dag_id, {"dag_id": dag.dag_id}
    )
    runtime.finish_flow_run(f"{dag.dag_id}/{run_id}", True)

    def solo():
        return 1

    standalone_step(dag, solo, step_reference=f"http://airflow/{run_id}/crlf")

    run = ln.Run.filter(reference=f"http://airflow/{run_id}/crlf").one()
    assert run.transform.uid == flow_run.transform.uid  # the same hash: no second transform
    assert ln.Transform.filter(key=dag.name).count() == 1


def test_standalone_step_errors_and_each_attempt_is_a_run(unique_dag_file, run_id):
    import lamindb as ln

    dag = unique_dag_file()
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("first attempt fails")
        return "ok"

    with pytest.raises(ValueError, match="first attempt"):
        standalone_step(dag, flaky, step_reference=f"http://airflow/{run_id}/try1")
    assert standalone_step(dag, flaky, step_reference=f"http://airflow/{run_id}/try2") == "ok"

    assert ln.Run.filter(reference=f"http://airflow/{run_id}/try1").one().status == "errored"
    assert ln.Run.filter(reference=f"http://airflow/{run_id}/try2").one().status == "completed"
    assert ln.context.run is None


def test_standalone_step_in_a_fresh_interpreter(unique_dag_file, run_id, instance, tmp_path):
    """Through the shipped source: no flow run lookup, the DAG file's key and no leftover state."""
    import lamindb as ln

    from airflow.providers.lamindb.utils.dag_run import dag_source

    dag = unique_dag_file()
    user_source = "def compute(x: int = 1) -> int:\n    return x * 3\n"
    config = {
        "flow_run_reference": None,
        "source_code": dag_source(dag),
        "fileloc": dag.fileloc,
        "step_reference": f"http://airflow/{run_id}/fresh",
        "instance": instance,
    }
    script = step_script(user_source, "compute", "compute(x=5)", config)

    assert "RESULT=15" in run_script(script, tmp_path).stdout

    run = ln.Run.filter(reference=f"http://airflow/{run_id}/fresh").one()
    assert (run.initiated_by_run, run.status, run.params) == (None, "completed", {"x": 5})
    assert run.transform.key == dag.name


def test_parallel_standalone_steps_of_a_new_dag_share_one_transform(
    unique_dag_file, run_id, instance, tmp_path
):
    """The first run of a DAG starts all its standalone (mapped) steps at once."""
    import lamindb as ln

    from airflow.providers.lamindb.utils.dag_run import dag_source

    dag = unique_dag_file()
    processes = []
    for index in range(4):
        config = {
            "flow_run_reference": None,
            "source_code": dag_source(dag),
            "fileloc": dag.fileloc,
            "step_reference": f"http://airflow/{run_id}/par{index}",
            "instance": instance,
        }
        script = step_script("def work() -> int:\n    return 1\n", "work", "work()", config)
        path = tmp_path / f"par{index}.py"
        path.write_text(script)
        processes.append(
            subprocess.Popen(
                [sys.executable, str(path)],
                cwd=tmp_path,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
            )
        )
    results = [(p.communicate(timeout=300), p.returncode) for p in processes]

    assert [code for _, code in results] == [0, 0, 0, 0], [err for (_, err), _ in results]
    runs = ln.Run.filter(reference__startswith=f"http://airflow/{run_id}/par")
    assert runs.count() == 4
    assert len({run.transform.uid for run in runs}) == 1
    assert ln.Transform.filter(key=dag.name).count() == 1


def test_sibling_scopes_have_flow_runs_of_their_own(unique_dag_file, run_id):
    import lamindb as ln

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source, flow_run_reference

    dag = unique_dag_file()
    default_ref = flow_run_reference(dag.dag_id, run_id)
    enrich_ref = flow_run_reference(dag.dag_id, run_id, "enrich")
    assert default_ref != enrich_ref
    default = runtime.start_flow_run(default_ref, dag.fileloc, dag.dag_id, {})
    enrich = runtime.start_flow_run(enrich_ref, dag.fileloc, dag.dag_id, {})
    assert default.uid != enrich.uid
    assert default.transform.uid == enrich.transform.uid

    def step_in_scope():
        return 1

    runtime.run_step(step_in_scope, (), {}, enrich_ref, dag_source(dag))
    assert ln.Run.filter(initiated_by_run=enrich).count() == 1
    assert ln.Run.filter(initiated_by_run=default).count() == 0

    runtime.finish_flow_run(enrich_ref, False)
    runtime.finish_flow_run(default_ref, True)
    assert (ln.Run.get(uid=enrich.uid).status, ln.Run.get(uid=default.uid).status) == ("errored", "completed")


def test_scoped_step_without_its_flow_run_does_not_fall_back_to_standalone(unique_dag_file, run_id):
    import lamindb as ln

    dag = unique_dag_file()

    def never():
        raise AssertionError("must not run")

    from airflow.providers.lamindb.utils import runtime
    from airflow.providers.lamindb.utils.dag_run import dag_source

    runs_before = ln.Run.filter().count()
    with pytest.raises(LookupError, match="No LaminDB flow run found"):
        runtime.run_step(never, (), {}, f"{dag.dag_id}/{run_id}/enrich", dag_source(dag), fileloc=dag.fileloc)
    assert ln.Run.filter().count() == runs_before
