from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock

import pytest

from airflow.providers.lamindb.utils import runtime


def test_runtime_is_self_contained() -> None:
    """The module is shipped as source to interpreters without Airflow or this package."""
    tree = ast.parse(inspect.getsource(runtime))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert imported <= {"datetime", "inspect", "logging", "pathlib", "re", "lamindb"}
    top_level = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(node.module == "datetime" for node in top_level if isinstance(node, ast.ImportFrom))
    assert not any(isinstance(node, ast.Import) for node in top_level)


def test_connect_uses_instance_only_when_given(fake_lamindb: MagicMock) -> None:
    assert runtime.connect() is fake_lamindb
    fake_lamindb.connect.assert_not_called()
    runtime.connect("owner/name")
    fake_lamindb.connect.assert_called_once_with("owner/name")


@pytest.mark.parametrize("version", ["2.8.0", "1.17.0"])
def test_connect_rejects_older_lamindb(fake_lamindb: MagicMock, version: str) -> None:
    fake_lamindb.__version__ = version
    with pytest.raises(
        RuntimeError, match=rf"lamindb {version} is installed, .* needs lamindb 2\.8\.1 or newer"
    ):
        runtime.connect("owner/name")
    fake_lamindb.connect.assert_not_called()


@pytest.mark.parametrize(
    ("version", "supported"),
    [
        ("2.8.1", True),
        ("2.8.1a3", True),  # pre-releases of 2.8.1 count as 2.8.1
        ("2.8.2", True),
        ("2.9", True),
        ("2.10.0", True),
        ("2.11.0", True),
        ("2.12.0.dev4+g1a2b3c", True),
        ("3.0.0", True),
        ("10.0", True),
        ("2.8.0", False),
        ("2.8", False),
        ("2.7.9", False),
        ("1.17.0", False),
        ("", False),
        ("unknown", False),
    ],
)
def test_lamindb_version_supported(version: str, supported: bool) -> None:
    assert runtime.lamindb_version_supported(version) is supported


def test_start_flow_run_tracks_dag_file_and_tags_run(fake_lamindb: MagicMock) -> None:
    run = runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {"a": 1}, instance="owner/name")

    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_called_once_with(
        path="/dags/my_dag.py", entrypoint="my_dag", params={"a": 1}, new_run=True, stream_tracking=False
    )
    assert run.reference == "my_dag/run_1"
    assert run.reference_type == runtime.FLOW_RUN_REFERENCE_TYPE
    run.save.assert_called()
    assert fake_lamindb.context.run is None
    run.transform._update_source_code_from_path.assert_not_called()


def test_start_flow_run_saves_source_of_new_transform(fake_lamindb: MagicMock) -> None:
    def track(**kwargs) -> None:
        fake_lamindb.context._run = MagicMock(**{"transform.hash": None})

    fake_lamindb.track.side_effect = track
    run = runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {})
    run.transform._update_source_code_from_path.assert_called_once()
    run.transform.save.assert_called_once()


def test_start_flow_run_restarts_existing_run_on_retry(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = existing = MagicMock(finished_at="yesterday")
    assert runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {}) is existing
    fake_lamindb.track.assert_not_called()
    assert existing._status_code == runtime.STATUS_RESTARTED
    assert existing.finished_at is None
    fake_lamindb.Run.filter.assert_called_with(
        reference="my_dag/run_1", reference_type=runtime.FLOW_RUN_REFERENCE_TYPE
    )


def test_start_flow_run_refuses_to_clobber_global_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.context._run = MagicMock()
    with pytest.raises(RuntimeError, match="already set"):
        runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {})


@pytest.mark.parametrize(
    ("success", "status"), [(True, runtime.STATUS_COMPLETED), (False, runtime.STATUS_ERRORED)]
)
def test_finish_flow_run(fake_lamindb: MagicMock, success: bool, status: int) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(finished_at=None)
    assert runtime.finish_flow_run("my_dag/run_1", success) is flow_run
    assert flow_run._status_code == status
    assert flow_run.finished_at is not None
    flow_run.save.assert_called_once()


def test_finish_flow_run_without_flow_run_raises(fake_lamindb: MagicMock) -> None:
    with pytest.raises(LookupError, match="No LaminDB flow run found for DAG run 'my_dag/run_1'"):
        runtime.finish_flow_run("my_dag/run_1", True)


def test_run_step_tracks_step_under_flow_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(**{"transform.key": "my_dag.py"})

    def extract(count, scale=2):
        assert fake_lamindb.context.run is not None
        return count * scale

    result = runtime.run_step(
        extract, (3,), {}, "my_dag/run_1", "# dag source", step_reference="http://ti", instance="owner/name"
    )

    assert result == 6
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_called_once_with(
        key="my_dag.py",
        source_code="# dag source",
        kind="script",
        entrypoint="extract",
        params={"count": 3, "scale": 2},
        initiated_by_run=flow_run,
        new_run=True,
        stream_tracking=False,
    )
    assert fake_lamindb.context.run is None


def test_run_step_records_reference_and_status(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock()
    runs = []

    def track(**kwargs) -> None:
        runs.append(MagicMock())
        fake_lamindb.context._run = runs[-1]

    fake_lamindb.track.side_effect = track

    runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", step_reference="http://ti")
    runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", step_reference="x" * 256)

    assert runs[0].reference == "http://ti"
    assert runs[0].reference_type == runtime.STEP_RUN_REFERENCE_TYPE
    assert runs[0]._status_code == runtime.STATUS_COMPLETED
    assert not isinstance(runs[1].reference, str)  # too long for Run.reference: left unset


def test_run_step_marks_run_errored_and_reraises(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock()

    def boom():
        raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        runtime.run_step(boom, (), {}, "my_dag/run_1", "")
    assert fake_lamindb.context.run._status_code == runtime.STATUS_ERRORED


def test_run_step_without_flow_run_raises(fake_lamindb: MagicMock) -> None:
    with pytest.raises(LookupError, match="No LaminDB flow run"):
        runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "")
    fake_lamindb.track.assert_not_called()


def test_run_step_leaves_out_excluded_params_but_passes_them(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock(**{"transform.key": "my_dag.py"})
    received = {}

    def load(user, password, *, retries=3, **options):
        received.update(user=user, password=password, retries=retries, **options)
        return "ok"

    result = runtime.run_step(
        load,
        ("alice", "s3cret"),
        {"token": "t0ken", "region": "eu"},
        "my_dag/run_1",
        "",
        exclude_params=["password", "token"],
    )

    assert result == "ok"
    assert received == {"user": "alice", "password": "s3cret", "retries": 3, "token": "t0ken", "region": "eu"}
    params = fake_lamindb.track.call_args.kwargs["params"]
    assert params == {"user": "alice", "retries": 3, "options": {"region": "eu"}}


@pytest.fixture
def lamindb_inference(monkeypatch: pytest.MonkeyPatch, fake_lamindb: MagicMock):
    """``lamindb.models`` with a stand-in for lamindb's parameter type inference."""
    import sys
    import types

    class SQLRecord:
        pass

    def infer_convert_dtype_key_value(key, value, mute=False):
        assert mute
        if isinstance(value, (int, str)):
            return type(value).__name__, value, ""
        if isinstance(value, list) and all(isinstance(item, SQLRecord) for item in value):
            return "list[cat[ULabel]]", value, ""
        return "?", value, ""

    models = types.ModuleType("lamindb.models")
    models.SQLRecord = SQLRecord  # type: ignore[attr-defined]
    feature_manager = types.ModuleType("lamindb.models._feature_manager")
    feature_manager.infer_convert_dtype_key_value = infer_convert_dtype_key_value  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lamindb.models", models)
    monkeypatch.setitem(sys.modules, "lamindb.models._feature_manager", feature_manager)
    return SQLRecord


def test_step_params_drops_values_lamindb_cannot_record_without_logging_them(
    lamindb_inference, caplog: pytest.LogCaptureFixture
) -> None:
    class Client:
        def __repr__(self) -> str:
            return "Client(password='s3cret')"

    record = lamindb_inference()

    def step(count, name, client, label, labels, missing=None):
        pass

    params = runtime.step_params(step, (1, "x", Client(), record, [record]), {})

    assert params == {"count": 1, "name": "x", "label": record, "labels": [record], "missing": None}
    assert "Not recording parameter 'client' of type Client" in caplog.text
    assert "s3cret" not in caplog.text


def test_step_params_keeps_everything_when_lamindb_cannot_be_asked(fake_lamindb: MagicMock) -> None:
    def step(client):
        pass

    client = object()
    assert runtime.step_params(step, (client,), {}) == {"client": client}


def test_run_connected_connects_without_recording(fake_lamindb: MagicMock) -> None:
    assert runtime.run_connected(lambda a, b=2: a + b, (1,), {"b": 3}, instance="owner/name") == 4
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()
