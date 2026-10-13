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
    assert imported <= {"datetime", "inspect", "pathlib", "lamindb"}
    top_level = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(node.module == "datetime" for node in top_level if isinstance(node, ast.ImportFrom))
    assert not any(isinstance(node, ast.Import) for node in top_level)


def test_connect_uses_instance_only_when_given(fake_lamindb: MagicMock) -> None:
    assert runtime.connect() is fake_lamindb
    fake_lamindb.connect.assert_not_called()
    runtime.connect("owner/name")
    fake_lamindb.connect.assert_called_once_with("owner/name")


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
