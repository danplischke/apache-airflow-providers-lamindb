from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.lamindb.utils import remote
from airflow.providers.lamindb.utils.remote import (
    add_lamindb_requirement,
    build_remote_flow_source,
    build_remote_step_source,
)

CONFIG = {
    "flow_run_reference": "my_dag/run_1",
    "source_code": "# dag source",
    "step_reference": "http://ti",
    "instance": "owner/name",
}


def test_remote_step_source_runs_user_function_as_step(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(**{"transform.key": "my_dag.py"})
    source = build_remote_step_source(
        user_source="def extract(count=10):\n    return {'count': count}\n", callable_name="extract", config=CONFIG
    )
    namespace: dict = {}
    exec(source, namespace)  # what the virtualenv/pod script does, minus the template

    assert namespace["extract"](count=4) == {"count": 4}
    fake_lamindb.connect.assert_called_once_with("owner/name")
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["key"] == "my_dag.py"
    assert kwargs["source_code"] == "# dag source"
    assert kwargs["entrypoint"] == "extract"
    assert kwargs["params"] == {"count": 4}
    assert kwargs["initiated_by_run"] is flow_run


def test_remote_step_source_does_not_leak_runtime_names(fake_lamindb: MagicMock) -> None:
    """The runtime lives in its own module, so it cannot clash with user names."""
    source = build_remote_step_source(
        user_source="def run_step():\n    return 1\n", callable_name="run_step", config={}
    )
    namespace: dict = {}
    exec(source, namespace)
    assert not {"connect", "finish_run", "STATUS_COMPLETED"} & namespace.keys()


def test_remote_flow_source_calls_runtime_and_returns_run_uid(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    source = build_remote_flow_source(function_name="flow_fn", runtime_function="finish_flow_run")
    namespace: dict = {}
    exec(source, namespace)

    assert namespace["flow_fn"]({"reference": "my_dag/run_1", "success": False, "instance": "owner/name"}) == "flowuid"
    fake_lamindb.connect.assert_called_once_with("owner/name")
    assert flow_run._status_code == 1


@pytest.mark.parametrize(
    ("requirements", "expected"),
    [
        (["pandas"], ["pandas", "lamindb==9.9"]),
        (["lamindb"], ["lamindb"]),
        (["lamindb[bionty]>=1"], ["lamindb[bionty]>=1"]),
        (["LaminDB==1.0"], ["LaminDB==1.0"]),
        (["lamindb_setup"], ["lamindb_setup", "lamindb==9.9"]),
        (["lamindb-airflow"], ["lamindb-airflow", "lamindb==9.9"]),
    ],
)
def test_add_lamindb_requirement(requirements: list[str], expected: list[str]) -> None:
    with patch.object(remote, "worker_lamindb_version", return_value="9.9"):
        add_lamindb_requirement(requirements)
    assert requirements == expected


def test_add_lamindb_requirement_prefers_explicit_version_then_unpinned() -> None:
    requirements: list[str] = []
    add_lamindb_requirement(requirements, "1.2.3")
    assert requirements == ["lamindb==1.2.3"]

    requirements = []
    with patch.object(remote, "version", side_effect=PackageNotFoundError):
        add_lamindb_requirement(requirements)
    assert requirements == ["lamindb"]


def test_worker_instance_slug_without_lamindb(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(remote.importlib.util, "find_spec", lambda name: None)
    assert remote.worker_instance_slug() is None
