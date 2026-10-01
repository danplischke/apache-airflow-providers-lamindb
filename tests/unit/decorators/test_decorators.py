from __future__ import annotations

from importlib.metadata import version
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import dag, task

from lamindb_airflow.utils import remote


def _single_task(decorator, fn):
    @dag
    def test_dag():
        decorator(fn)()

    return test_dag().get_task("step")


def step():
    return 1


def fake_venv_execute(self, context):
    """Instead of a virtualenv, exec the shipped source here and call the step."""
    namespace: dict = {}
    exec(self.get_python_source(), namespace)
    return namespace["step"]()


def test_task_decorators_registered() -> None:
    assert all(hasattr(task, name) for name in ("lamindb", "lamindb_venv", "lamindb_k8s"))


def test_task_lamindb_builds_operator() -> None:
    op = _single_task(task.lamindb, step)
    assert type(op).__name__ == "LaminDBDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb"
    assert op.python_callable is step


def test_task_lamindb_keeps_xcom_args_and_dependencies() -> None:
    """Regression: the step operator must not reset op_args set by the TaskFlow decorator."""

    @dag
    def test_dag():
        @task
        def produce():
            return 1

        @task.lamindb
        def consume(value):
            return value

        consume(produce())

    consume_op = test_dag().get_task("consume")
    assert consume_op.upstream_task_ids == {"produce", "lamindb_flow_init"}
    assert len(consume_op.op_args) == 1


def test_task_lamindb_venv_adds_lamindb_requirement() -> None:
    op = _single_task(task.lamindb_venv(requirements=["pandas"]), step)
    assert type(op).__name__ == "LaminDBVenvDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_venv"
    assert op.requirements == ["pandas", f"lamindb=={version('lamindb')}"]
    assert _single_task(task.lamindb_venv(lamindb_version="1.0"), step).requirements == ["lamindb==1.0"]


def test_remote_step_ships_wrapped_source_only_during_execute(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = _single_task(task.lamindb_venv, step)
    plain = op.get_python_source()
    assert plain.startswith("def step():") and "_lamindb_airflow_step" not in plain

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(remote, "worker_instance_slug", return_value="owner/worker"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        assert op.execute(make_context()) == 1

    fake_lamindb.connect.assert_called_once_with("owner/worker")
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["source_code"] == "# dag source"
    assert kwargs["entrypoint"] == "step"
    assert kwargs["initiated_by_run"] is fake_lamindb.flow_run
    assert op.get_python_source() == plain


def test_untracked_remote_step_only_connects(fake_lamindb: MagicMock, make_context) -> None:
    op = _single_task(task.lamindb_venv(track=False), step)
    assert op.requirements == [f"lamindb=={version('lamindb')}"]

    with (
        patch.object(remote, "dag_source") as dag_source,
        patch.object(remote, "worker_instance_slug", return_value="owner/worker"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        assert op.execute(make_context()) == 1  # fake_lamindb.flow_run is None

    dag_source.assert_not_called()
    fake_lamindb.connect.assert_called_once_with("owner/worker")
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()


def test_task_lamindb_k8s_builds_operator() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_k8s(image="python:3.12", lamindb_instance="owner/name"), step)
    assert type(op).__name__ == "LaminDBK8sDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_k8s"
    assert op.lamindb_instance == "owner/name"


def test_task_lamindb_k8s_untracked_adds_no_flow_tasks() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_k8s(image="python:3.12", track=False), step)
    assert op.track is False
    assert set(op.get_dag().task_ids) == {"step"}
