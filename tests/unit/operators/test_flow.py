from __future__ import annotations

from importlib.metadata import version
from unittest.mock import MagicMock, patch

import pytest
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

from airflow.providers.lamindb.operators.flow import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
    LaminDBVenvFlowFinishOperator,
    LaminDBVenvFlowInitOperator,
)


def test_flow_init_starts_flow_run(fake_lamindb: MagicMock, make_context) -> None:
    run = MagicMock(uid="flowuid")

    def track(**kwargs) -> None:
        fake_lamindb.context._run = run

    fake_lamindb.track.side_effect = track

    assert LaminDBFlowInitOperator().execute(make_context()) == "flowuid"
    assert run.reference == "my_dag/run_1"
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["path"] == "/dags/my_dag.py"
    assert kwargs["entrypoint"] == "my_dag"
    assert kwargs["params"] == {"dag_id": "my_dag", "run_id": "run_1", "run_type": "manual", "conf": {"x": 1}}


def test_flow_init_refuses_to_clobber_global_run(fake_lamindb: MagicMock, make_context) -> None:
    from airflow.exceptions import AirflowException

    fake_lamindb.context._run = MagicMock()
    with pytest.raises(AirflowException, match="already set"):
        LaminDBFlowInitOperator().execute(make_context())


@pytest.mark.parametrize(
    ("task_states", "status"),
    [({"a": "success", "lamindb_flow_finish": "running"}, 0), ({"a": "success", "b": "upstream_failed"}, 1)],
)
def test_flow_finish_records_dag_run_outcome(fake_lamindb: MagicMock, make_context, task_states, status) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    context = make_context(task_states=task_states)

    assert LaminDBFlowFinishOperator().execute(context) == "flowuid"
    assert flow_run._status_code == status
    context["ti"].get_task_states.assert_called_once_with(dag_id="my_dag", run_ids=["run_1"])


def test_flow_finish_without_flow_run_raises(fake_lamindb: MagicMock, make_context) -> None:
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="No LaminDB flow run"):
        LaminDBFlowFinishOperator().execute(make_context())


@pytest.mark.parametrize(
    ("init_cls", "finish_cls"),
    [
        (LaminDBFlowInitOperator, LaminDBFlowFinishOperator),
        (LaminDBVenvFlowInitOperator, LaminDBVenvFlowFinishOperator),
    ],
)
def test_flow_operators_are_setup_and_teardown(init_cls, finish_cls) -> None:
    init, finish = init_cls(), finish_cls()
    assert (init.task_id, finish.task_id) == ("lamindb_flow_init", "lamindb_flow_finish")
    assert init.is_setup and not init.is_teardown
    assert finish.is_teardown and finish.trigger_rule == "all_done_setup_success"
    plain = finish_cls(is_teardown=False)
    assert not plain.is_teardown and plain.trigger_rule == "all_done"
    assert not init_cls(is_setup=False).is_setup


def test_venv_flow_operator_requirements() -> None:
    op = LaminDBVenvFlowInitOperator(requirements=["pandas"])
    assert op.requirements == ["pandas", f"lamindb=={version('lamindb')}"]
    assert op.expect_airflow is False
    assert LaminDBVenvFlowFinishOperator(lamindb_version="1.0").requirements == ["lamindb==1.0"]
    assert LaminDBVenvFlowFinishOperator(requirements=["lamindb>=1"]).requirements == ["lamindb>=1"]


def _run_in_fake_venv(op, context, fake_lamindb):
    """Execute ``op`` but, instead of a virtualenv, exec its shipped source here."""

    def fake_venv_execute(self, context):
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace[self.python_callable.__name__](**self.op_kwargs)

    with (
        patch.object(PythonVirtualenvOperator, "execute", fake_venv_execute),
        patch("airflow.providers.lamindb.operators.flow.worker_instance_slug", return_value="owner/worker"),
    ):
        return op.execute(context)


def test_venv_flow_init_starts_flow_run_in_venv(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")  # restart path: returns the existing run
    op = LaminDBVenvFlowInitOperator()

    assert _run_in_fake_venv(op, make_context(), fake_lamindb) == "flowuid"
    fake_lamindb.connect.assert_called_once_with("owner/worker")
    config = op.op_kwargs["lamindb_airflow_config"]
    assert config["reference"] == "my_dag/run_1"
    assert config["path"] == "/dags/my_dag.py"
    assert config["entrypoint"] == "my_dag"
    assert config["params"]["run_id"] == "run_1"


def test_venv_flow_finish_closes_flow_run_in_venv(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    op = LaminDBVenvFlowFinishOperator(lamindb_instance="owner/explicit")

    assert _run_in_fake_venv(op, make_context(task_states={"b": "failed"}), fake_lamindb) == "flowuid"
    fake_lamindb.connect.assert_called_once_with("owner/explicit")
    assert op.op_kwargs == {
        "lamindb_airflow_config": {"instance": "owner/explicit", "reference": "my_dag/run_1", "success": False}
    }
    assert flow_run._status_code == 1
