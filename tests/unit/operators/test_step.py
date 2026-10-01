from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from airflow.providers.lamindb.operators.step import LaminDBStepOperator
from airflow.providers.lamindb.utils.context import flow_run_context


def test_step_operator_runs_callable_as_step_of_flow_run(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    fake_lamindb.step.return_value = lambda fn: fn
    seen = {}

    def add(x: int, *, y: int) -> int:
        seen["global_run"] = fake_lamindb.context.run
        return x + y

    op = LaminDBStepOperator(task_id="step", python_callable=add, op_args=[1], op_kwargs={"y": 2})

    assert op.execute(make_context()) == 3
    assert seen["global_run"] is flow_run
    assert fake_lamindb.context.run is None
    fake_lamindb.step.assert_called_once_with()


def test_step_operator_without_flow_run_raises(fake_lamindb: MagicMock, make_context) -> None:
    op = LaminDBStepOperator(task_id="step", python_callable=lambda: None)
    with pytest.raises(AirflowException, match="No LaminDB flow run"):
        op.execute(make_context())


def test_step_operator_templates_args() -> None:
    assert set(LaminDBStepOperator.template_fields) >= {"op_args", "op_kwargs"}


def test_flow_run_context_refuses_to_clobber_other_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.context._run = MagicMock(uid="other")
    with pytest.raises(AirflowException, match="already set"), flow_run_context(MagicMock(uid="flow")):
        pass


def test_step_operator_untracked_runs_plain_callable(fake_lamindb: MagicMock, make_context) -> None:
    op = LaminDBStepOperator(
        task_id="step", python_callable=lambda x, *, y: x + y, op_args=[1], op_kwargs={"y": 2}, track=False
    )

    assert op.execute(make_context()) == 3  # no flow run needed
    fake_lamindb.step.assert_not_called()
    assert fake_lamindb.context.run is None


def test_step_operator_untracked_unwraps_lamindb_step() -> None:
    def add(x: int) -> int:
        return x + 1

    def wrapper_tracked(*args, **kwargs):
        raise AssertionError("tracked wrapper must not run")

    # what @ln.step() returns: a functools.wraps'd wrapper defined in lamindb
    wrapper_tracked.__code__ = wrapper_tracked.__code__.replace(co_filename="/site-packages/lamindb/core/_context.py")
    wrapper_tracked.__wrapped__ = add

    op = LaminDBStepOperator(task_id="step", python_callable=wrapper_tracked, op_args=[1], track=False)
    assert op.execute({}) == 2


def test_step_operator_untracked_is_not_wired_to_flow() -> None:
    from airflow.sdk import DAG

    with DAG("my_dag") as dag:
        LaminDBStepOperator(task_id="step", python_callable=lambda: None, track=False)
    assert dag.task_ids == ["step"]
