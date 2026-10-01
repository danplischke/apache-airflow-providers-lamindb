from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

from lamindb_airflow.operators.step import LaminDBStepOperator
from lamindb_airflow.utils.context import flow_run_context


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


def test_untracked_step_operator_needs_no_flow_run_and_records_nothing(fake_lamindb: MagicMock, make_context) -> None:
    def add(x: int, *, y: int) -> int:
        return x + y

    op = LaminDBStepOperator(task_id="step", python_callable=add, op_args=[1], op_kwargs={"y": 2}, track=False)

    assert op.execute(make_context()) == 3  # fake_lamindb.flow_run is None
    fake_lamindb.step.assert_not_called()
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()


def test_step_operator_templates_args() -> None:
    assert set(LaminDBStepOperator.template_fields) >= {"op_args", "op_kwargs"}


def test_flow_run_context_refuses_to_clobber_other_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.context._run = MagicMock(uid="other")
    with pytest.raises(AirflowException, match="already set"), flow_run_context(MagicMock(uid="flow")):
        pass
