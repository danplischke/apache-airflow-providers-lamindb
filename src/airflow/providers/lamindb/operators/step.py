"""Run a callable as a LaminDB step in the worker process."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.sdk import BaseOperator

from airflow.providers.lamindb.operators.flow import wire_flow_tasks


class LaminDBStepOperator(BaseOperator):
    """Run ``python_callable`` as a LaminDB step of this DAG run's flow run.

    The callable is wrapped with ``ln.step()`` at execution time, so lamindb records
    its source, parameters and outcome. A callable already decorated with
    ``@ln.step()`` is used as is. Needs lamindb on the worker; see
    ``@task.lamindb_venv`` otherwise.

    :param auto_flow: wire ``init >> step >> finish`` with the DAG's flow operators,
        adding in-process ones if the DAG has none yet. Pass ``False`` to wire the
        flow operators yourself. Mapped steps (``.expand()``) are never auto-wired.
    :param track: record the call in LaminDB. Pass ``False`` to run ``python_callable``
        as a plain Python task: no step run, no flow wiring, no lamindb needed. A
        callable decorated with ``@ln.step()`` is unwrapped and called directly.
    """

    template_fields = ("op_args", "op_kwargs")

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        auto_flow: bool = True,
        track: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = list(op_args or [])
        self.op_kwargs = dict(op_kwargs or {})
        self.track = track
        if track and auto_flow:
            wire_flow_tasks(self, venv=False)

    def execute(self, context: Any) -> Any:
        from airflow.providers.lamindb.utils.context import require_flow_run, require_lamindb, run_as_step, untracked

        if not self.track:
            return untracked(self.python_callable)(*self.op_args, **self.op_kwargs)
        require_lamindb()
        flow_run = require_flow_run(context)
        return run_as_step(self.python_callable, flow_run, *self.op_args, **self.op_kwargs)
