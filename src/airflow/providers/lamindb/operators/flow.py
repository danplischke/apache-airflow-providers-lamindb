"""Operators that open and close the LaminDB flow run of a DAG run."""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import BaseOperator, TriggerRule

from airflow.providers.lamindb.utils.dag_run import (
    context_flow_run_reference,
    dag_and_run_id,
    dag_run_failed,
    flow_run_params,
)
from airflow.providers.lamindb.utils.remote import (
    add_lamindb_requirement,
    build_remote_flow_source,
    worker_instance_slug,
)

# Airflow's BaseOperatorMeta only wraps __init__ of BaseOperator subclasses and expects
# the most-derived class to define one, so each operator repeats its own __init__
# rather than inheriting it from a shared mixin.


class LaminDBFlowInitOperator(BaseOperator):
    """Start the LaminDB flow run for this DAG run, in the worker process.

    Creates (or restarts, on retry) a ``Run`` of the DAG file's ``Transform`` and tags
    it with the DAG run so downstream LaminDB steps can find it. Step operators add
    and wire one automatically; declare it yourself (before the steps) to configure
    it. Returns the flow run uid.

    Marked as an Airflow *setup* task by default, pairing with the *teardown*
    ``LaminDBFlowFinishOperator``; pass ``is_setup=False`` to opt out. Needs lamindb
    on the worker; see ``LaminDBVenvFlowInitOperator`` otherwise.
    """

    def __init__(self, *, task_id: str = "lamindb_flow_init", is_setup: bool = True, **kwargs: Any) -> None:
        super().__init__(task_id=task_id, **kwargs)
        if is_setup:
            self.as_setup()

    def execute(self, context: Any) -> str:
        from airflow.providers.lamindb.utils.context import require_lamindb, start_flow_run

        require_lamindb()
        return start_flow_run(context).uid


class LaminDBFlowFinishOperator(BaseOperator):
    """Close the LaminDB flow run with the DAG run's outcome, in the worker process.

    Marked as an Airflow *teardown* task by default: it runs after every other task
    (``trigger_rule=all_done_setup_success``) and is ignored when Airflow decides the
    DAG run state, so a failed step still fails the DAG run. Marks the flow run
    errored if any other task in the DAG run failed. Step operators wire it
    downstream of themselves (``init >> steps >> finish``). Pass ``is_teardown=False`` together
    with your own ``trigger_rule`` to opt out. Needs lamindb on the worker; see
    ``LaminDBVenvFlowFinishOperator`` otherwise.
    """

    def __init__(self, *, task_id: str = "lamindb_flow_finish", is_teardown: bool = True, **kwargs: Any) -> None:
        if not is_teardown:
            kwargs.setdefault("trigger_rule", TriggerRule.ALL_DONE)
        super().__init__(task_id=task_id, **kwargs)
        if is_teardown:
            self.as_teardown()

    def execute(self, context: Any) -> str:
        from airflow.providers.lamindb.utils.context import require_flow_run, require_lamindb
        from airflow.providers.lamindb.utils.runtime import finish_run

        require_lamindb()
        flow_run = require_flow_run(context)
        finish_run(flow_run, not dag_run_failed(context, self.task_id, self.log))
        return flow_run.uid


def lamindb_airflow_flow_task(lamindb_airflow_config: dict[str, Any]) -> str:
    """Placeholder callable; the venv flow operators ship generated source under this name."""
    raise NotImplementedError


class _LaminDBVenvFlowOperator(PythonVirtualenvOperator):
    """Run a :mod:`airflow.providers.lamindb.utils.runtime` function inside a virtualenv.

    Accepts every ``PythonVirtualenvOperator`` argument except ``python_callable``,
    ``op_args`` and ``op_kwargs``. ``lamindb`` is added to ``requirements`` (see
    ``lamindb_version``) unless listed.

    :param lamindb_instance: instance slug (``owner/name``) to connect to in the
        virtualenv. Defaults to the worker's instance if lamindb is set up there,
        else to lamindb's own default (``LAMIN_CURRENT_INSTANCE``).
    :param lamindb_version: lamindb version to install. Defaults to the worker's
        version if installed, else the latest.
    """

    _runtime_function: str

    def __init__(
        self,
        *,
        lamindb_instance: str | None = None,
        lamindb_version: str | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("expect_airflow", False)
        super().__init__(python_callable=lamindb_airflow_flow_task, **kwargs)
        self.lamindb_instance = lamindb_instance
        add_lamindb_requirement(self.requirements, lamindb_version)

    def get_python_source(self) -> str:
        return build_remote_flow_source(
            function_name=self.python_callable.__name__, runtime_function=self._runtime_function
        )

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        raise NotImplementedError

    def execute(self, context: Any) -> Any:
        # a single named argument: a **kwargs signature would make PythonOperator pass the whole context
        self.op_kwargs = {
            "lamindb_airflow_config": {
                "instance": self.lamindb_instance or worker_instance_slug(),
                **self._runtime_kwargs(context),
            }
        }
        return super().execute(context)


class LaminDBVenvFlowInitOperator(_LaminDBVenvFlowOperator):
    """``LaminDBFlowInitOperator`` that talks to LaminDB from a virtualenv.

    The worker needs no lamindb. Accepts every ``PythonVirtualenvOperator`` argument
    except ``python_callable``, ``op_args`` and ``op_kwargs``.
    """

    _runtime_function = "start_flow_run"

    def __init__(self, *, task_id: str = "lamindb_flow_init", is_setup: bool = True, **kwargs: Any) -> None:
        super().__init__(task_id=task_id, **kwargs)
        if is_setup:
            self.as_setup()

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        dag_id, _ = dag_and_run_id(context)
        return {
            "reference": context_flow_run_reference(context),
            "path": context["dag"].fileloc,
            "entrypoint": dag_id,
            "params": flow_run_params(context),
        }


class LaminDBVenvFlowFinishOperator(_LaminDBVenvFlowOperator):
    """``LaminDBFlowFinishOperator`` that talks to LaminDB from a virtualenv.

    The worker needs no lamindb. Accepts every ``PythonVirtualenvOperator`` argument
    except ``python_callable``, ``op_args`` and ``op_kwargs``.
    """

    _runtime_function = "finish_flow_run"

    def __init__(self, *, task_id: str = "lamindb_flow_finish", is_teardown: bool = True, **kwargs: Any) -> None:
        if not is_teardown:
            kwargs.setdefault("trigger_rule", TriggerRule.ALL_DONE)
        super().__init__(task_id=task_id, **kwargs)
        if is_teardown:
            self.as_teardown()

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        return {
            "reference": context_flow_run_reference(context),
            "success": not dag_run_failed(context, self.task_id, self.log),
        }


_INIT_OPERATORS = (LaminDBFlowInitOperator, LaminDBVenvFlowInitOperator)
_FINISH_OPERATORS = (LaminDBFlowFinishOperator, LaminDBVenvFlowFinishOperator)


def wire_flow_tasks(step: BaseOperator, *, venv: bool, **flow_kwargs: Any) -> None:
    """Wire ``init >> step >> finish``, adding the DAG's flow init/finish tasks on first use.

    Called by the step operators at DAG parse time. Flow operators already in the DAG
    are reused, so the first LaminDB step decides whether they run in the worker
    process or in a virtualenv (``venv``, built with ``flow_kwargs``). Declare the flow
    operators yourself before the steps to control that.
    """
    dag = step.get_dag()
    # tasks unmapped at run time were wired as a mapped task at parse time, if at all
    if dag is None or getattr(step, "_BaseOperator__from_mapped", False):
        return
    init = next((t for t in dag.tasks if isinstance(t, _INIT_OPERATORS)), None)
    finish = next((t for t in dag.tasks if isinstance(t, _FINISH_OPERATORS)), None)
    # the root task group keeps the default task ids when the first step sits in a group
    if init is None:
        init_class = LaminDBVenvFlowInitOperator if venv else LaminDBFlowInitOperator
        init = init_class(dag=dag, task_group=dag.task_group, **flow_kwargs)
    if finish is None:
        finish_class = LaminDBVenvFlowFinishOperator if venv else LaminDBFlowFinishOperator
        finish = finish_class(dag=dag, task_group=dag.task_group, **flow_kwargs)
        init >> finish
    init >> step >> finish
