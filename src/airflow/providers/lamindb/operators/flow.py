"""Operators that open and close the LaminDB flow run of a DAG run.

Both run lamindb inside a virtualenv, so the worker needs no lamindb.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.utils.dag_run import (
    context_flow_run_reference,
    dag_and_run_id,
    dag_run_failed,
    flow_run_params,
)
from airflow.providers.lamindb.utils.remote import (
    RemoteLaminDB,
    build_remote_flow_source,
    check_lamindb_version,
    ensure_lamindb_requirement,
    lamindb_virtualenv_env,
    space_name,
)
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import TriggerRule

if TYPE_CHECKING:
    from airflow.sdk import BaseOperator

# Airflow's BaseOperatorMeta only wraps __init__ of BaseOperator subclasses and expects
# the most-derived class to define one, so each operator repeats its own __init__
# rather than inheriting it from a shared mixin.


FLOW_INSTANCE_XCOM_KEY = "lamindb_instance"
"""XCom key under which the flow init task records the instance of the flow run."""


def lamindb_airflow_flow_task(lamindb_airflow_config: dict[str, Any]) -> str:
    """Placeholder callable; the flow operators ship generated source under this name."""
    raise NotImplementedError


class _LaminDBFlowOperator(PythonVirtualenvOperator):
    """Run a ``airflow.providers.lamindb.utils.runtime`` function inside a virtualenv.

    Accepts every ``PythonVirtualenvOperator`` argument except ``python_callable``,
    ``op_args`` and ``op_kwargs``. ``lamindb-core`` is added to ``requirements`` (see
    ``lamindb_version``) unless lamindb is listed.

    :param lamindb_conn_id: Airflow connection of type ``lamindb`` with the Lamin API key
        and the instance. The virtualenv gets them instead of the worker's ``~/.lamin``.
        ``None`` uses lamindb's own configuration (``LAMIN_API_KEY``,
        ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``) instead. Templated.
    :param lamindb_instance: instance slug (``owner/name``) to connect to; overrides the
        connection's instance. Templated.
    :param lamindb_branch: name or uid of the branch the flow run is on; steps default to it. The DAG
        file's transform stays on the branch lamindb finds it on. Templated; only used by the init
        operator.
    :param lamindb_space: name or uid of the space the flow run (and the DAG file's transform,
        when first recorded) is created in; steps default to it. Templated; only used by the init
        operator.
    :param lamindb_version: lamindb version to install, 2.8.1 or newer. Defaults to the
        instance's version on LaminHub (with a connection), else the latest. Templated.
    :param lamindb_cache_dir: absolute path of a lamindb cache shared between tasks, set as
        ``LAMIN_CACHE_DIR``; overrides the connection's ``cache_dir`` extra. Templated.
    """

    template_fields: Sequence[str] = (
        *PythonVirtualenvOperator.template_fields,
        "lamindb_conn_id",
        "lamindb_instance",
        "lamindb_space",
        "lamindb_branch",
        "lamindb_version",
        "lamindb_cache_dir",
    )
    _runtime_function: str

    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        check_lamindb_version(lamindb_version)
        kwargs.setdefault("expect_airflow", False)
        super().__init__(python_callable=lamindb_airflow_flow_task, **kwargs)
        self.lamindb_conn_id = lamindb_conn_id
        self.lamindb_instance = lamindb_instance
        self.lamindb_space = lamindb_space
        self.lamindb_branch = lamindb_branch
        self.lamindb_version = lamindb_version
        self.lamindb_cache_dir = lamindb_cache_dir

    def get_python_source(self) -> str:
        return build_remote_flow_source(
            function_name=self.python_callable.__name__, runtime_function=self._runtime_function
        )

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        raise NotImplementedError

    def execute(self, context: Any) -> Any:
        remote = RemoteLaminDB.resolve(self.lamindb_conn_id, self.lamindb_instance, self.lamindb_cache_dir)
        ensure_lamindb_requirement(self, remote, self.lamindb_version)
        # a single named argument: a **kwargs signature would make PythonOperator pass the whole context
        self.op_kwargs = {
            "lamindb_airflow_config": {"instance": remote.instance, **self._runtime_kwargs(context)}
        }
        with lamindb_virtualenv_env(self, remote):
            result = super().execute(context)
        self._after_execute(context, remote)
        return result

    def _after_execute(self, context: Any, remote: RemoteLaminDB) -> None:
        pass


class LaminDBFlowInitOperator(_LaminDBFlowOperator):
    """Start the LaminDB flow run for this DAG run, in a virtualenv.

    Creates (or restarts, on retry) a ``Run`` of the DAG file's ``Transform`` and tags
    it with the DAG run so downstream LaminDB steps can find it. Wrap the steps in
    ``with init >> finish:`` to wire them, or let steps with ``auto_flow=True`` add
    and wire one. Returns the flow run uid.

    Marked as an Airflow *setup* task by default, pairing with the *teardown*
    ``LaminDBFlowFinishOperator``; pass ``is_setup=False`` to opt out. Accepts
    every ``PythonVirtualenvOperator`` argument except ``python_callable``,
    ``op_args`` and ``op_kwargs``.
    """

    _runtime_function = "start_flow_run"

    def __init__(
        self,
        *,
        task_id: str = "lamindb_flow_init",
        is_setup: bool = True,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            task_id=task_id,
            lamindb_conn_id=lamindb_conn_id,
            lamindb_instance=lamindb_instance,
            lamindb_space=lamindb_space,
            lamindb_branch=lamindb_branch,
            lamindb_version=lamindb_version,
            lamindb_cache_dir=lamindb_cache_dir,
            **kwargs,
        )
        if is_setup:
            self.as_setup()

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        dag_id, _ = dag_and_run_id(context)
        return {
            "reference": context_flow_run_reference(context),
            "path": context["dag"].fileloc,
            "entrypoint": dag_id,
            "params": flow_run_params(context),
            "space": space_name(self.lamindb_space),
            "branch": space_name(self.lamindb_branch),
        }

    def _after_execute(self, context: Any, remote: RemoteLaminDB) -> None:
        # lets steps that use another connection fail early with a clear message
        if remote.instance:
            context["ti"].xcom_push(key=FLOW_INSTANCE_XCOM_KEY, value=remote.instance)


class LaminDBFlowFinishOperator(_LaminDBFlowOperator):
    """Close the LaminDB flow run with the DAG run's outcome, in a virtualenv.

    Marked as an Airflow *teardown* task by default: it runs after every other task
    (``trigger_rule=all_done_setup_success``) and is ignored when Airflow decides the
    DAG run state, so a failed step still fails the DAG run. Marks the flow run
    errored if any other task in the DAG run failed. Pass ``is_teardown=False``
    together with your own ``trigger_rule`` to opt out. Accepts every
    ``PythonVirtualenvOperator`` argument except ``python_callable``, ``op_args`` and
    ``op_kwargs``.
    """

    _runtime_function = "finish_flow_run"

    def __init__(
        self,
        *,
        task_id: str = "lamindb_flow_finish",
        is_teardown: bool = True,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        **kwargs: Any,
    ) -> None:
        if not is_teardown:
            kwargs.setdefault("trigger_rule", TriggerRule.ALL_DONE)
        super().__init__(
            task_id=task_id,
            lamindb_conn_id=lamindb_conn_id,
            lamindb_instance=lamindb_instance,
            lamindb_space=lamindb_space,
            lamindb_branch=lamindb_branch,
            lamindb_version=lamindb_version,
            lamindb_cache_dir=lamindb_cache_dir,
            **kwargs,
        )
        if is_teardown:
            self.as_teardown()

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        return {
            "reference": context_flow_run_reference(context),
            "success": not dag_run_failed(context, self.task_id, self.log),
        }


def wire_flow_tasks(step: BaseOperator, **flow_kwargs: Any) -> None:
    """Wire ``init >> step >> finish``, adding the DAG's flow init/finish tasks on first use.

    Called at DAG parse time by step decorators with ``auto_flow=True``. Flow operators
    already in the DAG are reused, so the first LaminDB step decides their virtualenv
    and LaminDB settings (``flow_kwargs``). Declare the flow operators yourself before
    the steps to control them.
    """
    dag = step.get_dag()
    # tasks unmapped at run time were wired as a mapped task at parse time, if at all
    if dag is None or getattr(step, "_BaseOperator__from_mapped", False):
        return
    init = next((t for t in dag.tasks if isinstance(t, LaminDBFlowInitOperator)), None)
    finish = next((t for t in dag.tasks if isinstance(t, LaminDBFlowFinishOperator)), None)
    # the root task group keeps the default task ids when the first step sits in a group
    if init is None:
        init = LaminDBFlowInitOperator(dag=dag, task_group=dag.task_group, **flow_kwargs)
    if finish is None:
        finish = LaminDBFlowFinishOperator(dag=dag, task_group=dag.task_group, **flow_kwargs)
        init >> finish
    init >> step >> finish
