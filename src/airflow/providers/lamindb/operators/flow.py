"""Operators that open and close a LaminDB flow run of a DAG run.

Both run lamindb inside a virtualenv, so the worker needs no lamindb. ``LaminDBFlow`` adds them to a
DAG around its tasks; they do not make any task a child of the flow run by being in the DAG.
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from contextvars import ContextVar
from typing import Any

from airflow.providers.lamindb.exceptions import LaminDBFlowWarning
from airflow.providers.lamindb.execution.config import FlowFinishConfig, FlowStartConfig
from airflow.providers.lamindb.execution.dag_run import (
    DEFAULT_SCOPE,
    context_flow_run_reference,
    dag_and_run_id,
    dag_run_failed,
    flow_run_params,
    flow_task_ids,
)
from airflow.providers.lamindb.execution.environment import (
    RemoteLaminDB,
    ensure_lamindb_requirement,
    lamindb_virtualenv_env,
)
from airflow.providers.lamindb.execution.parameters import space_name
from airflow.providers.lamindb.execution.requirements import check_lamindb_version
from airflow.providers.lamindb.execution.setup import (
    LaminDBSetup,
    resolve_setup,
    setup_bootstrap_source,
    setup_execution_env,
)
from airflow.providers.lamindb.execution.source import build_remote_flow_source
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import TriggerRule

# Airflow's BaseOperatorMeta only wraps __init__ of BaseOperator subclasses and expects
# the most-derived class to define one, so each operator repeats its own __init__
# rather than inheriting it from a shared mixin.


_CREATED_BY_LAMINDB_FLOW: ContextVar[bool] = ContextVar("lamindb_created_by_flow", default=False)
"""Set by ``LaminDBFlow`` while it adds its flow operators."""

FLOW_INSTANCE_XCOM_KEY = "lamindb_instance"
"""XCom key under which the flow init task records the instance of the flow run."""


def lamindb_airflow_flow_task(lamindb_airflow_config: dict[str, Any]) -> str:
    """Placeholder callable; the flow operators ship generated source under this name."""
    raise NotImplementedError


class _LaminDBFlowOperator(PythonVirtualenvOperator):
    """Run a ``airflow.providers.lamindb.execution.runtime`` function inside a virtualenv.

    Accepts every ``PythonVirtualenvOperator`` argument except ``python_callable``,
    ``op_args`` and ``op_kwargs``. ``lamindb`` is added to ``requirements`` (see
    ``lamindb_version``) unless ``lamindb`` or ``lamindb-core`` is listed.

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
    :param lamindb_setup: a [`LaminDBSetup`][airflow.providers.lamindb.execution.setup.LaminDBSetup]
        that prepares the virtualenv; the steps of the DAG must have the same one.
    :param lamindb_scope: the flow run to open or close: ``""`` (default) for the DAG run's, else the
        ``scope_id`` of a ``LaminDBFlow`` whose flow run it is. Tasks are children of the flow run of
        their ``LaminDBFlow`` scope, which sets this up; it is not an option of single tasks.
    """

    template_fields: Sequence[str] = (
        *PythonVirtualenvOperator.template_fields,
        "lamindb_scope",
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
        lamindb_setup: LaminDBSetup | None = None,
        lamindb_scope: str = DEFAULT_SCOPE,
        **kwargs: Any,
    ) -> None:
        check_lamindb_version(lamindb_version)
        kwargs.setdefault("expect_airflow", False)
        super().__init__(python_callable=lamindb_airflow_flow_task, **kwargs)
        if not _CREATED_BY_LAMINDB_FLOW.get():
            warnings.warn(
                f"{type(self).__name__} {self.task_id!r} is not part of a LaminDBFlow, so no task is a "
                "child of its flow run: LaminDB tasks are standalone unless they are created inside "
                "`with LaminDBFlow():`, which adds the flow operators itself. `with init >> finish:` and "
                "auto_flow no longer group tasks; replace them with LaminDBFlow.",
                LaminDBFlowWarning,
                stacklevel=3,
            )
        self.lamindb_scope = lamindb_scope
        self.lamindb_conn_id = lamindb_conn_id
        self.lamindb_instance = lamindb_instance
        self.lamindb_space = lamindb_space
        self.lamindb_branch = lamindb_branch
        self.lamindb_version = lamindb_version
        self.lamindb_cache_dir = lamindb_cache_dir
        self.lamindb_setup = lamindb_setup
        resolve_setup(lamindb_setup)  # a broken [lamindb] setup_class fails when the DAG is parsed

    def get_python_source(self) -> str:
        return build_remote_flow_source(
            function_name=self.python_callable.__name__,
            runtime_function=self._runtime_function,
            bootstrap=setup_bootstrap_source(self.lamindb_setup),
        )

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        raise NotImplementedError

    def execute(self, context: Any) -> Any:
        remote = RemoteLaminDB.resolve(self.lamindb_conn_id, self.lamindb_instance, self.lamindb_cache_dir)
        ensure_lamindb_requirement(self, remote, self.lamindb_version)
        # a single named argument: a **kwargs signature would make PythonOperator pass the whole context
        config_models: dict[str, type[FlowStartConfig] | type[FlowFinishConfig]] = {
            "start_flow_run": FlowStartConfig,
            "finish_flow_run": FlowFinishConfig,
        }
        config_model = config_models[self._runtime_function]
        config = config_model.model_validate({"instance": remote.instance, **self._runtime_kwargs(context)})
        self.op_kwargs = {"lamindb_airflow_config": config.model_dump(exclude_unset=True)}
        with lamindb_virtualenv_env(self, remote, self._setup_env(context)):
            result = super().execute(context)
        self._after_execute(context, remote)
        return result

    def _setup_env(self, context: Any) -> dict[str, str]:
        return setup_execution_env(self.lamindb_setup, context)

    def _after_execute(self, context: Any, remote: RemoteLaminDB) -> None:
        pass


class LaminDBFlowInitOperator(_LaminDBFlowOperator):
    """Start the LaminDB flow run for this DAG run, in a virtualenv.

    Creates (or restarts, on retry) a ``Run`` of the DAG file's ``Transform`` and tags
    it with the DAG run (and ``lamindb_scope``) so the tasks of its ``LaminDBFlow`` scope can find it.
    Create the scope's tasks inside ``with LaminDBFlow():`` to have it added and wired for you; used
    on its own, the operator is no parent of any task. Returns the flow run uid.

    Marked as an Airflow *setup* task by default, pairing with the *teardown*
    ``LaminDBFlowFinishOperator``; pass ``is_setup=False`` to opt out. Accepts
    every ``PythonVirtualenvOperator`` argument except ``python_callable``,
    ``op_args`` and ``op_kwargs``.
    """

    _runtime_function = "start_flow_run"

    def __init__(
        self,
        *,
        task_id: str | None = None,
        is_setup: bool = True,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        lamindb_setup: LaminDBSetup | None = None,
        lamindb_scope: str = DEFAULT_SCOPE,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            task_id=task_id or flow_task_ids(lamindb_scope)[0],
            lamindb_conn_id=lamindb_conn_id,
            lamindb_instance=lamindb_instance,
            lamindb_space=lamindb_space,
            lamindb_branch=lamindb_branch,
            lamindb_version=lamindb_version,
            lamindb_cache_dir=lamindb_cache_dir,
            lamindb_setup=lamindb_setup,
            lamindb_scope=lamindb_scope,
            **kwargs,
        )
        if is_setup:
            self.as_setup()

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        dag_id, _ = dag_and_run_id(context)
        return {
            "reference": context_flow_run_reference(context, self.lamindb_scope),
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
    """Close the LaminDB flow run with the outcome of its tasks, in a virtualenv.

    Marked as an Airflow *teardown* task by default: it runs after every other task
    (``trigger_rule=all_done_setup_success``) and is ignored when Airflow decides the
    DAG run state, so a failed step still fails the DAG run. Marks the flow run
    errored if a task failed: one of ``lamindb_members`` (all their map indexes) if given, as for
    the finish task of a ``LaminDBFlow``, else any other task of the DAG run. Pass
    ``is_teardown=False`` together with your own ``trigger_rule`` to opt out. Accepts every
    ``PythonVirtualenvOperator`` argument except ``python_callable``, ``op_args`` and
    ``op_kwargs``.

    :param lamindb_members: ids of the tasks whose failure fails the flow run, set by ``LaminDBFlow``.
        ``None`` (default) looks at every other task of the DAG run.
    """

    template_fields: Sequence[str] = (*_LaminDBFlowOperator.template_fields, "lamindb_members")
    _runtime_function = "finish_flow_run"

    def __init__(
        self,
        *,
        task_id: str | None = None,
        is_teardown: bool = True,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        lamindb_setup: LaminDBSetup | None = None,
        lamindb_scope: str = DEFAULT_SCOPE,
        lamindb_members: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        if not is_teardown:
            kwargs.setdefault("trigger_rule", TriggerRule.ALL_DONE)
        super().__init__(
            task_id=task_id or flow_task_ids(lamindb_scope)[1],
            lamindb_conn_id=lamindb_conn_id,
            lamindb_instance=lamindb_instance,
            lamindb_space=lamindb_space,
            lamindb_branch=lamindb_branch,
            lamindb_version=lamindb_version,
            lamindb_cache_dir=lamindb_cache_dir,
            lamindb_setup=lamindb_setup,
            lamindb_scope=lamindb_scope,
            **kwargs,
        )
        self.lamindb_members = None if lamindb_members is None else list(lamindb_members)
        if is_teardown:
            self.as_teardown()

    def _setup_env(self, context: Any) -> dict[str, str]:
        # closing the flow run matters more than the setup's environment: without it the run would
        # stay started, e.g. when execution_env() can't read a Variable
        try:
            return super()._setup_env(context)
        except Exception:
            self.log.warning(
                "lamindb_setup.execution_env() failed; closing the flow run without its variables",
                exc_info=True,
            )
            return {}

    def _runtime_kwargs(self, context: Any) -> dict[str, Any]:
        return {
            "reference": context_flow_run_reference(context, self.lamindb_scope),
            "success": not dag_run_failed(context, self.task_id, self.log, self.lamindb_members),
        }
