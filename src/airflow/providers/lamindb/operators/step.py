"""Track virtualenv and pod steps in LaminDB."""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.lamindb.execution.config import ConnectedStepConfig, TrackedStepConfig
from airflow.providers.lamindb.execution.dag_run import (
    context_flow_run_reference,
    dag_source,
    task_instance_reference,
)
from airflow.providers.lamindb.execution.environment import RemoteLaminDB
from airflow.providers.lamindb.execution.parameters import (
    ExcludeTemplated,
    check_exclude_params,
    masked_parameters,
    space_name,
    templated_parameters,
)
from airflow.providers.lamindb.execution.setup import LaminDBSetup, resolve_setup, setup_bootstrap_source
from airflow.providers.lamindb.execution.source import build_remote_step_source
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.operators.flow import FLOW_INSTANCE_XCOM_KEY, LaminDBFlowInitOperator


def _operator_default(operator_class: type, name: str) -> Any:
    """The default of an ``__init__`` argument of ``operator_class`` or its bases, else ``None``."""
    for cls in operator_class.__mro__:
        init = vars(cls).get("__init__")
        parameter = inspect.signature(init).parameters.get(name) if init is not None else None
        if parameter is not None and parameter.default is not inspect.Parameter.empty:
            return parameter.default
    return None


def task_setting(task: Any, name: str) -> Any:
    """An argument of a task as it will run.

    Mapped tasks get theirs when Airflow unmaps them: from ``partial``, else from the ``default_args``
    of their task group (which include its parent groups') over the DAG's, as Airflow merges them,
    else the operator's default.
    """
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    if not isinstance(task, MappedOperator):
        return getattr(task, name, None)
    if name in task.partial_kwargs:
        return task.partial_kwargs[name]
    dag = task.get_dag()
    default_args = dict(dag.default_args or {}) if dag else {}
    if task.task_group is not None:
        default_args.update(task.task_group.default_args or {})
    if name in default_args:
        return default_args[name]
    return _operator_default(task.operator_class, name)


def check_same_setup(step: Any, *flow_tasks: Any) -> None:
    """Fail if a step's ``lamindb_setup`` differs from its flow tasks', which would then skip it.

    A step without one, or with the no-op ``LaminDBSetup()``, needs nothing from its flow tasks.
    """
    setup = resolve_setup(task_setting(step, "lamindb_setup"))
    if setup is None or type(setup) is LaminDBSetup:
        return
    for flow_task in flow_tasks:
        if resolve_setup(task_setting(flow_task, "lamindb_setup")) != setup:
            raise AirflowException(
                f"LaminDB step {step.task_id!r} and flow task {flow_task.task_id!r} have different "
                "lamindb_setup. Set it for the whole DAG, through default_args or the [lamindb] "
                "setup_class option."
            )


def check_flow_instance(step: Any, context: Any, instance: str | None, scope: str) -> None:
    """Fail early if the step would connect to another instance than the flow run of its scope."""
    dag = step.get_dag()
    inits = [
        t.task_id
        for t in (dag.tasks if dag else [])
        if isinstance(t, LaminDBFlowInitOperator) and t.lamindb_scope == scope
    ]
    if instance is None or not inits:
        return
    flow_instance = context["ti"].xcom_pull(task_ids=inits[0], key=FLOW_INSTANCE_XCOM_KEY)
    if flow_instance and flow_instance != instance:
        raise AirflowException(
            f"Task {step.task_id!r} connects to LaminDB instance {instance!r}, but its flow run is in "
            f"{flow_instance!r}. All LaminDB tasks of a LaminDBFlow must use the same instance."
        )


def reject_auto_flow(kwargs: dict[str, Any]) -> None:
    """Fail for the removed ``auto_flow`` argument, which used to add flow tasks to a DAG implicitly.

    Also when it comes through ``default_args``, which Airflow hands to ``__init__`` merged from the
    DAG, its task groups and the task: Airflow would drop it there silently, and the task would be
    standalone without a word. ``lamindb_scope`` there would make standalone tasks scoped ones.
    """
    default_args = kwargs.get("default_args") or {}
    if "lamindb_scope" in default_args:
        raise TypeError(
            "lamindb_scope can't be set through default_args: it would make every LaminDB task a "
            "member of a flow run. Create the tasks inside `with LaminDBFlow():` instead."
        )
    if "auto_flow" in kwargs or "auto_flow" in default_args:
        raise TypeError(
            "auto_flow was removed: tracked LaminDB tasks no longer need a flow run. Drop the argument "
            "to track the task standalone, or create the tasks inside `with LaminDBFlow():` "
            "(from airflow.providers.lamindb import LaminDBFlow) to group them under a flow run."
        )


class RemoteLaminDBStepMixin:
    """Track a virtualenv/pod step, standalone or in a flow run, by rewriting the shipped source.

    :param lamindb_conn_id: Airflow connection of type ``lamindb`` with the Lamin API key and the
        instance; the step fails if it does not exist. ``None`` uses lamindb's own configuration in
        the virtualenv or pod instead.
    :param lamindb_instance: instance slug (``owner/name``) to connect to remotely; overrides the
        connection's instance.
    :param lamindb_branch: name or uid of the branch the step run, and records the function saves,
        are on; defaults to the flow run's branch. The DAG file's transform stays on the branch
        lamindb finds it on. With ``connect=True``, the branch records the function saves are on.
    :param lamindb_space: name or uid of the space the step run, and records the function saves,
        are created in; defaults to the flow run's space. With ``connect=True``, the space records
        the function saves are created in.
    :param lamindb_scope: set by ``LaminDBFlow`` when it wires the step, not to be passed: ``None``
        for a standalone step, ``""`` for the DAG's default scope, else the scope id. A scoped step is
        a child of its scope's flow run and fails if that does not exist.
    :param track: record the call in LaminDB. Pass ``False`` to ship the function
        unchanged, as the plain virtualenv/pod operator would.
    :param connect: with ``track=False``, still connect to the instance before the function
        runs, without recording anything: virtualenvs get lamindb and the connection's
        credentials as for tracked steps, pods connect with their own credentials.
    :param lamindb_setup: a [`LaminDBSetup`][airflow.providers.lamindb.execution.setup.LaminDBSetup]
        that prepares the virtualenv or pod; give all LaminDB tasks of a DAG the same one.
    :param lamindb_exclude_params: names of parameters (or ``**kwargs`` keys) whose arguments are
        not recorded. The function still gets them.
    :param lamindb_exclude_templated: which templated arguments are not recorded: ``"secrets"``
        (default) those whose template reads an Airflow Variable or Connection (``var``, ``conn``),
        ``"all"`` every templated argument, ``"none"`` none. Arguments that contain a secret Airflow
        masks in logs are never recorded.
    """

    _lamindb_remote: dict[str, Any] | None = None
    _lamindb_runtime_function = "run_step"
    _lamindb_templated_params: frozenset[str] = frozenset()
    # only virtualenv steps can point lamindb at a shared cache
    lamindb_cache_dir: str | None = None

    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_branch: str | None = None,
        lamindb_scope: str | None = None,
        track: bool = True,
        connect: bool = False,
        lamindb_setup: LaminDBSetup | None = None,
        lamindb_exclude_params: Sequence[str] = (),
        lamindb_exclude_templated: ExcludeTemplated = "secrets",
        **kwargs: Any,
    ) -> None:
        reject_auto_flow(kwargs)
        super().__init__(**kwargs)
        self.lamindb_conn_id = lamindb_conn_id
        self.lamindb_instance = lamindb_instance
        self.lamindb_space = lamindb_space
        self.lamindb_branch = lamindb_branch
        self.track = track
        self.connect = connect
        self.lamindb_setup = lamindb_setup
        if track or connect:
            resolve_setup(lamindb_setup)  # a broken [lamindb] setup_class fails when the DAG is parsed
        self.lamindb_scope = lamindb_scope
        if lamindb_exclude_templated not in ("secrets", "all", "none"):
            raise ValueError(
                "lamindb_exclude_templated must be 'secrets', 'all' or 'none', "
                f"not {lamindb_exclude_templated!r}"
            )
        if isinstance(lamindb_exclude_params, str):
            lamindb_exclude_params = [lamindb_exclude_params]
        self.lamindb_exclude_params = tuple(lamindb_exclude_params)
        self.lamindb_exclude_templated = lamindb_exclude_templated
        if track:
            check_exclude_params(self.python_callable, self.lamindb_exclude_params)  # type: ignore[attr-defined]

    def _do_render_template_fields(
        self, parent: Any, template_fields: Iterable[str], context: Any, jinja_env: Any, seen_oids: set[int]
    ) -> None:
        # called for the task itself both when it's mapped and when it isn't; before rendering, the
        # arguments still hold their templates
        if parent is self and self.track:
            self._lamindb_templated_params = frozenset(
                templated_parameters(self, jinja_env, self.lamindb_exclude_templated, seen_oids)
            )
        super()._do_render_template_fields(  # type: ignore[misc]
            parent, template_fields, context, jinja_env, seen_oids
        )

    def _lamindb_exclude_params(self) -> list[str]:
        masked = masked_parameters(
            self.python_callable,  # type: ignore[attr-defined]
            self.op_args,  # type: ignore[attr-defined]
            self.op_kwargs,  # type: ignore[attr-defined]
        )
        return sorted({*self.lamindb_exclude_params, *self._lamindb_templated_params, *masked})

    def _lamindb_space(self) -> str | None:
        return space_name(self.lamindb_space)

    @contextmanager
    def _lamindb_environment(self, remote: RemoteLaminDB, context: Any) -> Iterator[None]:
        """Prepare the remote environment; pods bring lamindb, credentials and environment in their spec."""
        yield

    def execute(self, context: Any) -> Any:
        if not self.track and not self.connect:
            return super().execute(context)  # type: ignore[misc]
        remote = RemoteLaminDB.resolve(self.lamindb_conn_id, self.lamindb_instance, self.lamindb_cache_dir)
        if self.track:
            scope = self.lamindb_scope
            if scope is not None:
                check_flow_instance(self, context, remote.instance, scope)
            self._lamindb_runtime_function = "run_step"
            self._lamindb_remote = TrackedStepConfig(
                flow_run_reference=context_flow_run_reference(context, scope) if scope is not None else None,
                source_code=dag_source(context["dag"]),
                fileloc=context["dag"].fileloc,
                step_reference=task_instance_reference(context),
                instance=remote.instance,
                exclude_params=self._lamindb_exclude_params(),
                space=self._lamindb_space(),
                branch=space_name(self.lamindb_branch),
            ).model_dump()
        else:
            # not recorded, so it may connect to any instance
            self._lamindb_runtime_function = "run_connected"
            self._lamindb_remote = ConnectedStepConfig(
                instance=remote.instance,
                space=self._lamindb_space(),
                branch=space_name(self.lamindb_branch),
            ).model_dump()
        try:
            with self._lamindb_environment(remote, context):
                return super().execute(context)  # type: ignore[misc]
        finally:
            self._lamindb_remote = None

    def get_python_source(self) -> str:
        user_source = super().get_python_source()  # type: ignore[misc]
        if self._lamindb_remote is None:
            return user_source
        return build_remote_step_source(
            user_source=user_source,
            callable_name=self.python_callable.__name__,  # type: ignore[attr-defined]
            config=self._lamindb_remote,
            runtime_function=self._lamindb_runtime_function,
            bootstrap=setup_bootstrap_source(self.lamindb_setup),
        )
