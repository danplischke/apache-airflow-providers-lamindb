"""``LaminDBFlow``: an optional flow run around some of a DAG's LaminDB tasks.

A tracked LaminDB task is standalone unless it is created inside ``with LaminDBFlow():``. On exit the
block adds the flow init and finish tasks of its flow run and gives the block's tracked tasks the
scope they run in. Parse-time only: no lamindb is imported, and the scope reaches the workers as the
plain ``lamindb_scope`` argument of the tasks, which Airflow serializes with them.
"""

from __future__ import annotations

import warnings
from collections.abc import Collection
from contextvars import ContextVar, Token
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.lamindb.exceptions import LaminDBFlowWarning
from airflow.providers.lamindb.execution.dag_run import DEFAULT_SCOPE, flow_task_ids, validate_scope_id
from airflow.providers.lamindb.operators.flow import (
    _CREATED_BY_LAMINDB_FLOW,
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)
from airflow.providers.lamindb.operators.step import RemoteLaminDBStepMixin, check_same_setup, task_setting
from airflow.sdk.definitions._internal.contextmanager import DagContext

__all__ = ["LaminDBFlow"]

_active: ContextVar[LaminDBFlow | None] = ContextVar("lamindb_flow_scope", default=None)

# set by the scope itself
_RESERVED = ("dag", "task_id", "task_group", "lamindb_scope", "lamindb_members")


def _is_step(task: Any) -> bool:
    """Whether ``task`` is a LaminDB step (mapped or not) that records itself, so a scope parents it."""
    operator_class = getattr(task, "operator_class", type(task))  # mapped tasks hold their class
    if not (isinstance(operator_class, type) and issubclass(operator_class, RemoteLaminDBStepMixin)):
        return False
    return bool(task_setting(task, "track"))


class LaminDBFlow:
    """Group the tracked LaminDB tasks created in the block under one flow run per DAG run.

    ::

        with DAG("pipeline"):
            standalone = extract()
            with LaminDBFlow(lamindb_version="2.11"):
                curate(extract())

    Every task invocation created in the block is a member: normal and mapped ones, plain Airflow
    tasks and ``track=False`` tasks too, also inside task groups. A task created outside the block
    and only wired inside it is not. On exit the block adds a ``LaminDBFlowInitOperator`` upstream of
    the members that have no upstream member and a ``LaminDBFlowFinishOperator`` downstream of those
    that have no downstream member, paired as Airflow's setup and teardown. Tracked members are
    children of the flow run; the finish task marks it errored when a member failed, any map index
    included.

    A DAG can have one default scope. More scopes need a stable ``scope_id``, which names their
    tasks and flow run. Scopes can't be nested. If the block raises, nothing is wired.

    :param scope_id: letters, digits, ``_`` and ``-``; ``None`` for the DAG's default scope.
    :param flow_operator_kwargs: arguments of both flow operators: ``lamindb_conn_id``,
        ``lamindb_instance``, ``lamindb_setup``, ``lamindb_version``, ``requirements`` and any
        ``PythonVirtualenvOperator`` argument. Nothing is borrowed from the tasks in the block.
    """

    def __init__(self, scope_id: str | None = None, **flow_operator_kwargs: Any) -> None:
        if scope_id is not None:
            validate_scope_id(scope_id)
        if reserved := sorted(set(_RESERVED) & flow_operator_kwargs.keys()):
            raise TypeError(f"LaminDBFlow sets {', '.join(reserved)} itself; don't pass them")
        self.scope_id = scope_id
        self.flow_operator_kwargs = flow_operator_kwargs
        self._dag: Any = None
        self._before: Collection[str] = ()
        self._token: Token[LaminDBFlow | None] | None = None

    @property
    def scope(self) -> str:
        """The ``lamindb_scope`` of its tasks: ``""`` for the default scope, else the id."""
        return self.scope_id or DEFAULT_SCOPE

    def __enter__(self) -> LaminDBFlow:
        active = _active.get()
        if active is not None:
            raise AirflowException(
                "LaminDBFlow scopes can't be nested: "
                f"{self._describe()} was entered inside {active._describe()}. Close the first "
                "scope, or give the tasks to one of them."
            )
        dag = DagContext.get_current()
        if dag is None:
            raise AirflowException("LaminDBFlow must be used inside a DAG, as `with LaminDBFlow():`")
        if self.scope_id is not None:
            validate_scope_id(self.scope_id, dag.dag_id)
        self._check_unused(dag)
        self._dag = dag
        self._before = frozenset(dag.task_dict)
        self._token = _active.set(self)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if exc_type is None:
                self._wire()
        finally:
            if self._token is not None:
                _active.reset(self._token)
            self._token = self._dag = None
            self._before = ()

    def _describe(self) -> str:
        return f"LaminDBFlow({self.scope_id!r})" if self.scope_id else "the default LaminDBFlow"

    def _check_unused(self, dag: Any) -> None:
        if any(
            isinstance(task, LaminDBFlowInitOperator) and task.lamindb_scope == self.scope
            for task in dag.task_dict.values()
        ):
            what = (
                f"scope_id {self.scope_id!r} is already used in DAG {dag.dag_id!r}"
                if self.scope_id
                else f"DAG {dag.dag_id!r} already has a default LaminDBFlow; give each further scope "
                'a stable scope_id, as `LaminDBFlow("enrich")`'
            )
            raise AirflowException(f"{self._describe()}: {what}")

    def _wire(self) -> None:
        dag = self._dag
        members = {
            task_id: dag.task_dict[task_id] for task_id in dag.task_dict if task_id not in self._before
        }
        if not members:
            warnings.warn(
                f"{self._describe()} in DAG {dag.dag_id!r} has no tasks; it adds no flow run.",
                LaminDBFlowWarning,
                stacklevel=3,
            )
            return
        init_id, finish_id = flow_task_ids(self.scope)
        created_by_flow = _CREATED_BY_LAMINDB_FLOW.set(True)
        try:
            init = LaminDBFlowInitOperator(
                dag=dag, task_id=init_id, lamindb_scope=self.scope, **self.flow_operator_kwargs
            )
            finish = LaminDBFlowFinishOperator(
                dag=dag,
                task_id=finish_id,
                lamindb_scope=self.scope,
                lamindb_members=list(members),
                **self.flow_operator_kwargs,
            )
        finally:
            _CREATED_BY_LAMINDB_FLOW.reset(created_by_flow)
        steps = [task for task in members.values() if _is_step(task)]
        try:
            for step in steps:  # validate everything before changing any task
                check_same_setup(step, init, finish)
        except Exception:
            for flow_task in (init, finish):  # leave the DAG as it was before the block's wiring
                dag._remove_task(flow_task.task_id)
            raise
        for step in steps:
            self._scope(step)
        roots = [t for t in members.values() if not t.upstream_task_ids & members.keys()]
        leaves = [t for t in members.values() if not t.downstream_task_ids & members.keys()]
        for root in roots:
            init >> root
        for leaf in leaves:
            leaf >> finish
        init >> finish

    def _scope(self, task: Any) -> None:
        if hasattr(task, "partial_kwargs"):  # mapped: created when it runs, from its partial arguments
            task.partial_kwargs["lamindb_scope"] = self.scope
        else:
            task.lamindb_scope = self.scope
