"""The task decorator behind ``@task.lamindb_venv`` and ``@task.lamindb_k8s``."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import attr

from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import task_setting
from airflow.sdk.bases.decorator import _TaskDecorator


class LaminDBTaskDecorator(_TaskDecorator):
    """Airflow's task decorator, wiring mapped LaminDB steps into the flow run like unmapped ones.

    Unmapped steps wire themselves when they are created. Mapped steps (``.expand()``,
    ``.expand_kwargs()``) aren't created until they run, so this wires them when Airflow maps them.
    ``partial()`` and ``override()`` keep this class.
    """

    def _expand(self, expand_input: Any, *, strict: bool) -> Any:
        result = super()._expand(expand_input, strict=strict)
        mapped = cast("Any", result).operator  # a PlainXComArg of the new mapped task
        if task_setting(mapped, "auto_flow") and task_setting(mapped, "track"):
            flow_kwargs = self.operator_class.lamindb_flow_kwargs(lambda name: task_setting(mapped, name))
            wire_flow_tasks(mapped, **flow_kwargs)
        return result


def lamindb_task_decorator_factory(
    python_callable: Callable[..., Any] | None,
    *,
    multiple_outputs: bool | None,
    decorated_operator_class: type,
    **kwargs: Any,
) -> Any:
    """Airflow's ``task_decorator_factory``, with ``LaminDBTaskDecorator``."""

    def decorate(function: Callable[..., Any]) -> LaminDBTaskDecorator:
        return LaminDBTaskDecorator(
            function=function,
            multiple_outputs=cast("bool", attr.NOTHING) if multiple_outputs is None else multiple_outputs,
            operator_class=decorated_operator_class,
            kwargs=kwargs,
        )

    if python_callable is not None:
        if not callable(python_callable):
            raise TypeError("No positional arguments allowed while using @task, use named arguments instead")
        return decorate(python_callable)
    return decorate
