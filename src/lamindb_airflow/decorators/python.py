"""``@task.lamindb``: run the function as a LaminDB step in the worker process."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.sdk.bases.decorator import DecoratedOperator, task_decorator_factory

from lamindb_airflow.operators.step import LaminDBStepOperator


class LaminDBDecoratedOperator(DecoratedOperator, LaminDBStepOperator):  # type: ignore[misc]
    """``@task.lamindb``: in-process LaminDB step."""

    custom_operator_name = "@task.lamindb"

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # DecoratedOperator sets op_args/op_kwargs itself, then calls the upstream
        # __init__ with kwargs_to_upstream; LaminDBStepOperator must see the same values
        # or it resets them and XComArg dependencies are lost.
        super().__init__(
            kwargs_to_upstream={
                "python_callable": python_callable,
                "op_args": op_args,
                "op_kwargs": op_kwargs,
            },
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def lamindb_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamindb``: run the function as a LaminDB step in the worker process.

    Accepts ``auto_flow`` and ``track`` like ``LaminDBStepOperator``. Needs lamindb on
    the worker; see ``@task.lamindb_venv`` otherwise.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=LaminDBDecoratedOperator,
        **kwargs,
    )
