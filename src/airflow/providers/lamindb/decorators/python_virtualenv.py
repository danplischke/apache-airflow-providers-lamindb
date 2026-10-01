"""``@task.lamindb_venv``: run the function as a LaminDB step inside a virtualenv."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from airflow.providers.standard.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator
from airflow.sdk.bases.decorator import task_decorator_factory

from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import (
    RemoteLaminDBStepMixin,
    add_lamindb_requirement,
    is_lamindb_requirement,
)

# virtualenv settings the auto-wired flow operators copy from the first step
_SHARED_VENV_ARGS = (
    "python_version",
    "system_site_packages",
    "pip_install_options",
    "index_urls",
    "index_urls_from_connection_ids",
    "venv_cache_path",
    "env_vars",
    "inherit_env",
)


class LaminDBVenvDecoratedOperator(RemoteLaminDBStepMixin, _PythonVirtualenvDecoratedOperator):  # type: ignore[misc]
    """``@task.lamindb_venv``: LaminDB step inside a virtualenv."""

    custom_operator_name = "@task.lamindb_venv"

    def __init__(self, *, lamindb_version: str | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if self.track:
            add_lamindb_requirement(self.requirements, lamindb_version)
        if self.auto_flow:
            wire_flow_tasks(
                self,
                venv=True,
                lamindb_instance=self.lamindb_instance,
                requirements=[r for r in self.requirements if is_lamindb_requirement(r)],
                **{name: getattr(self, name) for name in _SHARED_VENV_ARGS if hasattr(self, name)},
            )


def lamindb_venv_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamindb_venv``: run the function as a LaminDB step inside a virtualenv.

    Accepts every ``@task.virtualenv`` argument, plus:

    - ``lamindb_instance``: instance slug to connect to; defaults to the worker's
      instance if lamindb is set up there, else lamindb's own default.
    - ``lamindb_version``: lamindb version added to ``requirements`` unless you list
      lamindb yourself; defaults to the worker's version if installed, else latest.
    - ``auto_flow``: wire ``init >> step >> finish`` with the DAG's flow operators,
      adding virtualenv ones (same Python, index and lamindb settings) if the DAG
      has none yet. Default ``True``; mapped steps are never auto-wired.
    - ``track``: record the call in LaminDB. Default ``True``; with ``False`` the
      function runs exactly as under ``@task.virtualenv``, and lamindb is not added
      to ``requirements``.

    The worker needs no lamindb.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=LaminDBVenvDecoratedOperator,
        **kwargs,
    )
