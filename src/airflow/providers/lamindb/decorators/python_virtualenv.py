"""``@task.lamindb_venv``: run the function as a LaminDB step inside a virtualenv."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import (
    RemoteLaminDB,
    RemoteLaminDBStepMixin,
    ensure_lamindb_requirement,
    lamindb_requirement_lines,
    lamindb_virtualenv_env,
)
from airflow.providers.standard.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator
from airflow.sdk.bases.decorator import task_decorator_factory

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


class LaminDBVenvDecoratedOperator(RemoteLaminDBStepMixin, _PythonVirtualenvDecoratedOperator):
    """``@task.lamindb_venv``: LaminDB step inside a virtualenv."""

    custom_operator_name = "@task.lamindb_venv"

    # the lamindb arguments are repeated here so that DAG default_args reach them
    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_version: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(lamindb_conn_id=lamindb_conn_id, lamindb_instance=lamindb_instance, **kwargs)
        self.lamindb_version = lamindb_version
        if self.auto_flow:
            wire_flow_tasks(
                self,
                lamindb_conn_id=self.lamindb_conn_id,
                lamindb_instance=self.lamindb_instance,
                lamindb_version=self.lamindb_version,
                requirements=lamindb_requirement_lines(self.requirements),
                **{name: getattr(self, name) for name in _SHARED_VENV_ARGS if hasattr(self, name)},
            )

    @contextmanager
    def _lamindb_environment(self, remote: RemoteLaminDB) -> Iterator[None]:
        ensure_lamindb_requirement(self, remote, self.lamindb_version)
        with lamindb_virtualenv_env(self, remote):
            yield


def lamindb_venv_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamindb_venv``: run the function as a LaminDB step inside a virtualenv.

    Accepts every ``@task.virtualenv`` argument, plus:

    - ``lamindb_conn_id``: Airflow connection of type ``lamindb`` with the Lamin API key
      and the instance; default ``lamindb_default``. The virtualenv gets them instead of
      the worker's ``~/.lamin``. ``None`` uses lamindb's own configuration instead.
    - ``lamindb_instance``: instance slug to connect to; overrides the connection's.
    - ``lamindb_version``: version of ``lamindb-core`` added to ``requirements`` unless
      you list lamindb yourself; defaults to the instance's version on LaminHub (with a
      connection), else the latest.
    - ``auto_flow``: wire ``init >> step >> finish`` with the DAG's flow operators,
      adding ones with the same Python, index and LaminDB settings if the DAG has none
      yet. Default ``True``; mapped steps are never auto-wired.
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
