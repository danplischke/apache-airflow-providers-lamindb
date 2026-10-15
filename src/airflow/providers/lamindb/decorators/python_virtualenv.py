"""``@task.lamindb_venv``: run the function as a LaminDB step inside a virtualenv."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.operators.flow import wire_flow_tasks
from airflow.providers.lamindb.utils.remote import (
    ExcludeTemplated,
    RemoteLaminDB,
    RemoteLaminDBStepMixin,
    check_lamindb_version,
    ensure_lamindb_requirement,
    is_template,
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
    template_fields: Sequence[str] = (
        *_PythonVirtualenvDecoratedOperator.template_fields,
        "lamindb_conn_id",
        "lamindb_instance",
        "lamindb_space",
        "lamindb_version",
        "lamindb_cache_dir",
    )

    # the lamindb arguments are repeated here so that DAG default_args reach them
    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        lamindb_space: str | None = None,
        lamindb_version: str | None = None,
        lamindb_cache_dir: str | None = None,
        lamindb_exclude_params: Sequence[str] = (),
        lamindb_exclude_templated: ExcludeTemplated = "secrets",
        **kwargs: Any,
    ) -> None:
        check_lamindb_version(lamindb_version)
        super().__init__(
            lamindb_conn_id=lamindb_conn_id,
            lamindb_instance=lamindb_instance,
            lamindb_space=lamindb_space,
            lamindb_exclude_params=lamindb_exclude_params,
            lamindb_exclude_templated=lamindb_exclude_templated,
            **kwargs,
        )
        self.lamindb_version = lamindb_version
        self.lamindb_cache_dir = lamindb_cache_dir
        if self.auto_flow:
            wire_flow_tasks(
                self,
                lamindb_conn_id=self.lamindb_conn_id,
                lamindb_instance=self.lamindb_instance,
                lamindb_space=None if is_template(self.lamindb_space) else self.lamindb_space,
                # a template may refer to the step's own XComs or map index; the flow tasks fall
                # back to the instance's version instead
                lamindb_version=None if is_template(self.lamindb_version) else self.lamindb_version,
                lamindb_cache_dir=self.lamindb_cache_dir,
                requirements=[
                    line for line in lamindb_requirement_lines(self.requirements) if not is_template(line)
                ],
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
      Templated, e.g. from an upstream task's XCom.
    - ``lamindb_instance``: instance slug to connect to; overrides the connection's. Templated.
    - ``lamindb_space``: name or uid of the space for the step run and the records the
      function saves; defaults to the flow run's space. Templated.
    - ``lamindb_version``: version of ``lamindb-core`` added to ``requirements`` unless
      you list lamindb yourself, 2.8.1 or newer; defaults to the instance's version on
      LaminHub (with a connection), else the latest. Templated, e.g. per mapped task;
      auto-wired flow tasks don't copy a templated version.
    - ``lamindb_cache_dir``: absolute path of a lamindb cache shared between tasks, set as
      ``LAMIN_CACHE_DIR``; overrides the connection's ``cache_dir`` extra. Templated.
    - ``auto_flow``: wire ``init >> step >> finish`` with the DAG's flow operators,
      adding ones with the same Python, index and LaminDB settings if the DAG has none
      yet. Default ``False``; mapped steps are never auto-wired.
    - ``track``: record the call in LaminDB. Default ``True``; with ``False`` the
      function runs exactly as under ``@task.virtualenv``, and lamindb is not added
      to ``requirements``.
    - ``connect``: with ``track=False``, still install lamindb and connect to the
      instance with the connection's credentials before the function runs, without
      recording a step or needing a flow run. Default ``False``.
    - ``lamindb_exclude_params``: names of parameters (or ``**kwargs`` keys) not to
      record; the function still gets them.
    - ``lamindb_exclude_templated``: ``"secrets"`` (default) doesn't record arguments
      whose template reads an Airflow Variable or Connection (``var``, ``conn``),
      ``"all"`` no templated argument, ``"none"`` records them all. Arguments that
      contain a secret Airflow masks in logs are never recorded.

    The worker needs no lamindb.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=LaminDBVenvDecoratedOperator,
        **kwargs,
    )
