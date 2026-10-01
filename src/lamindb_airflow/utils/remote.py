"""Run LaminDB bookkeeping in a virtualenv or pod, so the worker needs no lamindb.

Airflow's virtualenv and Kubernetes operators ship the callable's *source text* to
the remote interpreter and call it by name. We keep that mechanism: the shipped
script embeds :mod:`lamindb_airflow.utils.runtime` as source and calls into it.
Only ``lamindb`` has to be installed remotely; nothing is pickled by reference.
"""

from __future__ import annotations

import importlib.util
import inspect
import re
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from lamindb_airflow.utils import runtime
from lamindb_airflow.utils.dag_run import context_flow_run_reference, dag_source, task_instance_url

_LAMINDB_REQUIREMENT = re.compile(r"^\s*lamindb\s*($|[\[<>=!~;@\s])", re.IGNORECASE)


def _runtime_loader_source() -> str:
    return f"""

def _lamindb_airflow_runtime():
    import types

    module = types.ModuleType("lamindb_airflow_runtime")
    exec({inspect.getsource(runtime)!r}, module.__dict__)
    return module
"""


def build_remote_step_source(
    *, user_source: str, callable_name: str, config: dict[str, str | None], runtime_function: str = "run_step"
) -> str:
    """Append a wrapper to ``user_source`` and rebind ``callable_name`` to it.

    The operator's script template then calls the wrapper, which runs the user
    function via ``runtime.<runtime_function>`` (:func:`runtime.run_step` or
    :func:`runtime.run_untracked`) with ``config`` as keyword arguments.
    """
    return (
        user_source.rstrip()
        + "\n"
        + _runtime_loader_source()
        + f"""

def _lamindb_airflow_step(*args, **kwargs):
    return _lamindb_airflow_runtime().{runtime_function}(_lamindb_airflow_user_fn, args, kwargs, **{config!r})


_lamindb_airflow_user_fn = {callable_name}
{callable_name} = _lamindb_airflow_step
"""
    )


def build_remote_flow_source(*, function_name: str, runtime_function: str) -> str:
    """Define ``function_name(lamindb_airflow_config)`` calling ``runtime.<runtime_function>``; returns the run uid."""
    return (
        _runtime_loader_source()
        + f"""

def {function_name}(lamindb_airflow_config):
    return _lamindb_airflow_runtime().{runtime_function}(**lamindb_airflow_config).uid
"""
    )


def worker_lamindb_version() -> str | None:
    try:
        return version("lamindb")
    except PackageNotFoundError:
        return None


def is_lamindb_requirement(requirement: Any) -> bool:
    return bool(_LAMINDB_REQUIREMENT.match(str(requirement)))


def add_lamindb_requirement(requirements: list[str], lamindb_version: str | None = None) -> None:
    """Append lamindb to ``requirements`` unless it is listed already.

    Pinned to ``lamindb_version``, else to the worker's lamindb version if installed
    (the remote side must speak the instance's schema), else unpinned.
    """
    if any(is_lamindb_requirement(r) for r in requirements):
        return
    pin = lamindb_version or worker_lamindb_version()
    requirements.append(f"lamindb=={pin}" if pin else "lamindb")


def worker_instance_slug() -> str | None:
    """Slug of the worker's lamindb instance, or None if lamindb is not set up on the worker."""
    if importlib.util.find_spec("lamindb_setup") is None:
        return None
    import lamindb_setup

    slug = lamindb_setup.settings.instance.slug
    return None if slug == "none/none" else slug


class RemoteLaminDBStepMixin:
    """Bind a virtualenv/pod step to the flow run (untracked: to the instance) by rewriting the shipped source.

    :param lamindb_instance: instance slug (``owner/name``) to connect to remotely.
        Defaults to the worker's instance if lamindb is set up there, else to
        lamindb's own default in the remote environment (``LAMIN_CURRENT_INSTANCE``).
    :param auto_flow: wire ``init >> step >> finish`` with the DAG's flow operators,
        adding virtualenv ones if the DAG has none yet. The concrete operator does
        the wiring once fully initialised, unless ``track`` is ``False``.
    :param track: record the function as a step run. ``False`` only connects to the
        instance before calling it: no flow run needed, nothing recorded.
    """

    _lamindb_remote: dict[str, str | None] | None = None

    def __init__(
        self, *, lamindb_instance: str | None = None, auto_flow: bool = True, track: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.lamindb_instance = lamindb_instance
        self.auto_flow = auto_flow
        self.track = track

    def execute(self, context: Any) -> Any:
        instance = self.lamindb_instance or worker_instance_slug()
        if self.track:
            self._lamindb_remote = {
                "flow_run_reference": context_flow_run_reference(context),
                "source_code": dag_source(context["dag"]),
                "step_reference": task_instance_url(context),
                "instance": instance,
            }
        else:
            self._lamindb_remote = {"instance": instance}
        try:
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
            runtime_function="run_step" if self.track else "run_untracked",
        )
