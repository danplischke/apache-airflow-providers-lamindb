"""Run LaminDB bookkeeping in a virtualenv or pod, so the worker needs no lamindb.

Airflow's virtualenv and Kubernetes operators ship the callable's *source text* to
the remote interpreter and call it by name. We keep that mechanism: the shipped
script embeds :mod:`airflow.providers.lamindb.utils.runtime` as source and calls into it.
Only lamindb has to be installed remotely; nothing is pickled by reference.
"""

from __future__ import annotations

import inspect
import logging
import re
import tempfile
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from packaging.version import InvalidVersion, Version

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook, parse_instance_slug
from airflow.providers.lamindb.utils import runtime
from airflow.providers.lamindb.utils.dag_run import context_flow_run_reference, dag_source, task_instance_url

log = logging.getLogger(__name__)

_LAMINDB_REQUIREMENT = re.compile(r"^\s*lamindb(?P<core>[-_.]core)?\s*($|[\[<>=!~;@\s])", re.IGNORECASE)
_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")

LAMINDB_CORE_MIN_VERSION = Version("2.6.1")
"""First lamindb release that is also published as ``lamindb-core``."""
LAMINDB_CORE_COMPANIONS = ("numpy", "pandas>=2.0.0", "pandera>=0.24.0")
"""Packages ``lamindb-core`` needs for querying runs but does not declare (they come with ``lamindb``)."""


def _runtime_loader_source() -> str:
    return f"""

def _lamindb_airflow_runtime():
    import types

    module = types.ModuleType("lamindb_airflow_runtime")
    exec({inspect.getsource(runtime)!r}, module.__dict__)
    return module
"""


def build_remote_step_source(*, user_source: str, callable_name: str, config: dict[str, str | None]) -> str:
    """Append a wrapper to ``user_source`` and rebind ``callable_name`` to it.

    The operator's script template then calls the wrapper, which runs the user
    function via :func:`runtime.run_step` with ``config`` as keyword arguments.
    """
    return (
        user_source.rstrip()
        + "\n"
        + _runtime_loader_source()
        + f"""

def _lamindb_airflow_step(*args, **kwargs):
    return _lamindb_airflow_runtime().run_step(_lamindb_airflow_user_fn, args, kwargs, **{config!r})


_lamindb_airflow_user_fn = {callable_name}
{callable_name} = _lamindb_airflow_step
"""
    )


def build_remote_flow_source(*, function_name: str, runtime_function: str) -> str:
    """Define ``function_name(lamindb_airflow_config)`` calling ``runtime.<runtime_function>``.

    The generated function returns the run uid.
    """
    return (
        _runtime_loader_source()
        + f"""

def {function_name}(lamindb_airflow_config):
    return _lamindb_airflow_runtime().{runtime_function}(**lamindb_airflow_config).uid
"""
    )


def _requirement_lines(requirements: Iterable[Any]) -> Iterator[str]:
    """Yield each requirement, also from multi-line elements such as a rendered requirements file.

    Skips blank lines, comments and pip options such as ``-r other.txt``, whose contents can't be seen.
    """
    for requirement in requirements:
        for line in str(requirement).splitlines():
            line = _REQUIREMENT_COMMENT.sub("", line).strip()
            if line and not line.startswith("-"):
                yield line


def _requirement_name(requirement: str) -> str | None:
    match = _REQUIREMENT_NAME.match(requirement)
    return re.sub(r"[-_.]+", "-", match.group(1)).lower() if match else None


def lamindb_requirement_lines(requirements: Iterable[Any]) -> list[str]:
    """The ``lamindb`` and ``lamindb-core`` requirements among ``requirements``."""
    return [line for line in _requirement_lines(requirements) if _LAMINDB_REQUIREMENT.match(line)]


def lamindb_requirements(lamindb_version: str | None = None) -> list[str]:
    """Requirements that install lamindb in a virtualenv, pinned to ``lamindb_version`` if given.

    ``lamindb-core`` plus the packages it needs for this provider: about half the size of the full
    ``lamindb`` distribution. Versions before ``lamindb-core`` existed install ``lamindb``.
    """
    if lamindb_version is not None:
        try:
            if Version(lamindb_version) < LAMINDB_CORE_MIN_VERSION:
                return [f"lamindb=={lamindb_version}"]
        except InvalidVersion:
            pass
    core = f"lamindb-core=={lamindb_version}" if lamindb_version else "lamindb-core"
    return [core, *LAMINDB_CORE_COMPANIONS]


def add_lamindb_requirement(requirements: list[str], lamindb_version: str | None = None) -> None:
    """Append lamindb to ``requirements`` unless the full ``lamindb`` is listed already.

    A listed ``lamindb-core`` is kept and only completed with the packages it does not declare.
    Multi-line elements (a rendered requirements file) are searched line by line; a nested
    ``-r other.txt`` is not, so list lamindb directly or pass ``lamindb_version`` then.
    """
    lines = list(_requirement_lines(requirements))
    lamindb = [m for line in lines if (m := _LAMINDB_REQUIREMENT.match(line))]
    if any(not m.group("core") for m in lamindb):
        return
    listed = {_requirement_name(line) for line in lines}
    to_add = list(LAMINDB_CORE_COMPANIONS) if lamindb else lamindb_requirements(lamindb_version)
    requirements.extend(r for r in to_add if _requirement_name(r) not in listed)


@dataclass(frozen=True)
class RemoteLaminDB:
    """How a virtualenv or pod reaches LaminDB, resolved on the worker at execute time.

    With a connection, the instance and API key come from the Airflow ``lamindb`` connection;
    ``instance`` overrides the connection's instance. With ``conn_id=None``, the remote lamindb uses
    its own configuration (``LAMIN_API_KEY``, ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``) and connects to
    ``instance`` if given.
    """

    conn_id: str | None
    instance: str | None
    api_key: str | None = field(default=None, repr=False)
    _hook: LaminDBHook | None = field(default=None, repr=False, compare=False)

    @classmethod
    def resolve(cls, conn_id: str | None, instance: str | None) -> RemoteLaminDB:
        if conn_id is None:
            return cls(conn_id=None, instance="/".join(parse_instance_slug(instance)) if instance else None)
        hook = LaminDBHook(lamindb_conn_id=conn_id, instance=instance)
        try:
            return cls(
                conn_id=conn_id, instance=hook.get_instance_slug(), api_key=hook.get_api_key(), _hook=hook
            )
        except AirflowNotFoundException as e:
            raise AirflowNotFoundException(
                f"{e}. Create the {conn_id!r} connection of type 'lamindb' with the Lamin API key and the "
                "instance, or pass lamindb_conn_id=None if the virtualenv or pod brings its own lamindb "
                "credentials (LAMIN_API_KEY, LAMIN_CURRENT_INSTANCE, ~/.lamin)."
            ) from e

    def instance_lamindb_version(self) -> str | None:
        """The instance's lamindb version according to LaminHub, or None if unknown."""
        if self._hook is None:
            return None
        try:
            return self._hook.get_instance().lamindb_version
        except Exception as e:
            log.warning(
                "Could not look up the lamindb version of %s, installing the latest: %s", self.instance, e
            )
            return None
        finally:
            self._hook.close()


def ensure_lamindb_requirement(operator: Any, remote: RemoteLaminDB, lamindb_version: str | None) -> None:
    """Add lamindb to a virtualenv operator's ``requirements`` unless listed.

    Pinned to ``lamindb_version``, else to the instance's version on LaminHub (with a connection),
    else unpinned. The remote side must speak the instance's schema version.
    """
    requirements = list(operator.requirements)
    if not lamindb_requirement_lines(requirements):
        lamindb_version = lamindb_version or remote.instance_lamindb_version()
    add_lamindb_requirement(requirements, lamindb_version)
    operator.requirements = requirements


@contextmanager
def lamindb_virtualenv_env(operator: Any, remote: RemoteLaminDB) -> Iterator[None]:
    """Point a virtualenv operator's lamindb at the connection instead of the worker's ``~/.lamin``.

    The virtualenv gets a temporary ``LAMIN_SETTINGS_DIR`` and the connection's ``LAMIN_API_KEY``.
    Both are set while executing only (``env_vars`` is not templated, so the key is never rendered),
    and explicit ``env_vars`` of the operator take precedence.
    """
    if remote.conn_id is None:
        yield
        return
    original = operator.env_vars
    with tempfile.TemporaryDirectory(prefix="lamindb-airflow-") as settings_dir:
        env = {"LAMIN_SETTINGS_DIR": settings_dir}
        if remote.api_key:
            env["LAMIN_API_KEY"] = remote.api_key
        operator.env_vars = {**env, **(original or {})}
        try:
            yield
        finally:
            operator.env_vars = original


def check_flow_instance(step: Any, context: Any, instance: str | None) -> None:
    """Fail early if the step would connect to another instance than its DAG run's flow run."""
    from airflow.providers.lamindb.operators.flow import FLOW_INSTANCE_XCOM_KEY, LaminDBFlowInitOperator

    dag = step.get_dag()
    inits = [t.task_id for t in (dag.tasks if dag else []) if isinstance(t, LaminDBFlowInitOperator)]
    if instance is None or not inits:
        return
    flow_instance = context["ti"].xcom_pull(task_ids=inits[0], key=FLOW_INSTANCE_XCOM_KEY)
    if flow_instance and flow_instance != instance:
        raise AirflowException(
            f"Task {step.task_id!r} connects to LaminDB instance {instance!r}, but the flow run of this "
            f"DAG run is in {flow_instance!r}. All LaminDB tasks of a DAG must use the same instance."
        )


class RemoteLaminDBStepMixin:
    """Bind a virtualenv/pod step to the flow run by rewriting the shipped source.

    :param lamindb_conn_id: Airflow connection of type ``lamindb`` with the Lamin API key and the
        instance; the step fails if it does not exist. ``None`` uses lamindb's own configuration in
        the virtualenv or pod instead.
    :param lamindb_instance: instance slug (``owner/name``) to connect to remotely; overrides the
        connection's instance.
    :param auto_flow: wire ``init >> step >> finish`` with the DAG's flow operators,
        adding virtualenv ones if the DAG has none yet. The concrete operator does
        the wiring once fully initialised. Ignored when ``track`` is ``False``.
    :param track: record the call in LaminDB. Pass ``False`` to ship the function
        unchanged, as the plain virtualenv/pod operator would.
    """

    _lamindb_remote: dict[str, str | None] | None = None

    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        lamindb_instance: str | None = None,
        auto_flow: bool = True,
        track: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.lamindb_conn_id = lamindb_conn_id
        self.lamindb_instance = lamindb_instance
        self.track = track
        self.auto_flow = auto_flow and track

    @contextmanager
    def _lamindb_environment(self, remote: RemoteLaminDB) -> Iterator[None]:
        """Prepare the remote environment; pods bring lamindb and credentials in their image."""
        yield

    def execute(self, context: Any) -> Any:
        if not self.track:
            return super().execute(context)  # type: ignore[misc]
        remote = RemoteLaminDB.resolve(self.lamindb_conn_id, self.lamindb_instance)
        check_flow_instance(self, context, remote.instance)
        self._lamindb_remote = {
            "flow_run_reference": context_flow_run_reference(context),
            "source_code": dag_source(context["dag"]),
            "step_reference": task_instance_url(context),
            "instance": remote.instance,
        }
        try:
            with self._lamindb_environment(remote):
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
        )
