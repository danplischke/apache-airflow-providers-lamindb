"""Run LaminDB bookkeeping in a virtualenv or pod, so the worker needs no lamindb.

Airflow's virtualenv and Kubernetes operators ship the callable's *source text* to
the remote interpreter and call it by name. We keep that mechanism: the shipped
script embeds ``airflow.providers.lamindb.utils.runtime`` as source and calls into it.
Only lamindb has to be installed remotely; nothing is pickled by reference.
"""

from __future__ import annotations

import inspect
import logging
import re
import tempfile
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, Literal

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook, parse_instance_slug
from airflow.providers.lamindb.utils import runtime
from airflow.providers.lamindb.utils.dag_run import context_flow_run_reference, dag_source, task_instance_url

log = logging.getLogger(__name__)

_LAMINDB_REQUIREMENT = re.compile(r"^\s*lamindb(?P<core>[-_.]core)?\s*($|[\[<>=!~;@\s])", re.IGNORECASE)
_REQUIREMENT_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")
_EXACT_PIN = re.compile(r"==\s*(\d[^,;\s]*)")

LAMINDB_CORE_COMPANIONS = ("numpy", "pandas>=2.0.0", "pandera>=0.24.0")
"""Packages ``lamindb-core`` needs for querying runs but does not declare (they come with ``lamindb``)."""
LAMINDB_CORE_COMPANIONS_PANDAS_2 = ("numpy", "pandas>=2.0.0,<3", "pandera>=0.24.0")
"""The same for lamindb before 2.10, which requires pandas below 3."""


def _runtime_loader_source() -> str:
    return f"""

def _lamindb_airflow_runtime():
    import types

    module = types.ModuleType("lamindb_airflow_runtime")
    exec({inspect.getsource(runtime)!r}, module.__dict__)
    return module
"""


def build_remote_step_source(
    *, user_source: str, callable_name: str, config: dict[str, Any], runtime_function: str = "run_step"
) -> str:
    """Append a wrapper to ``user_source`` and rebind ``callable_name`` to it.

    The operator's script template then calls the wrapper, which runs the user
    function via ``runtime.<runtime_function>`` (``run_step`` records it, ``run_connected``
    only connects) with ``config`` as keyword arguments.
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


def space_name(space: str | None) -> str | None:
    """The rendered ``lamindb_space``; empty means lamindb's default space."""
    return space.strip() or None if space else None


def is_template(value: Any) -> bool:
    """Whether ``value`` is a string with Jinja markup, known only once Airflow renders it."""
    return isinstance(value, str) and ("{{" in value or "{%" in value)


def check_lamindb_version(lamindb_version: str | None) -> None:
    """Fail for a lamindb version older than ``runtime.MIN_LAMINDB_VERSION``, the oldest one supported.

    Templates pass; they are checked once rendered, before anything is installed.
    """
    if not lamindb_version or is_template(lamindb_version):
        return
    if not re.fullmatch(r"\s*\d+(\.\d+)+\S*\s*", lamindb_version):
        raise LaminDBVersionError(
            f"lamindb_version {lamindb_version!r} is not a lamindb version, such as '2.11.0'"
        )
    if not runtime.lamindb_version_supported(lamindb_version):
        raise LaminDBVersionError(
            f"lamindb {lamindb_version} is not supported: apache-airflow-providers-lamindb needs lamindb "
            f"{runtime.MIN_LAMINDB_VERSION} or newer."
        )


def lamindb_core_companions(lamindb_version: str | None = None) -> tuple[str, ...]:
    """The packages ``lamindb-core`` needs here for ``lamindb_version``; ``None`` for the latest."""
    if lamindb_version and not runtime.lamindb_version_at_least(lamindb_version, "2.10"):
        return LAMINDB_CORE_COMPANIONS_PANDAS_2
    return LAMINDB_CORE_COMPANIONS


def lamindb_requirements(lamindb_version: str | None = None) -> list[str]:
    """Requirements that install lamindb in a virtualenv, pinned to ``lamindb_version`` if given.

    ``lamindb-core`` plus the packages it needs for this provider: about half the size of the full
    ``lamindb`` distribution. Unpinned, any version from ``runtime.MIN_LAMINDB_VERSION`` on; older
    versions fail.
    """
    check_lamindb_version(lamindb_version)
    if lamindb_version:
        return [f"lamindb-core=={lamindb_version}", *lamindb_core_companions(lamindb_version)]
    return [f"lamindb-core>={runtime.MIN_LAMINDB_VERSION}", *lamindb_core_companions()]


def add_lamindb_requirement(requirements: list[str], lamindb_version: str | None = None) -> None:
    """Append lamindb to ``requirements`` unless the full ``lamindb`` is listed already.

    A listed ``lamindb-core`` is kept and only completed with the packages it does not declare, for
    its version if pinned with ``==``. Multi-line elements (a rendered requirements file) are searched
    line by line; a nested ``-r other.txt`` is not, so list lamindb directly or pass ``lamindb_version``
    then.
    """
    lines = list(_requirement_lines(requirements))
    lamindb = [m for line in lines if (m := _LAMINDB_REQUIREMENT.match(line))]
    if any(not m.group("core") for m in lamindb):
        return
    listed = {_requirement_name(line) for line in lines}
    if lamindb:
        pins = [pin[1] for m in lamindb if (pin := _EXACT_PIN.search(m.string))]
        to_add = list(lamindb_core_companions(pins[0] if pins else None))
    else:
        to_add = lamindb_requirements(lamindb_version)
    requirements.extend(r for r in to_add if _requirement_name(r) not in listed)


@dataclass(frozen=True)
class RemoteLaminDB:
    """How a virtualenv or pod reaches LaminDB, resolved on the worker at execute time.

    With a connection, the instance and API key come from the Airflow ``lamindb`` connection;
    ``instance`` overrides the connection's instance. With ``conn_id=None``, the remote lamindb uses
    its own configuration (``LAMIN_API_KEY``, ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``) and connects to
    ``instance`` if given.

    ``cache_dir`` is the lamindb cache shared by virtualenvs: ``cache_dir`` if given, else the
    connection's ``cache_dir`` extra; ``None`` keeps lamindb's default.
    """

    conn_id: str | None
    instance: str | None
    cache_dir: str | None = None
    api_key: str | None = field(default=None, repr=False)
    _hook: LaminDBHook | None = field(default=None, repr=False, compare=False)

    @classmethod
    def resolve(
        cls, conn_id: str | None, instance: str | None, cache_dir: str | None = None
    ) -> RemoteLaminDB:
        if conn_id is None:
            return cls(
                conn_id=None,
                instance="/".join(parse_instance_slug(instance)) if instance else None,
                cache_dir=cache_dir,
            )
        hook = LaminDBHook(lamindb_conn_id=conn_id, instance=instance)
        try:
            return cls(
                conn_id=conn_id,
                instance=hook.get_instance_slug(),
                cache_dir=cache_dir or hook.get_cache_dir(),
                api_key=hook.get_api_key(),
                _hook=hook,
            )
        except AirflowNotFoundException as e:
            raise AirflowNotFoundException(
                f"{e}. Create the {conn_id!r} connection of type 'lamindb' with the Lamin API key and the "
                "instance, or pass lamindb_conn_id=None if the virtualenv or pod brings its own lamindb "
                "credentials (LAMIN_API_KEY, LAMIN_CURRENT_INSTANCE, ~/.lamin)."
            ) from e

    def instance_lamindb_version(self) -> str | None:
        """The instance's lamindb version according to LaminHub, or None if unknown.

        Raises ``LaminDBVersionError`` for instances on lamindb older than the provider supports.
        """
        if self._hook is None:
            return None
        try:
            return self._hook.get_instance().lamindb_version
        except LaminDBVersionError:
            raise
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
    else to any supported version. The remote side must speak the instance's schema version. A
    listed lamindb is not resolved here; the virtualenv checks the version it installed.
    """
    if lamindb_version is not None:
        lamindb_version = lamindb_version.strip()
        check_lamindb_version(lamindb_version)
    requirements = list(operator.requirements)
    if not lamindb_requirement_lines(requirements):
        lamindb_version = lamindb_version or remote.instance_lamindb_version()
    add_lamindb_requirement(requirements, lamindb_version)
    operator.requirements = requirements


@contextmanager
def lamindb_virtualenv_env(operator: Any, remote: RemoteLaminDB) -> Iterator[None]:
    """Point a virtualenv operator's lamindb at the connection instead of the worker's ``~/.lamin``.

    The virtualenv gets a temporary ``LAMIN_SETTINGS_DIR`` and the connection's ``LAMIN_API_KEY``,
    plus ``LAMIN_CACHE_DIR`` if ``remote.cache_dir`` is set (also without a connection).
    They are set while executing only (``env_vars`` is not templated, so the key is never rendered),
    and explicit ``env_vars`` of the operator take precedence.
    """
    with ExitStack() as stack:
        env: dict[str, str] = {}
        if remote.conn_id is not None:
            env["LAMIN_SETTINGS_DIR"] = stack.enter_context(
                tempfile.TemporaryDirectory(prefix="lamindb-airflow-")
            )
            if remote.api_key:
                env["LAMIN_API_KEY"] = remote.api_key
        if remote.cache_dir:
            env["LAMIN_CACHE_DIR"] = remote.cache_dir
        if not env:
            yield
            return
        original = operator.env_vars
        operator.env_vars = {**env, **(original or {})}
        try:
            yield
        finally:
            operator.env_vars = original


ExcludeTemplated = Literal["secrets", "all", "none"]
"""Which templated arguments a step leaves out of its recorded parameters."""

SECRET_TEMPLATE_NAMES = frozenset({"var", "conn"})
"""Template variables that read Airflow Variables and Connections, where secrets come from."""


def _named_arguments(
    fn: Callable[..., Any], op_args: Sequence[Any] | None, op_kwargs: dict[str, Any] | None
) -> Iterator[tuple[str, Any]]:
    """Yield the parameter name of each argument; ``*args`` and ``**kwargs`` entries as in ``runtime``."""
    parameters = list(inspect.signature(fn).parameters.values())
    positional = [p.name for p in parameters if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    var_positional = next((p.name for p in parameters if p.kind is p.VAR_POSITIONAL), None)
    for index, value in enumerate(op_args or ()):
        name = positional[index] if index < len(positional) else var_positional
        if name is not None:
            yield name, value
    yield from (op_kwargs or {}).items()


def check_exclude_params(fn: Callable[..., Any], names: Iterable[str]) -> None:
    """Fail for names that can't be arguments of ``fn``."""
    parameters = inspect.signature(fn).parameters
    if any(p.kind is p.VAR_KEYWORD for p in parameters.values()):
        return
    unknown = sorted(set(names) - set(parameters))
    if unknown:
        raise ValueError(
            f"lamindb_exclude_params names {unknown}, which are not parameters of {fn.__name__}()"
        )


def _references_template(source: str, jinja_env: Any, secrets_only: bool) -> bool:
    from jinja2 import TemplateSyntaxError, nodes

    if jinja_env.variable_start_string not in source and jinja_env.block_start_string not in source:
        return False
    try:
        tree = jinja_env.parse(source)
    except TemplateSyntaxError:
        return False  # rendering fails with the reason
    if secrets_only:
        return any(node.name in SECRET_TEMPLATE_NAMES for node in tree.find_all(nodes.Name))
    return any(not isinstance(node, (nodes.Output, nodes.TemplateData)) for node in tree.find_all(nodes.Node))


def is_templated(
    value: Any,
    jinja_env: Any,
    *,
    secrets_only: bool,
    template_ext: Sequence[str] = (),
    skip_oids: Iterable[int] = (),
) -> bool:
    """Whether Airflow renders a template in ``value`` that reads a Variable or Connection, or any template.

    Walks what Airflow renders: strings, files named by ``template_ext``, containers and nested template
    fields. Values in ``skip_oids``, resolved from XComs for mapped tasks, are not rendered.
    """
    skip = set(skip_oids)

    def walk(value: Any) -> bool:
        if id(value) in skip:
            return False
        skip.add(id(value))
        if isinstance(value, str):
            source = value
            if template_ext and value.endswith(tuple(template_ext)):
                with suppress(Exception):  # rendering fails with the reason
                    source = jinja_env.loader.get_source(jinja_env, value)[0]
            return _references_template(source, jinja_env, secrets_only)
        if isinstance(value, dict):
            return any(walk(item) for item in value.values())
        if isinstance(value, (list, tuple, set, frozenset)):
            return any(walk(item) for item in value)
        try:
            fields = list(getattr(value, "template_fields", None) or ())
        except Exception:
            return False
        return any(walk(getattr(value, name, None)) for name in fields)

    return walk(value)


def templated_parameters(
    step: Any, jinja_env: Any, mode: ExcludeTemplated, skip_oids: Iterable[int] = ()
) -> set[str]:
    """Parameters of a decorated step whose arguments contain templates of the kind ``mode`` names."""
    if mode == "none":
        return set()
    oids = set(skip_oids)
    return {
        name
        for name, value in _named_arguments(step.python_callable, step.op_args, step.op_kwargs)
        if is_templated(
            value,
            jinja_env,
            secrets_only=mode == "secrets",
            template_ext=step.template_ext,
            skip_oids=oids,
        )
    }


def _secrets_redact() -> Callable[..., Any] | None:
    try:
        from airflow.sdk._shared.secrets_masker import redact
    except ImportError:
        try:
            from airflow.sdk.execution_time.secrets_masker import redact  # type: ignore[no-redef]
        except ImportError:
            return None
    return redact


def masked_parameters(
    fn: Callable[..., Any], op_args: Sequence[Any] | None, op_kwargs: dict[str, Any] | None
) -> set[str]:
    """Parameters whose argument Airflow hides in logs: a sensitive name, or a registered secret in it."""
    redact = _secrets_redact()
    if redact is None:
        return set()
    names = set()
    for name, value in _named_arguments(fn, op_args, op_kwargs):
        try:
            if bool(redact(value, name) != value):
                names.add(name)
        except Exception:
            log.debug("Could not check parameter %r for secrets", name, exc_info=True)
    return names


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
    :param lamindb_space: name or uid of the space the step run, and records the function saves,
        are created in; defaults to the flow run's space. With ``connect=True``, the space records
        the function saves are created in.
    :param auto_flow: wire ``init >> step >> finish`` with the DAG's flow operators,
        adding virtualenv ones if the DAG has none yet. Default ``False``. The concrete
        operator does the wiring once fully initialised. Ignored when ``track`` is ``False``.
    :param track: record the call in LaminDB. Pass ``False`` to ship the function
        unchanged, as the plain virtualenv/pod operator would.
    :param connect: with ``track=False``, still connect to the instance before the function
        runs, without recording anything: virtualenvs get lamindb and the connection's
        credentials as for tracked steps, pods connect with their own credentials.
    :param lamindb_exclude_params: names of parameters (or ``**kwargs`` keys) whose arguments are
        not recorded. The function still gets them.
    :param lamindb_exclude_templated: which templated arguments are not recorded: ``"secrets"``
        (default) those whose template reads an Airflow Variable or Connection (``var``, ``conn``),
        ``"all"`` every templated argument, ``"none"`` none. Arguments Airflow masks in logs are
        never recorded.
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
        auto_flow: bool = False,
        track: bool = True,
        connect: bool = False,
        lamindb_exclude_params: Sequence[str] = (),
        lamindb_exclude_templated: ExcludeTemplated = "secrets",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.lamindb_conn_id = lamindb_conn_id
        self.lamindb_instance = lamindb_instance
        self.lamindb_space = lamindb_space
        self.track = track
        self.connect = connect
        self.auto_flow = auto_flow and track
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
    def _lamindb_environment(self, remote: RemoteLaminDB) -> Iterator[None]:
        """Prepare the remote environment; pods bring lamindb and credentials in their image."""
        yield

    def execute(self, context: Any) -> Any:
        if not self.track and not self.connect:
            return super().execute(context)  # type: ignore[misc]
        remote = RemoteLaminDB.resolve(self.lamindb_conn_id, self.lamindb_instance, self.lamindb_cache_dir)
        if self.track:
            check_flow_instance(self, context, remote.instance)
            self._lamindb_runtime_function = "run_step"
            self._lamindb_remote = {
                "flow_run_reference": context_flow_run_reference(context),
                "source_code": dag_source(context["dag"]),
                "step_reference": task_instance_url(context),
                "instance": remote.instance,
                "exclude_params": self._lamindb_exclude_params(),
                "space": self._lamindb_space(),
            }
        else:
            # not part of the flow run, so it may connect to any instance
            self._lamindb_runtime_function = "run_connected"
            self._lamindb_remote = {"instance": remote.instance, "space": self._lamindb_space()}
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
            runtime_function=self._lamindb_runtime_function,
        )
