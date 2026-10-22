"""Deployment-specific preparation of the virtualenvs and pods that run lamindb."""

from __future__ import annotations

import functools
from typing import Any

from airflow.exceptions import AirflowException


class LaminDBSetup:
    """Deployment-specific preparation of the virtualenvs and pods that run lamindb.

    Subclass it and name the subclass in the ``[lamindb] setup_class`` Airflow option, e.g.
    ``AIRFLOW__LAMINDB__SETUP_CLASS=mycompany.airflow.SiteSetup``: then every LaminDB task of the
    installation uses it, loaded once per process. A DAG or task overrides it with a
    ``lamindb_setup`` instance, in ``default_args`` or as an argument; ``LaminDBSetup()`` itself does
    nothing, so it opts out. Wiring fails when a step needs another setup than its DAG's flow tasks
    run; a step that opts out needs none. Setups are equal when their class and ``repr`` are.

    In a task, the steps run in this order:

    1. Airflow renders the templates.
    2. The worker resolves the lamindb connection and calls [`execution_env`][].
    3. The virtualenv is built with lamindb and the setup's [`requirements`][], or the pod started.
    4. In the virtualenv or pod, the [`bootstrap_source`][] runs, then lamindb connects, then the
       task's function runs.

    Airflow stores ``default_args`` in the serialized DAG through ``repr()``. The default ``repr``
    names the class, so the DAG doesn't get a new version on every parse; a subclass that takes
    settings should include them in its ``repr``, so that changing them makes a new DAG version.
    """

    def __repr__(self) -> str:
        # Airflow serializes default_args with repr(); the default one contains the object's address,
        # which would make every parse of the DAG a new DAG version
        return f"{type(self).__module__}.{type(self).__qualname__}()"

    def __eq__(self, other: object) -> bool:
        # setups are equal when their repr is: task groups deep-copy their default_args, and steps
        # may each create their own instance of the same setup
        if not isinstance(other, LaminDBSetup):
            return NotImplemented
        return type(self) is type(other) and repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash((type(self), repr(self)))

    def bootstrap_source(self) -> str:
        """Python source that runs in the virtualenv or pod before lamindb is imported.

        For example to point Python at a custom certificate bundle. It runs as its own module, so it
        may be indented or start with ``from __future__``, and its names don't leak into the task's
        script. In a virtualenv it runs before Airflow unpickles the function's arguments; in a pod,
        after.
        """
        return ""

    def execution_env(self, context: Any) -> dict[str, str]:
        """Environment variables for a task's virtualenv, resolved on the worker when the task runs.

        For example values of Airflow Variables or Connections: they are never rendered into the
        task's template fields. Keys and values must be strings. The provider's own lamindb variables
        (``LAMIN_SETTINGS_DIR``, ``LAMIN_API_KEY``, ``LAMIN_CACHE_DIR``) take precedence, and a task's
        own ``env_vars`` take precedence over both. ``context`` is the task's Airflow context. Pod
        steps don't call it: pods get their environment from the pod spec. When it fails, the flow
        finish task logs it and still closes the flow run.
        """
        return {}

    def requirements(self) -> list[str]:
        """Default requirements installed in every LaminDB virtualenv, in addition to lamindb.

        For example ``["bionty", "pandas>=2"]``, packages all of a site's tasks need. They are
        added to a task's ``requirements`` when it runs, after the task's own; a package the task lists
        itself is not added again, so tasks pin their own versions. lamindb itself is not allowed here:
        pass ``lamindb_version`` instead, or list it in the task's ``requirements``. Pods don't use
        them: the image brings its packages. Each element must be a string.
        """
        return []


@functools.cache
def configured_setup() -> LaminDBSetup | None:
    """The installation's ``LaminDBSetup``: the ``[lamindb] setup_class`` Airflow option, if set.

    Read and instantiated once per process, so all tasks share the instance.
    """
    from airflow.sdk.configuration import conf
    from airflow.sdk.module_loading import import_string

    path = (conf.get("lamindb", "setup_class", fallback="") or "").strip()
    if not path:
        return None
    try:
        setup_class = import_string(path)
    except ImportError as e:
        raise AirflowException(f"[lamindb] setup_class {path!r} can't be imported: {e}") from e
    if not (isinstance(setup_class, type) and issubclass(setup_class, LaminDBSetup)):
        raise AirflowException(f"[lamindb] setup_class {path!r} is not a subclass of LaminDBSetup")
    return setup_class()


def resolve_setup(setup: LaminDBSetup | None) -> LaminDBSetup | None:
    """A task's ``lamindb_setup``, else the installation's."""
    return setup if setup is not None else configured_setup()


def setup_execution_env(setup: LaminDBSetup | None, context: Any) -> dict[str, str]:
    setup = resolve_setup(setup)
    if setup is None:
        return {}
    env = dict(setup.execution_env(context))
    invalid = sorted(
        repr(key) for key, value in env.items() if not isinstance(key, str) or not isinstance(value, str)
    )
    if invalid:
        raise TypeError(
            f"{setup!r}.execution_env() must return strings; the values of {', '.join(invalid)} are not"
        )
    return env


def setup_requirements(setup: LaminDBSetup | None) -> list[str]:
    setup = resolve_setup(setup)
    if setup is None:
        return []
    requirements = list(setup.requirements())
    invalid = sorted(repr(r) for r in requirements if not isinstance(r, str))
    if invalid:
        raise TypeError(f"{setup!r}.requirements() must return strings; {', '.join(invalid)} are not")
    return requirements


def setup_bootstrap_source(setup: LaminDBSetup | None) -> str:
    setup = resolve_setup(setup)
    return setup.bootstrap_source() if setup is not None else ""
