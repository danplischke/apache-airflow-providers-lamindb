"""How virtualenvs and pods reach LaminDB: credentials, cache and lamindb version."""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from typing import Any

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.lamindb.client.client import parse_instance_slug
from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.execution.requirements import (
    add_lamindb_requirement,
    check_lamindb_version,
    lamindb_requirement_lines,
)
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook

log = logging.getLogger(__name__)


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
        lamindb_version = str(lamindb_version).strip()
        check_lamindb_version(lamindb_version)
    requirements = list(operator.requirements)
    if not lamindb_requirement_lines(requirements):
        lamindb_version = lamindb_version or remote.instance_lamindb_version()
    add_lamindb_requirement(requirements, lamindb_version)
    operator.requirements = requirements


@contextmanager
def lamindb_virtualenv_env(
    operator: Any, remote: RemoteLaminDB, setup_env: dict[str, str] | None = None
) -> Iterator[None]:
    """Point a virtualenv operator's lamindb at the connection instead of the worker's ``~/.lamin``.

    The virtualenv gets a temporary ``LAMIN_SETTINGS_DIR`` and the connection's ``LAMIN_API_KEY``,
    plus ``LAMIN_CACHE_DIR`` if ``remote.cache_dir`` is set (also without a connection), on top of
    ``setup_env`` from ``LaminDBSetup.execution_env``. They are set while executing only
    (``env_vars`` is not templated, so the key is never rendered), and explicit ``env_vars`` of the
    operator take precedence.
    """
    with ExitStack() as stack:
        env: dict[str, str] = dict(setup_env or {})
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
