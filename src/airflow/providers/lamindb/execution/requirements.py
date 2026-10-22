"""The lamindb requirements of virtualenvs."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from typing import Any

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.execution import runtime

_LAMINDB_NAMES = {"lamindb", "lamindb-core"}
_REQUIREMENT_COMMENT = re.compile(r"(^|\s)#.*$")


def _requirement_lines(requirements: Iterable[Any]) -> Iterator[str]:
    """Yield each requirement, also from multi-line elements such as a rendered requirements file.

    Skips blank lines, comments and pip options such as ``-r other.txt``, whose contents can't be seen.
    """
    for requirement in requirements:
        for line in str(requirement).splitlines():
            line = _REQUIREMENT_COMMENT.sub("", line).strip()
            if line and not line.startswith("-"):
                yield line


def lamindb_requirement_lines(requirements: Iterable[Any]) -> list[str]:
    """The ``lamindb`` and ``lamindb-core`` requirements among ``requirements``."""
    return [line for line in _requirement_lines(requirements) if _requirement_name(line) in _LAMINDB_NAMES]


def _requirement_name(line: str) -> str | None:
    """The normalized project name of a requirement; ``None`` for lines that aren't one, such as paths."""
    try:
        return canonicalize_name(Requirement(line).name)
    except InvalidRequirement:
        return None


def is_template(value: Any) -> bool:
    """Whether ``value`` is a string with Jinja markup, known only once Airflow renders it."""
    return isinstance(value, str) and ("{{" in value or "{%" in value)


def check_lamindb_version(lamindb_version: str | None) -> None:
    """Fail for a lamindb version older than ``runtime.MIN_LAMINDB_VERSION``, the oldest one supported.

    Templates pass; they are checked once rendered, before anything is installed. Other values are
    checked as strings, as DAGs that render templates as native objects may produce numbers.
    """
    if lamindb_version is None or lamindb_version == "" or is_template(lamindb_version):
        return
    lamindb_version = str(lamindb_version)
    if not re.fullmatch(r"\s*\d+(\.\d+)+\S*\s*", lamindb_version):
        raise LaminDBVersionError(
            f"lamindb_version {lamindb_version!r} is not a lamindb version, such as '2.11.0'"
        )
    if not runtime.lamindb_version_supported(lamindb_version):
        raise LaminDBVersionError(
            f"lamindb {lamindb_version} is not supported: apache-airflow-providers-lamindb needs lamindb "
            f"{runtime.MIN_LAMINDB_VERSION} or newer."
        )


def lamindb_requirement(lamindb_version: str | None = None) -> str:
    """The requirement that installs lamindb in a virtualenv, pinned to ``lamindb_version`` if given.

    Unpinned, any version from ``runtime.MIN_LAMINDB_VERSION`` on; older versions fail.
    """
    check_lamindb_version(lamindb_version)
    if lamindb_version:
        return f"lamindb=={lamindb_version}"
    return f"lamindb>={runtime.MIN_LAMINDB_VERSION}"


def add_lamindb_requirement(requirements: list[str], lamindb_version: str | None = None) -> None:
    """Append lamindb to ``requirements`` unless ``lamindb`` or ``lamindb-core`` is listed already.

    Multi-line elements (a rendered requirements file) are searched line by line; a nested
    ``-r other.txt`` is not, so list lamindb directly or pass ``lamindb_version`` then.
    """
    if not lamindb_requirement_lines(requirements):
        requirements.append(lamindb_requirement(lamindb_version))
