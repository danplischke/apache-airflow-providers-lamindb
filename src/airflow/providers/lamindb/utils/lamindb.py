from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any, Literal

from airflow.providers.lamindb.utils.enums import plain_value
from airflow.providers.lamindb.utils.filters import FilterLike, combine_filters

BranchStatus = Literal["standalone", "draft", "review", "merged", "closed"]

BRANCH_STATUS_TO_CODE: dict[str, int] = {
    "closed": -2,
    "merged": -1,
    "standalone": 0,
    "draft": 1,
    "review": 2,
}
"""LaminDB branch statuses and the codes stored in ``Branch._status_code``."""

BRANCH_CODE_TO_STATUS: dict[int, str] = {code: status for status, code in BRANCH_STATUS_TO_CODE.items()}

SPECIAL_BRANCH_IDS: dict[str, int] = {"main": 1, "archive": 0, "trash": -1}
"""Built-in branches that every LaminDB instance has, with fixed ids."""

INTERNAL_ARTIFACT_KINDS: tuple[str, ...] = ("__lamindb_run__", "__lamindb_config__")
"""Artifact kinds LaminDB uses internally (run logs, environments, configs)."""

BLOCK_KINDS: tuple[str, ...] = ("comment", "readme")


def branch_status(code: int | None) -> str:
    """Return the branch status name for a ``_status_code``."""
    if code is None:
        return "standalone"
    return BRANCH_CODE_TO_STATUS.get(int(code), "standalone")


def normalize_statuses(statuses: str | Iterable[str] | None, *, name: str) -> list[str] | None:
    """Validate branch status names; returns ``None`` if no statuses were given."""
    if statuses is None:
        return None
    values = [statuses] if isinstance(statuses, str) else list(statuses)
    invalid = [status for status in values if status not in BRANCH_STATUS_TO_CODE]
    if invalid:
        raise ValueError(
            f"Invalid branch status(es) {invalid} for {name}; expected any of {sorted(BRANCH_STATUS_TO_CODE)}"
        )
    return values


def normalize_str_list(value: str | Iterable[str] | None) -> list[str] | None:
    """Return a list of plain strings; enum members such as ``ArtifactKind.DATASET`` become their values."""
    if value is None:
        return None
    values = [value] if isinstance(value, str) else list(value)
    return [plain_value(item) for item in values]


def parse_json_value(value: Any) -> Any:
    """
    Decode JSON columns in the database write log.

    Previous values of JSON fields (such as ``_aux``) are stored as JSON-encoded strings.
    """
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def storage_ongoing(aux: Any) -> bool:
    """Whether an artifact's ``_aux`` marks its upload to storage as ongoing (``{"so": 1}``)."""
    aux = parse_json_value(aux)
    return isinstance(aux, dict) and aux.get("so") == 1


def is_internal_artifact(record: dict[str, Any]) -> bool:
    return str(record.get("kind") or "").startswith("__lamindb")


def artifact_filter(
    *,
    key: str | None = None,
    key_prefix: str | None = None,
    suffix: str | Iterable[str] | None = None,
    kind: str | Iterable[str] | None = None,
    include_internal: bool = False,
    extra: FilterLike | None = None,
) -> dict[str, Any] | None:
    """Build a LaminHub REST filter for artifacts from common criteria."""
    suffixes = normalize_str_list(suffix)
    kinds = normalize_str_list(kind)
    return combine_filters(
        extra,
        {"key": {"eq": key}} if key is not None else None,
        {"key": {"startswith": key_prefix}} if key_prefix is not None else None,
        {"suffix": {"in": suffixes}} if suffixes is not None else None,
        {"kind": {"in": kinds}} if kinds is not None else None,
        None
        if include_internal or kinds is not None
        else {"or": [{"kind": {"isnull": True}}, {"kind": {"notin": list(INTERNAL_ARTIFACT_KINDS)}}]},
    )
