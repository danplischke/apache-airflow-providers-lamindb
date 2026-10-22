"""LaminDB branches: statuses and built-in branches."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

BranchStatus = Literal["standalone", "draft", "review", "merged", "closed"]


BRANCH_STATUS_TO_CODE: dict[BranchStatus, int] = {
    "closed": -2,
    "merged": -1,
    "standalone": 0,
    "draft": 1,
    "review": 2,
}
"""LaminDB branch statuses and the codes stored in ``Branch._status_code``."""


BRANCH_CODE_TO_STATUS: dict[int, BranchStatus] = {
    code: status for status, code in BRANCH_STATUS_TO_CODE.items()
}


SPECIAL_BRANCH_IDS: dict[str, int] = {"main": 1, "archive": 0, "trash": -1}
"""Built-in branches that every LaminDB instance has, with fixed ids."""


BLOCK_KINDS: tuple[str, ...] = ("comment", "readme")


def branch_status(code: int | None) -> BranchStatus:
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
