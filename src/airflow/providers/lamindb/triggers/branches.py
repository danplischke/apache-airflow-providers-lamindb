from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable, Sequence
from fnmatch import fnmatchcase
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import MAX_PAGE_SIZE, LaminDBHook, LaminDBInstance
from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger
from airflow.providers.lamindb.utils.dbwrite import previous_values, resolve_status_transitions
from airflow.providers.lamindb.utils.lamindb import (
    BLOCK_KINDS,
    branch_status,
    normalize_statuses,
    normalize_str_list,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.lamindb.utils.filters import Filter

BRANCH_TABLE = "lamindb_branch"
BRANCH_BLOCK_TABLE = "lamindb_branchblock"
_STATUS_FIELD = "_status_code"


def branch_summary(branch: dict[str, Any] | None) -> dict[str, Any] | None:
    """The branch fields included in event payloads, with the status as a name."""
    if branch is None:
        return None
    return {
        "id": branch.get("id"),
        "uid": branch.get("uid"),
        "name": branch.get("name"),
        "description": branch.get("description"),
        "status": branch_status(branch.get(_STATUS_FIELD)),
        "created_by_id": branch.get("created_by_id"),
        "created_at": branch.get("created_at"),
    }


def matches_branch_name(branch: dict[str, Any] | None, patterns: Sequence[str] | None) -> bool:
    """Whether a branch name matches any of the (case-sensitive, glob-style) patterns."""
    if not patterns:
        return True
    if branch is None or branch.get("name") is None:
        return False
    return any(fnmatchcase(branch["name"], pattern) for pattern in patterns)


def evaluate_branch_status(
    branch: dict[str, Any] | None, target_status: Sequence[str], failure_status: Sequence[str]
) -> str | None:
    """Return ``"success"``/``"failure"`` if the branch reached a target/failure status, else ``None``."""
    if branch is None:
        return None
    status = branch_status(branch.get(_STATUS_FIELD))
    if status in target_status:
        return "success"
    if status in failure_status:
        return "failure"
    return None


class LaminDBBranchStatusEventTrigger(LaminDBDbWriteEventTrigger):
    """
    Fire an event whenever the status of a LaminDB branch changes.

    Branch statuses are ``standalone``, ``draft`` and ``review`` (a Change Request is open), and
    ``merged`` and ``closed``. The event payload contains ``from_status``, ``to_status``, the
    ``branch`` (with its current ``status``), ``changed_by`` and the underlying ``dbwrite`` entry.

    :param to_status: Only report transitions to this status (or any of these statuses).
    :param from_status: Only report transitions from this status (or any of these statuses).
    :param branch_name: Only report branches whose name matches this glob pattern (or any of these
        patterns), e.g. ``"release-*"``.
    """

    def __init__(
        self,
        *,
        to_status: str | Sequence[str] | None = None,
        from_status: str | Sequence[str] | None = None,
        branch_name: str | Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.to_status = normalize_statuses(to_status, name="to_status")
        self.from_status = normalize_statuses(from_status, name="from_status")
        self.branch_name = normalize_str_list(branch_name)

    def _trigger_kwargs(self) -> dict[str, Any]:
        return {"to_status": self.to_status, "from_status": self.from_status, "branch_name": self.branch_name}

    def _dbwrite_filter(self) -> Filter:
        return {
            "and": [
                {"table_name": {"eq": BRANCH_TABLE}},
                {"event_type": {"eq": "UPDATE"}},
                {f'data["{_STATUS_FIELD}"]': {"isnull": False}},
            ]
        }

    async def _events_for(
        self, hook: LaminDBHook, instance: LaminDBInstance, writes: list[dict[str, Any]]
    ) -> list[TriggerEvent]:
        writes = [w for w in writes if _STATUS_FIELD in previous_values(w)]
        if not writes:
            return []
        branch_ids = {w["sqlrecord_id"] for w in writes}
        # the write log only stores previous values: the new status is the previous value of the next
        # status change after this batch or, if there is none, the current status
        later_codes = await self._later_status_codes(hook, branch_ids, after_id=max(w["id"] for w in writes))
        branches = await hook.aget_records_by_ids("core.branch", branch_ids)
        current_codes = {branch_id: branch.get(_STATUS_FIELD, 0) for branch_id, branch in branches.items()}

        transitions = []
        for status_write, previous_code, new_code in resolve_status_transitions(
            writes, later_codes=later_codes, current_codes=current_codes
        ):
            if new_code is None or previous_code == new_code:
                continue
            from_status, to_status = branch_status(previous_code), branch_status(new_code)
            if self.to_status and to_status not in self.to_status:
                continue
            if self.from_status and from_status not in self.from_status:
                continue
            branch = branches.get(status_write["sqlrecord_id"])
            if not matches_branch_name(branch, self.branch_name):
                continue
            transitions.append((status_write, from_status, to_status, branch))

        await self._load_users(hook, [t[0] for t in transitions])
        return [
            TriggerEvent(
                self._payload(
                    instance,
                    write,
                    event="status_changed",
                    branch=branch_summary(branch),
                    from_status=from_status,
                    to_status=to_status,
                )
            )
            for write, from_status, to_status, branch in transitions
        ]

    async def _later_status_codes(
        self, hook: LaminDBHook, branch_ids: Iterable[int], *, after_id: int
    ) -> dict[int, int]:
        """The previous status of each branch's first status change after ``after_id``, if any."""
        later_codes: dict[int, int] = {}
        unresolved = set(branch_ids)
        # only unresolved branches are queried, so every full page resolves at least one of them
        while unresolved:
            page = await hook.aquery_dbwrites(
                {"and": [*self._dbwrite_filter()["and"], {"sqlrecord_id": {"in": sorted(unresolved)}}]},
                after_id=after_id,
                limit=MAX_PAGE_SIZE,
            )
            for write in page:
                if write["sqlrecord_id"] in unresolved:
                    later_codes[write["sqlrecord_id"]] = previous_values(write)[_STATUS_FIELD]
                    unresolved.discard(write["sqlrecord_id"])
            if len(page) < MAX_PAGE_SIZE:
                break
            after_id = page[-1]["id"]
        return later_codes


class LaminDBBranchBlockEventTrigger(LaminDBDbWriteEventTrigger):
    """
    Fire an event whenever a comment or readme is added to a LaminDB branch.

    Comments and readmes are ``core.branchblock`` records; a new readme version is a new block. The
    event payload contains the ``kind``, the ``block`` (including its Markdown ``content``), the
    ``branch``, ``changed_by`` and the underlying ``dbwrite`` entry.

    :param kinds: Block kinds to report, ``comment`` and/or ``readme``.
    :param branch_name: Only report blocks on branches whose name matches this glob pattern (or any
        of these patterns).
    """

    def __init__(
        self,
        *,
        kinds: str | Sequence[str] = BLOCK_KINDS,
        branch_name: str | Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kinds = normalize_str_list(kinds) or []
        invalid = [kind for kind in self.kinds if kind not in BLOCK_KINDS]
        if invalid or not self.kinds:
            raise ValueError(
                f"Invalid block kinds {self.kinds}; expected a non-empty subset of {BLOCK_KINDS}"
            )
        self.branch_name = normalize_str_list(branch_name)

    def _trigger_kwargs(self) -> dict[str, Any]:
        return {"kinds": self.kinds, "branch_name": self.branch_name}

    def _dbwrite_filter(self) -> Filter:
        return {"and": [{"table_name": {"eq": BRANCH_BLOCK_TABLE}}, {"event_type": {"eq": "INSERT"}}]}

    async def _events_for(
        self, hook: LaminDBHook, instance: LaminDBInstance, writes: list[dict[str, Any]]
    ) -> list[TriggerEvent]:
        blocks = await hook.aget_records_by_ids(
            "core.branchblock", [w["sqlrecord_id"] for w in writes], filter={"kind": {"in": self.kinds}}
        )
        if not blocks:
            return []
        branches = await hook.aget_records_by_ids(
            "core.branch", {b["branch_id"] for b in blocks.values() if b.get("branch_id") is not None}
        )
        selected = []
        for write in writes:
            block = blocks.get(write["sqlrecord_id"])
            if block is None:
                continue
            branch = branches.get(block.get("branch_id"))  # type: ignore[arg-type]
            if matches_branch_name(branch, self.branch_name):
                selected.append((write, block, branch))

        await self._load_users(hook, [s[0] for s in selected])
        return [
            TriggerEvent(
                self._payload(
                    instance,
                    write,
                    event="block_created",
                    kind=block.get("kind"),
                    block=block,
                    branch=branch_summary(branch),
                )
            )
            for write, block, branch in selected
        ]


class LaminDBBranchStatusSensorTrigger(BaseTrigger):
    """
    Wait until a LaminDB branch reaches a target status.

    Used by :class:`~airflow.providers.lamindb.sensors.branches.LaminDBBranchStatusSensor` in deferrable
    mode. Yields ``{"status": "success" | "failure" | "error", "branch": ..., "message": ...}``.

    :param branch: Branch name or id.
    :param target_status: Statuses that end the wait successfully.
    :param failure_status: Statuses that end the wait with a failure.
    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param poll_interval: Seconds between checks.
    """

    def __init__(
        self,
        branch: str | int,
        *,
        target_status: Sequence[str] = ("merged",),
        failure_status: Sequence[str] = ("closed",),
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        poll_interval: float = 60.0,
    ) -> None:
        super().__init__()
        self.branch = branch
        self.target_status = list(target_status)
        self.failure_status = list(failure_status)
        self.lamindb_conn_id = lamindb_conn_id
        self.instance = instance
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "branch": self.branch,
                "target_status": self.target_status,
                "failure_status": self.failure_status,
                "lamindb_conn_id": self.lamindb_conn_id,
                "instance": self.instance,
                "poll_interval": self.poll_interval,
            },
        )

    def _get_hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self._get_hook()
        try:
            while True:
                try:
                    branch = await hook.aget_branch(self.branch)
                except (LaminDBApiError, ValueError) as err:
                    yield TriggerEvent({"status": "error", "message": str(err)})
                    return
                outcome = evaluate_branch_status(branch, self.target_status, self.failure_status)
                if outcome is not None:
                    summary = branch_summary(branch)
                    yield TriggerEvent(
                        {
                            "status": outcome,
                            "branch": summary,
                            "message": f"Branch {self.branch!r} has status {summary['status']!r}",  # type: ignore[index]
                        }
                    )
                    return
                self.log.info("Branch %r has not reached any of %s yet", self.branch, self.target_status)
                await asyncio.sleep(self.poll_interval)
        finally:
            await hook.aclose()
