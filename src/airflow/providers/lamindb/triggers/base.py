from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import deque
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import MAX_PAGE_SIZE, LaminDBHook, LaminDBInstance
from airflow.providers.lamindb.utils.dbwrite import dbwrite_summary
from airflow.triggers.base import BaseEventTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.lamindb.utils.filters import Filter

_MIN_COMMIT_DELAY = 5.0
"""Minimum seconds between yielding events and committing the cursor past them."""
_MAX_ERROR_BACKOFF = 600.0

log = logging.getLogger(__name__)


def _state_accessors(store: Any) -> list[Any]:
    """Return one accessor per watched asset from an ``AssetStateStoreAccessors`` (or a single accessor).

    Airflow has no public API that lists the accessors of several watched assets, so this reads
    ``_by_name``/``_by_uri``; ``test_base.py`` checks them against the installed Airflow.
    """
    if store is None:
        return []
    by_name = getattr(store, "_by_name", None)
    by_uri = getattr(store, "_by_uri", None)
    if isinstance(by_name, dict) and isinstance(by_uri, dict):
        return [*by_name.values(), *by_uri.values()]
    if callable(getattr(store, "aget", None)) and callable(getattr(store, "aset", None)):
        return [store]
    log.warning(
        "Unsupported asset state store %s; the cursor is kept in memory only", type(store).__qualname__
    )
    return []


class _CursorStore:
    """Persist the id of the last processed database write in the asset state store."""

    def __init__(self, store: Any, key: str) -> None:
        self.key = key
        self._accessors = _state_accessors(store)

    @property
    def is_persistent(self) -> bool:
        return bool(self._accessors)

    async def load(self, instance_id: str) -> int | None:
        cursors = []
        for accessor in self._accessors:
            value = await accessor.aget(self.key, None)
            if (
                isinstance(value, Mapping)
                and value.get("instance_id") == instance_id
                and isinstance(value.get("dbwrite_id"), int)
            ):
                cursors.append(value["dbwrite_id"])
        # with several watched assets, resume from the oldest cursor (at-least-once)
        return min(cursors) if cursors else None

    async def save(self, cursor: int, instance_id: str) -> None:
        for accessor in self._accessors:
            await accessor.aset(self.key, {"dbwrite_id": cursor, "instance_id": instance_id})


class LaminDBDbWriteEventTrigger(BaseEventTrigger):
    """
    Base class for event-driven scheduling triggers that watch the LaminHub database write log.

    LaminHub logs every ``INSERT``, ``UPDATE`` and ``DELETE`` on a LaminDB instance's database in the
    ``hubmodule.dbwrite`` registry ("Changes → Database writes" in the LaminHub UI). Subclasses define
    which writes to poll and how to turn them into trigger events.

    The trigger polls writes with an id greater than its cursor. When used in an
    :class:`~airflow.sdk.AssetWatcher`, the cursor is stored in the asset state store under
    :attr:`state_key` so that a restarted triggerer resumes where it left off. The cursor is committed
    one poll cycle after the events were yielded, which gives Airflow time to persist them: events
    are delivered at least once. On the first start the trigger only reports writes that happen from
    then on.

    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param poll_interval: Seconds between polls of the write log.
    :param batch_size: Number of writes fetched per request (at most 200).
    :param max_batches_per_poll: Maximum number of batches processed per poll cycle.
    """

    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        poll_interval: float = 30.0,
        batch_size: int = MAX_PAGE_SIZE,
        max_batches_per_poll: int = 10,
    ) -> None:
        super().__init__()
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if not 1 <= batch_size <= MAX_PAGE_SIZE:
            raise ValueError(f"batch_size must be between 1 and {MAX_PAGE_SIZE}")
        if max_batches_per_poll < 1:
            raise ValueError("max_batches_per_poll must be at least 1")
        self.lamindb_conn_id = lamindb_conn_id
        self.instance = instance
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.max_batches_per_poll = max_batches_per_poll
        self._users: dict[int, dict[str, Any]] = {}

    # -- subclass API --------------------------------------------------------------------------------

    def _trigger_kwargs(self) -> dict[str, Any]:
        """Subclass-specific keyword arguments; they define :attr:`state_key` and are serialized."""
        raise NotImplementedError

    async def _setup(self, hook: LaminDBHook) -> None:
        """Resolve anything that requires the API (registries, branch ids); called before polling."""

    def _dbwrite_filter(self) -> Filter:
        """Filter for the database writes this trigger is interested in."""
        raise NotImplementedError

    async def _events_for(
        self, hook: LaminDBHook, instance: LaminDBInstance, writes: list[dict[str, Any]]
    ) -> list[TriggerEvent]:
        """Turn a batch of database writes (ordered by id) into trigger events."""
        raise NotImplementedError

    # -- trigger implementation ----------------------------------------------------------------------

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                **self._trigger_kwargs(),
                "lamindb_conn_id": self.lamindb_conn_id,
                "instance": self.instance,
                "poll_interval": self.poll_interval,
                "batch_size": self.batch_size,
                "max_batches_per_poll": self.max_batches_per_poll,
            },
        )

    @property
    def state_key(self) -> str:
        """Asset state store key of the cursor; changes when the trigger's filters change."""
        identity = {
            **self._trigger_kwargs(),
            "lamindb_conn_id": self.lamindb_conn_id,
            "instance": self.instance,
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()
        return f"lamindb.{type(self).__name__}.{digest[:16]}"

    def _get_hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    async def _initial_cursor(
        self, hook: LaminDBHook, cursor_store: _CursorStore, instance: LaminDBInstance
    ) -> int:
        stored = await cursor_store.load(instance.id)
        if stored is not None:
            self.log.info("Resuming after database write %d of %s", stored, instance.slug)
            return stored
        latest = await hook.aget_latest_dbwrite_id()
        await cursor_store.save(latest, instance.id)
        self.log.info("Watching database writes of %s after write %d", instance.slug, latest)
        return latest

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self._get_hook()
        cursor_store = _CursorStore(self.asset_state_store, self.state_key)
        if not cursor_store.is_persistent:
            self.log.warning(
                "No asset state store available; the cursor is kept in memory only. "
                "Use this trigger in an AssetWatcher to resume after triggerer restarts."
            )
        cursor: int | None = None
        # (cursor, yielded_at), oldest first
        pending: deque[tuple[int, float]] = deque()
        is_setup = False
        failures = 0
        try:
            while True:
                try:
                    instance = await hook.aget_instance()
                    if not is_setup:
                        await self._setup(hook)
                        is_setup = True
                    if cursor is None:
                        cursor = await self._initial_cursor(hook, cursor_store, instance)
                    await self._commit_pending(cursor_store, pending, instance.id)
                    for _ in range(self.max_batches_per_poll):
                        writes = await hook.aquery_dbwrites(
                            self._dbwrite_filter(), after_id=cursor, limit=self.batch_size
                        )
                        if not writes:
                            break
                        for event in await self._events_for(hook, instance, writes):
                            yield event
                        cursor = max(int(write["id"]) for write in writes)
                        pending.append((cursor, time.monotonic()))
                        if len(writes) < self.batch_size:
                            break
                    failures = 0
                    delay = self.poll_interval
                except Exception:
                    failures += 1
                    delay = min(self.poll_interval * 2 ** min(failures, 6), _MAX_ERROR_BACKOFF)
                    self.log.exception(
                        "Polling the LaminHub database write log failed (%d consecutive failures); "
                        "retrying in %.0f seconds",
                        failures,
                        delay,
                    )
                await asyncio.sleep(delay)
        finally:
            await hook.aclose()

    @staticmethod
    async def _commit_pending(
        cursor_store: _CursorStore, pending: deque[tuple[int, float]], instance_id: str
    ) -> None:
        """Commit the newest pending cursor whose events were yielded at least _MIN_COMMIT_DELAY ago."""
        now = time.monotonic()
        ready = None
        while pending and now - pending[0][1] >= _MIN_COMMIT_DELAY:
            ready = pending.popleft()[0]
        if ready is not None:
            await cursor_store.save(ready, instance_id)

    # -- helpers for subclasses ----------------------------------------------------------------------

    async def _load_users(self, hook: LaminDBHook, writes: Iterable[Mapping[str, Any]]) -> None:
        missing = {w["created_by_id"] for w in writes if w.get("created_by_id") is not None} - set(
            self._users
        )
        if not missing:
            return
        try:
            self._users.update(await hook.aget_users(missing))
        except LaminDBApiError:
            self.log.warning("Could not look up LaminDB users %s", sorted(missing), exc_info=True)

    def _user_summary(self, user_id: int | None) -> dict[str, Any] | None:
        if user_id is None:
            return None
        user = self._users.get(user_id, {})
        return {"id": user_id, "handle": user.get("handle"), "name": user.get("name")}

    def _payload(self, instance: LaminDBInstance, write: Mapping[str, Any], **fields: Any) -> dict[str, Any]:
        return {
            "instance": instance.slug,
            "instance_id": instance.id,
            **fields,
            "changed_by": self._user_summary(write.get("created_by_id")),
            "dbwrite": dbwrite_summary(write),
        }
