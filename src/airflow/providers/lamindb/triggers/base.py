from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from collections.abc import AsyncIterator, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import MAX_PAGE_SIZE, LaminDBHook, LaminDBInstance
from airflow.providers.lamindb.triggers.cursors import CursorBackend, CursorStore
from airflow.providers.lamindb.utils.dbwrite import dbwrite_summary
from airflow.triggers.base import BaseEventTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.lamindb.utils.filters import Filter

_MAX_ERROR_BACKOFF = 600.0


class LaminDBDbWriteEventTrigger(BaseEventTrigger):
    """
    Base class for event-driven scheduling triggers that watch the LaminHub database write log.

    LaminHub logs every ``INSERT``, ``UPDATE`` and ``DELETE`` on a LaminDB instance's database in the
    ``hubmodule.dbwrite`` registry ("Changes → Database writes" in the LaminHub UI). Subclasses define
    which writes to poll and how to turn them into trigger events.

    The trigger polls writes with an id greater than its cursor, and stores the cursor under
    :attr:`state_key` so that a restarted triggerer resumes where it left off. The cursor is committed
    at least ``commit_delay`` seconds after the events were yielded, which gives Airflow time to persist
    them: events are delivered at least once. It is only committed when the trigger processed new
    writes. On the first start the trigger only reports writes that happen from then on.

    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param poll_interval: Seconds between polls of the write log.
    :param batch_size: Number of writes fetched per request (at most 200).
    :param max_batches_per_poll: Maximum number of batches processed per poll cycle.
    :param cursor_store: Where and how to store the cursor:
        :class:`~airflow.providers.lamindb.triggers.cursors.AssetCursorStore` (default) in the asset
        state store of the :class:`~airflow.sdk.AssetWatcher`'s assets, or
        :class:`~airflow.providers.lamindb.triggers.cursors.RecordCursorStore` in a record of the
        watched instance. Both also set the ``commit_delay``.
    """

    def __init__(
        self,
        *,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        poll_interval: float = 30.0,
        batch_size: int = MAX_PAGE_SIZE,
        max_batches_per_poll: int = 10,
        cursor_store: CursorStore | Mapping[str, Any] | None = None,
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
        self.cursor_store = CursorStore.coerce(cursor_store)
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

    async def _should_commit(self, hook: LaminDBHook, writes: list[dict[str, Any]]) -> bool:
        """Whether a processed batch moves the stored cursor; ``False`` for writes of cursor records.

        Called after :meth:`_events_for`. Skipping the commit only costs re-reading the batch after a
        restart, but stops triggers that store cursors in LaminDB from reacting to each other's saves.
        """
        return True

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
                "cursor_store": self.cursor_store.serialize(),
            },
        )

    @property
    def state_key(self) -> str:
        """Key of the cursor (and default name of its record); changes when the trigger's filters change."""
        digest = hashlib.sha256(self._identity().encode()).hexdigest()
        return f"lamindb.{type(self).__name__}.{digest[:16]}"

    def _get_hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    def _identity(self) -> str:
        """The trigger's arguments that define what it watches, as JSON."""
        identity = {
            **self._trigger_kwargs(),
            "lamindb_conn_id": self.lamindb_conn_id,
            "instance": self.instance,
        }
        return json.dumps(identity, sort_keys=True, default=str)

    def _open_cursor(self, hook: LaminDBHook) -> CursorBackend:
        return self.cursor_store.open(
            hook=hook,
            asset_state_store=self.asset_state_store,
            key=self.state_key,
            description=f"Cursor of the Airflow trigger {type(self).__qualname__}: {self._identity()}",
        )

    async def _initial_cursor(
        self, hook: LaminDBHook, cursor_store: CursorBackend, instance: LaminDBInstance
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
        cursor_store = self._open_cursor(hook)
        if not cursor_store.is_persistent:
            self.log.warning(
                "No asset state store available; the cursor is kept in memory only. Use this trigger "
                "in an AssetWatcher, or pass cursor_store=RecordCursorStore(), to resume after restarts."
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
                        if await self._should_commit(hook, writes):
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

    async def _commit_pending(
        self, cursor_store: CursorBackend, pending: deque[tuple[int, float]], instance_id: str
    ) -> None:
        """Commit the newest pending cursor whose events were yielded at least ``commit_delay`` ago."""
        delay = self.cursor_store.commit_delay
        now = time.monotonic()
        ready = None
        while pending and now - pending[0][1] >= delay:
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
