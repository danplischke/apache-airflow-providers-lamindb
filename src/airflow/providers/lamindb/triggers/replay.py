"""Replays: deliver the events of past database writes again, without moving a trigger's cursor.

A replay request lives in the asset state store of the watched asset, under the trigger's
[`state_key`][airflow.providers.lamindb.triggers.base.LaminDBDbWriteEventTrigger.state_key] plus
``.replay``. Write it from a task with [`request_replay`][], or through Airflow's REST API. The
running trigger picks it up on its next poll and delivers the events of the requested writes again,
next to its live events, with ``"replay": {"id": ...}`` added to their payloads. It records the
outcome in the request's ``status``.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, TypeGuard

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers.cursors import _state_accessors
from airflow.providers.lamindb.utils.filters import combine_filters
from airflow.triggers.base import TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.lamindb.hooks.lamindb import LaminDBHook, LaminDBInstance
    from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger

REPLAY_KEY_SUFFIX = ".replay"
"""Appended to a trigger's ``state_key`` to get the asset state store key of its replay request."""
MAX_REPLAY_DBWRITE_IDS = 10_000
"""Maximum number of ``dbwrite_ids`` in one replay request."""
FINAL_STATUSES = frozenset({"done", "invalid", "failed"})
"""Statuses of requests the trigger is finished with; it ignores them."""

_REQUEST_FIELDS = frozenset({"id", "from_dbwrite_id", "to_dbwrite_id", "dbwrite_ids"})
_STATUS_FIELDS = frozenset({"status", "error", "events", "started_at", "finished_at"})
_MAX_ID_LENGTH = 200


def replay_key(state_key: str) -> str:
    """The asset state store key of the replay request for the trigger with ``state_key``."""
    return state_key + REPLAY_KEY_SUFFIX


def _is_dbwrite_id(value: Any) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


@dataclass(frozen=True)
class ReplayRequest:
    """
    Which database writes to replay: a range of ids, or a list of them.

    :param id: Identifies the request; added to the payload of every replayed event.
    :param from_dbwrite_id: First write of the range (inclusive).
    :param to_dbwrite_id: Last write of the range (inclusive); ``None`` for the last write the
        trigger has processed.
    :param dbwrite_ids: The writes to replay, instead of a range.
    """

    id: str | None = None
    from_dbwrite_id: int | None = None
    to_dbwrite_id: int | None = None
    dbwrite_ids: tuple[int, ...] | None = None

    def __post_init__(self) -> None:
        if self.id is not None and not (isinstance(self.id, str) and 0 < len(self.id) <= _MAX_ID_LENGTH):
            raise ValueError(f"id must be a string of 1 to {_MAX_ID_LENGTH} characters, got {self.id!r}")
        if self.dbwrite_ids is not None:
            if self.from_dbwrite_id is not None or self.to_dbwrite_id is not None:
                raise ValueError(
                    "Pass either dbwrite_ids or a range (from_dbwrite_id, to_dbwrite_id), not both"
                )
            if not isinstance(self.dbwrite_ids, tuple) or not self.dbwrite_ids:
                raise ValueError("dbwrite_ids must be a non-empty list of database write ids")
            if len(self.dbwrite_ids) > MAX_REPLAY_DBWRITE_IDS:
                raise ValueError(
                    f"dbwrite_ids has more than {MAX_REPLAY_DBWRITE_IDS} ids; replay a range instead"
                )
            if invalid := [i for i in self.dbwrite_ids if not _is_dbwrite_id(i)]:
                raise ValueError(f"dbwrite_ids must be positive integers, got {invalid[:5]!r}")
            return
        first, last = self.from_dbwrite_id, self.to_dbwrite_id
        if not _is_dbwrite_id(first):
            raise ValueError(
                f"from_dbwrite_id must be a positive integer, got {first!r}; or pass dbwrite_ids"
            )
        if last is not None and not _is_dbwrite_id(last):
            raise ValueError(f"to_dbwrite_id must be a positive integer, got {last!r}")
        if last is not None and last < first:
            raise ValueError(f"to_dbwrite_id {last} is lower than from_dbwrite_id {first}")

    @classmethod
    def from_value(cls, value: Any) -> ReplayRequest:
        """Parse a stored request; the status fields that the trigger adds are ignored."""
        if not isinstance(value, Mapping):
            raise ValueError(f"A replay request must be a JSON object, got {value!r}")
        if unknown := set(value) - _REQUEST_FIELDS - _STATUS_FIELDS:
            raise ValueError(f"Unknown replay request fields {sorted(unknown)}")
        ids = value.get("dbwrite_ids")
        return cls(
            id=value.get("id"),
            from_dbwrite_id=value.get("from_dbwrite_id"),
            to_dbwrite_id=value.get("to_dbwrite_id"),
            dbwrite_ids=tuple(ids) if isinstance(ids, list) else ids,
        )

    def to_value(self) -> dict[str, Any]:
        """The request as stored in the asset state store."""
        value = {
            "id": self.id,
            "from_dbwrite_id": self.from_dbwrite_id,
            "to_dbwrite_id": self.to_dbwrite_id,
            "dbwrite_ids": list(self.dbwrite_ids) if self.dbwrite_ids is not None else None,
        }
        return {key: item for key, item in value.items() if item is not None}


def request_replay(
    asset_state_store: Any,
    trigger: LaminDBDbWriteEventTrigger | str,
    *,
    from_dbwrite_id: int | None = None,
    to_dbwrite_id: int | None = None,
    dbwrite_ids: Iterable[int] | None = None,
    request_id: str | None = None,
) -> dict[str, Any]:
    """
    Ask an event trigger in an ``AssetWatcher`` to deliver the events of past database writes again.

    Call it in a task that has the watched asset as an inlet, with the task's ``asset_state_store``
    (``asset_state_store[asset]`` if the task has several assets). The request replaces the trigger's
    previous request; a replay that is still running stops.

    :param asset_state_store: The asset state store of the watched asset.
    :param trigger: The trigger, as passed to the ``AssetWatcher`` or created with the same arguments,
        or its ``state_key``.
    :param from_dbwrite_id: First write of the range (inclusive).
    :param to_dbwrite_id: Last write of the range (inclusive); by default the last write the trigger
        has processed. Later writes are delivered as live events anyway.
    :param dbwrite_ids: The writes to replay, instead of a range; at most 10,000.
    :param request_id: Identifies the request in the payloads of the replayed events, e.g. the run id
        of the requesting DAG run.
    :return: The stored request.
    """
    request = ReplayRequest(
        id=request_id,
        from_dbwrite_id=from_dbwrite_id,
        to_dbwrite_id=to_dbwrite_id,
        dbwrite_ids=tuple(dbwrite_ids) if dbwrite_ids is not None else None,
    )
    state_key = trigger if isinstance(trigger, str) else trigger.state_key
    value = request.to_value()
    asset_state_store.set(replay_key(state_key), value)
    return value


def _request_of(value: Any) -> ReplayRequest | None:
    try:
        return ReplayRequest.from_value(value)
    except (ValueError, TypeError):
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class _Replay:
    """A request being delivered; progress is kept in memory, so a restarted trigger starts over."""

    accessor: Any
    request: ReplayRequest
    after_id: int = 0
    """Range requests: the writes up to this id were delivered."""
    to_dbwrite_id: int = 0
    """Range requests: the last write to deliver."""
    ids: list[int] = field(default_factory=list)
    """List requests: the writes still to deliver."""
    events: int = 0
    finished_at: float | None = None
    """``time.monotonic()`` when the last events were handed to Airflow."""

    @property
    def exhausted(self) -> bool:
        if self.request.dbwrite_ids is not None:
            return not self.ids
        return self.after_id >= self.to_dbwrite_id


class Replayer:
    """
    Delivers the replay requests of one trigger, a few batches per poll, next to its live events.

    Errors stay contained: API errors leave the request for the next poll, and other errors mark it
    ``failed``, without slowing down the trigger's live polling.
    """

    def __init__(self, trigger: LaminDBDbWriteEventTrigger) -> None:
        self.trigger = trigger
        self.key = replay_key(trigger.state_key)
        self._accessors = _state_accessors(trigger.asset_state_store)
        self._replay: _Replay | None = None

    async def poll(self, hook: LaminDBHook, instance: LaminDBInstance, cursor: int) -> list[TriggerEvent]:
        """The replayed events for this poll; ``cursor`` is the trigger's live cursor."""
        if not self._accessors:
            return []
        try:
            replay = await self._current(cursor)
            if replay is None or replay.finished_at is not None:
                return []
            return await self._deliver(replay, hook, instance)
        except LaminDBApiError as err:
            self.trigger.log.warning("Replaying database writes failed, retrying at the next poll: %s", err)
        except Exception as err:
            self.trigger.log.exception("Replaying database writes failed")
            if (replay := self._replay) is not None:
                self._replay = None
                with contextlib.suppress(Exception):
                    await self._end(replay, "failed", error=f"{type(err).__name__}: {err}")
        return []

    async def _current(self, cursor: int) -> _Replay | None:
        """The request to work on: the current one unless it was withdrawn or is done, else a new one."""
        if (replay := self._replay) is not None:
            if _request_of(await replay.accessor.aget(self.key, None)) != replay.request:
                self.trigger.log.info("Replay request %r was withdrawn or replaced", replay.request.id)
                self._replay = None
            elif replay.finished_at is None:
                return replay
            elif time.monotonic() - replay.finished_at >= self.trigger.cursor_store.commit_delay:
                # like the cursor: only once Airflow had time to persist the last events
                self.trigger.log.info(
                    "Replayed %d events for replay request %r", replay.events, replay.request.id
                )
                await self._end(replay, "done", events=replay.events)
                return None
            else:
                return replay
        for accessor in self._accessors:
            value = await accessor.aget(self.key, None)
            if value is None or (isinstance(value, Mapping) and value.get("status") in FINAL_STATUSES):
                continue
            try:
                request = ReplayRequest.from_value(value)
            except (ValueError, TypeError) as err:
                self.trigger.log.error("Invalid replay request %r: %s", value, err)
                stored = dict(value) if isinstance(value, Mapping) else {"request": value}
                await accessor.aset(self.key, {**stored, "status": "invalid", "error": str(err)})
                continue
            self._replay = self._start(accessor, request, cursor)
            await accessor.aset(self.key, {**request.to_value(), "status": "running", "started_at": _now()})
            return self._replay
        return None

    def _start(self, accessor: Any, request: ReplayRequest, cursor: int) -> _Replay:
        # writes after the live cursor are delivered as live events anyway
        if request.dbwrite_ids is not None:
            ids = sorted({i for i in request.dbwrite_ids if i <= cursor})
            self.trigger.log.info("Replaying %d database writes for replay request %r", len(ids), request.id)
            return _Replay(accessor, request, ids=ids)
        first = request.from_dbwrite_id
        assert first is not None  # validated by ReplayRequest
        last = cursor if request.to_dbwrite_id is None else min(request.to_dbwrite_id, cursor)
        self.trigger.log.info(
            "Replaying database writes %d to %d for replay request %r", first, last, request.id
        )
        return _Replay(accessor, request, after_id=first - 1, to_dbwrite_id=last)

    async def _deliver(
        self, replay: _Replay, hook: LaminDBHook, instance: LaminDBInstance
    ) -> list[TriggerEvent]:
        trigger = self.trigger
        events: list[TriggerEvent] = []
        for _ in range(trigger.max_batches_per_poll):
            if replay.exhausted:
                break
            chunk: list[int] = []
            if replay.request.dbwrite_ids is not None:
                chunk = replay.ids[: trigger.batch_size]
                selection: dict[str, Any] = {"id": {"in": chunk}}
                after_id = None
            else:
                selection = {"id": {"lte": replay.to_dbwrite_id}}
                after_id = replay.after_id
            writes = await hook.aquery_dbwrites(
                combine_filters(trigger._dbwrite_filter(), selection),
                after_id=after_id,
                limit=trigger.batch_size,
            )
            batch = await trigger._events_for(hook, instance, writes) if writes else []
            # progress only moves once the events of the batch exist
            if chunk:
                replay.ids = replay.ids[len(chunk) :]
            elif len(writes) == trigger.batch_size:
                replay.after_id = max(int(write["id"]) for write in writes)
            else:
                replay.after_id = replay.to_dbwrite_id
            events += [
                TriggerEvent({**event.payload, "replay": {"id": replay.request.id}}) for event in batch
            ]
        replay.events += len(events)
        if replay.exhausted:
            replay.finished_at = time.monotonic()
        return events

    async def _end(self, replay: _Replay, status: str, **details: Any) -> None:
        """Record the outcome on the request, unless it was replaced in the meantime."""
        self._replay = None
        if _request_of(await replay.accessor.aget(self.key, None)) == replay.request:
            await replay.accessor.aset(
                self.key, {**replay.request.to_value(), "status": status, "finished_at": _now(), **details}
            )
