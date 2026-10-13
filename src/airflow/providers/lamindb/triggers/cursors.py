"""Where the event triggers keep the id of the last processed database write.

Pass a trigger ``cursor_store=AssetCursorStore()`` (the default) to keep it in the watched asset's
Airflow asset state store, or ``cursor_store=RecordCursorStore()`` to keep it in a record of the
watched instance itself.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.utils.lamindb import resolve_branch_id, resolve_space_id

if TYPE_CHECKING:
    from airflow.providers.lamindb.hooks.lamindb import LaminDBHook

log = logging.getLogger(__name__)

DEFAULT_COMMIT_DELAY = 5.0
"""Default minimum seconds between yielding events and committing the cursor past them."""
CURSOR_REFERENCE_TYPE = "airflow_trigger_cursor"
"""Default ``reference_type`` of the LaminDB records that hold cursors, and of their record type."""
CURSOR_RECORD_TYPE_NAME = "Airflow trigger cursors"
"""Default name of the record type that groups the cursor records in LaminDB."""


class CursorBackend(Protocol):
    """Loads and saves one trigger's cursor while it runs."""

    @property
    def is_persistent(self) -> bool: ...

    async def load(self, instance_id: str) -> int | None: ...

    async def save(self, cursor: int, instance_id: str) -> None: ...


@dataclass(frozen=True, kw_only=True)
class CursorStore:
    """Where and how an event trigger keeps its cursor; use a subclass.

    :param commit_delay: Minimum seconds between yielding events and committing the cursor past them,
        which gives Airflow time to persist the events. Longer delays save less often, but redeliver
        more events after a crash.
    """

    store_type: ClassVar[str]
    _types: ClassVar[dict[str, type[CursorStore]]] = {}

    commit_delay: float = DEFAULT_COMMIT_DELAY

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        CursorStore._types[cls.store_type] = cls

    def __post_init__(self) -> None:
        if self.commit_delay < 0:
            raise ValueError("commit_delay must not be negative")

    @classmethod
    def coerce(cls, value: CursorStore | Mapping[str, Any] | None) -> CursorStore:
        """Accept a store, its :meth:`serialize` form (as in serialized trigger kwargs) or None."""
        if value is None:
            return AssetCursorStore()
        if isinstance(value, CursorStore):
            return value
        if not isinstance(value, Mapping):
            raise TypeError(f"cursor_store must be a CursorStore, e.g. RecordCursorStore(), got {value!r}")
        settings = dict(value)
        store_type = CursorStore._types.get(settings.pop("type", None))
        if store_type is None:
            raise ValueError(f"cursor_store type must be one of {sorted(CursorStore._types)}, got {value!r}")
        unknown = set(settings) - {f.name for f in fields(store_type)}
        if unknown:
            raise ValueError(f"Unknown {store_type.__name__} settings: {sorted(unknown)}")
        return store_type(**settings)

    def serialize(self) -> dict[str, Any]:
        return {"type": self.store_type, **asdict(self)}

    def open(self, *, hook: LaminDBHook, asset_state_store: Any, key: str, description: str) -> CursorBackend:
        """The backend that loads and saves the cursor under ``key`` while the trigger runs."""
        raise NotImplementedError


@dataclass(frozen=True, kw_only=True)
class AssetCursorStore(CursorStore):
    """Keep the cursor in the asset state store of the :class:`~airflow.sdk.AssetWatcher`'s assets.

    The cursor lives in Airflow's metadata database next to the asset events. A trigger outside an
    ``AssetWatcher`` has no asset state store and keeps its cursor in memory only.
    """

    store_type: ClassVar[str] = "asset"

    def open(self, *, hook: LaminDBHook, asset_state_store: Any, key: str, description: str) -> CursorBackend:
        return _AssetCursor(asset_state_store, key)


@dataclass(frozen=True, kw_only=True)
class RecordCursorStore(CursorStore):
    """Keep the cursor in a record of the watched instance; needs write access to the instance.

    The defaults keep each trigger's cursor in its own ``core.record`` on ``main``, grouped under the
    record type ``Airflow trigger cursors``. The record and its type are created on the first save.
    Deleting the record resets the cursor when the trigger restarts; a running trigger creates the
    record again on its next save.

    :param registry: Registry of the cursor records. It needs the fields ``name``, ``branch``,
        ``space`` and ``field``, plus ``reference_type`` and ``type``/``is_type`` unless those are
        disabled below.
    :param field: JSON field that holds the cursor, ``{"dbwrite_id": ..., "instance_id": ...}``.
    :param record_type: Name of the record type (``is_type=True``) that groups the cursor records;
        ``None`` for no type, for registries without types.
    :param reference_type: Marker in ``reference_type`` that identifies cursor records, so that
        record triggers on ``registry`` can skip them; ``None`` for no marker. Then such triggers
        report the cursor records like any other record. A record trigger watching ``registry``
        itself needs a marker, so it rejects ``None``.
    :param branch: Branch (name or id) of the cursor records and their type.
    :param space: Space (name or id) of the cursor records and their type, e.g. a restricted space
        that only the Airflow API key can write to; ``None`` for LaminHub's default space.
    :param name: Name of the cursor record; ``None`` for the trigger's ``state_key``. A fixed name
        must be unique per trigger, or triggers overwrite each other's cursors.
    :param description: Description of the cursor record; ``None`` to describe the trigger.
    :param commit_delay: See :class:`CursorStore`.
    """

    store_type: ClassVar[str] = "record"

    registry: str = "core.record"
    field: str = "extra_data"
    record_type: str | None = CURSOR_RECORD_TYPE_NAME
    reference_type: str | None = CURSOR_REFERENCE_TYPE
    branch: str | int = "main"
    space: str | int | None = None
    name: str | None = None
    description: str | None = None

    def open(self, *, hook: LaminDBHook, asset_state_store: Any, key: str, description: str) -> CursorBackend:
        return _RecordCursor(hook, self, key, description)


def _cursor_value(cursor: int, instance_id: str) -> dict[str, Any]:
    return {"dbwrite_id": cursor, "instance_id": instance_id}


def _parse_cursor(value: Any, instance_id: str) -> int | None:
    """The cursor in a stored value, or None if it is invalid or belongs to another instance."""
    if (
        isinstance(value, Mapping)
        and value.get("instance_id") == instance_id
        and isinstance(value.get("dbwrite_id"), int)
    ):
        return value["dbwrite_id"]
    return None


def is_cursor_record(
    record: Mapping[str, Any] | None, reference_type: str | None = CURSOR_REFERENCE_TYPE
) -> bool:
    """Whether a row holds trigger cursors (or is their record type), by its ``reference_type`` marker."""
    return (
        reference_type is not None and record is not None and record.get("reference_type") == reference_type
    )


def _state_accessors(store: Any) -> list[Any]:
    """Return one accessor per watched asset from an ``AssetStateStoreAccessors`` (or a single accessor).

    Airflow has no public API that lists the accessors of several watched assets, so this reads
    ``_by_name``/``_by_uri``; ``test_cursors.py`` checks them against the installed Airflow.
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


class _AssetCursor:
    """The cursor in the asset state store of every watched asset, or in memory without one."""

    def __init__(self, store: Any, key: str) -> None:
        self.key = key
        self._accessors = _state_accessors(store)

    @property
    def is_persistent(self) -> bool:
        return bool(self._accessors)

    async def load(self, instance_id: str) -> int | None:
        cursors = [
            cursor
            for accessor in self._accessors
            if (cursor := _parse_cursor(await accessor.aget(self.key, None), instance_id)) is not None
        ]
        # with several watched assets, resume from the oldest cursor (at-least-once)
        return min(cursors) if cursors else None

    async def save(self, cursor: int, instance_id: str) -> None:
        for accessor in self._accessors:
            await accessor.aset(self.key, _cursor_value(cursor, instance_id))


class _RecordCursor:
    """The cursor in a record of the watched instance, as configured by :class:`RecordCursorStore`."""

    is_persistent = True

    def __init__(self, hook: LaminDBHook, settings: RecordCursorStore, key: str, description: str) -> None:
        self.hook = hook
        self.settings = settings
        self.name = settings.name or key
        self.description = settings.description or description
        self._uid: str | None = None
        self._scope: dict[str, int] | None = None

    async def _get_scope(self) -> dict[str, int]:
        """The branch and space ids of the cursor records, resolved once."""
        if self._scope is None:
            scope = {"branch_id": await resolve_branch_id(self.hook, self.settings.branch)}
            if self.settings.space is not None:
                scope["space_id"] = await resolve_space_id(self.hook, self.settings.space)
            self._scope = scope
        return self._scope

    def _marker(self, *, is_type: bool) -> dict[str, Any]:
        """The fields that tell cursor records (and their type) apart from other records."""
        marker: dict[str, Any] = {}
        if self.settings.reference_type is not None:
            marker["reference_type"] = self.settings.reference_type
        if self.settings.record_type is not None:
            marker["is_type"] = is_type
        return marker

    async def _find(self, *, name: str, is_type: bool) -> dict[str, Any] | None:
        conditions = {"name": name, **self._marker(is_type=is_type), **await self._get_scope()}
        rows = await self.hook.aquery_records(
            self.settings.registry,
            {"and": [{key: {"eq": value}} for key, value in conditions.items()]},
            order_by=["id"],
            limit=1,
        )
        return rows[0] if rows else None

    async def _insert(self, record: dict[str, Any], *, is_type: bool) -> dict[str, Any]:
        values = {**record, **self._marker(is_type=is_type), **await self._get_scope()}
        rows = await self.hook.ainsert_records(self.settings.registry, [values])
        if rows and rows[0].get("uid") and rows[0].get("id") is not None:
            return rows[0]
        # LaminHub returned no row: look the record up again
        found = await self._find(name=record["name"], is_type=is_type)
        if found is None:
            raise RuntimeError(f"Created the LaminDB record {record['name']!r}, but can't find it")
        return found

    async def _record_type_id(self, record_type: str) -> int:
        found = await self._find(name=record_type, is_type=True)
        if found is None:
            found = await self._insert(
                {
                    "name": record_type,
                    "description": "Cursors of Airflow triggers watching this instance's database writes.",
                },
                is_type=True,
            )
        return int(found["id"])

    def _parse(self, record: Mapping[str, Any], instance_id: str) -> int | None:
        value = record.get(self.settings.field)
        if isinstance(value, str):  # JSON fields may come back encoded
            try:
                value = json.loads(value)
            except ValueError:
                return None
        return _parse_cursor(value, instance_id)

    async def load(self, instance_id: str) -> int | None:
        record = await self._find(name=self.name, is_type=False)
        if record is None:
            return None
        self._uid = record["uid"]
        return self._parse(record, instance_id)

    async def save(self, cursor: int, instance_id: str) -> None:
        values = {self.settings.field: _cursor_value(cursor, instance_id)}
        if self._uid is None and (record := await self._find(name=self.name, is_type=False)) is not None:
            self._uid = record["uid"]
        if self._uid is not None:
            try:
                await self.hook.aupdate_record(self.settings.registry, self._uid, values)
                return
            except LaminDBApiError as err:
                if err.http_status_code != 404:
                    raise
                log.warning("The cursor record %s was deleted; creating it again", self._uid)
                self._uid = None
        record = {"name": self.name, "description": self.description, **values}
        if self.settings.record_type is not None:
            record["type_id"] = await self._record_type_id(self.settings.record_type)
        self._uid = (await self._insert(record, is_type=False))["uid"]
