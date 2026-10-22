from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.client.artifacts import artifact_filter, is_internal_artifact, storage_ongoing
from airflow.providers.lamindb.client.branches import SPECIAL_BRANCH_IDS
from airflow.providers.lamindb.client.client import MAX_PAGE_SIZE, LaminDBInstance, chunk_ids
from airflow.providers.lamindb.client.dbwrite import (
    RECORD_EVENTS,
    RecordEvent,
    changed_fields,
    classify_record_write,
    is_upload_completion,
    previous_values,
)
from airflow.providers.lamindb.client.enums import normalize_str_list, plain_value
from airflow.providers.lamindb.client.filters import (
    And,
    BranchField,
    F,
    FilterExpression,
    Or,
    RecordField,
    combine_filters,
    is_local_filter,
    matches_filter,
    normalize_filter,
)
from airflow.providers.lamindb.client.models import ArtifactRecordResponse
from airflow.providers.lamindb.client.registries import Registry, normalize_registry_name
from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger
from airflow.providers.lamindb.triggers.cursors import RecordCursorStore, is_cursor_record, resolve_branch_id
from airflow.providers.lamindb.triggers.events import RecordSensorSuccess, SensorError
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.providers.lamindb.client.enums import LaminDBRegistry


_Candidate = tuple[dict[str, Any], RecordEvent]


def _normalize_events(events: str | Iterable[str]) -> list[RecordEvent]:
    values = [events] if isinstance(events, str) else list(events)
    invalid = [event for event in values if event not in RECORD_EVENTS]
    if invalid or not values:
        raise ValueError(f"Invalid events {values}; expected a non-empty subset of {list(RECORD_EVENTS)}")
    return [event for event in RECORD_EVENTS if event in values]


def _restore_filter(filter: FilterExpression | Mapping[str, Any] | None) -> FilterExpression | None:
    # Airflow can't serialize filter objects, so serialize() passes the JSON form back to __init__
    if isinstance(filter, Mapping):
        return FilterExpression.from_dict(filter)
    normalize_filter(filter)
    return filter


def branch_scope_filter(branch: str | int | None) -> FilterExpression | None:
    """Filter restricting a record query to a branch given by name or id."""
    if branch is None:
        return None
    if isinstance(branch, int):
        return F(RecordField.BRANCH_ID).eq(branch)
    if branch in SPECIAL_BRANCH_IDS:
        return F(RecordField.BRANCH_ID).eq(SPECIAL_BRANCH_IDS[branch])
    return F(RecordField.BRANCH, BranchField.NAME).eq(branch)


def ready_records(records: Iterable[dict[str, Any]], *, wait_for_upload: bool) -> list[dict[str, Any]]:
    """Drop artifacts whose upload is still in progress (if ``wait_for_upload``)."""
    if not wait_for_upload:
        return list(records)
    return [
        record for record in records if not storage_ongoing(ArtifactRecordResponse.model_validate(record).aux)
    ]


class LaminDBRecordEventTrigger(LaminDBDbWriteEventTrigger):
    """
    Fire an event for every record of a LaminDB registry that is created, updated or deleted.

    Use it in an [`AssetWatcher`][airflow.sdk.AssetWatcher] to schedule DAGs on LaminDB changes. Each event
    payload contains the ``event`` (``created``/``updated``/``deleted``), the current ``record``
    (``None`` for hard deletes), the ``previous`` values logged with the write, the ``changed_fields``
    of updates, ``changed_by`` and the underlying ``dbwrite`` log entry.

    With ``branch`` set (``main`` by default), events are relative to that branch: ``created`` also
    covers records that arrive on the branch by merging a contribution branch or by restoring them
    from the trash or archive, and ``deleted`` also covers records moved off the branch, for example
    to the trash via ``record.delete()``. For registries without branches, ``branch`` is ignored.

    :param registry: Registry to watch, e.g. ``LaminDBRegistry.COLLECTION``, ``"core.run"``,
        ``"core.record"`` or ``"bionty.celltype"``.
    :param events: Events to report, any of ``created``, ``updated`` and ``deleted``.
    :param branch: Name or id of the branch to watch; ``None`` reports raw inserts, updates and
        deletes on all branches.
    :param filter: Filter the record must match, built with
        [`F`][airflow.providers.lamindb.client.filters.F], e.g.
        ``F(ArtifactField.KEY).startswith("raw/")``. It's evaluated against the current record; for
        hard deletes against the deleted row, which only supports direct fields.
    :param changed_fields: Only report updates that change at least one of these fields, e.g.
        ``[ArtifactField.KEY]``.

    Records that hold trigger cursors (see ``cursor_store``) are never reported. They are
    recognized by the ``registry`` and ``reference_type`` of this trigger's own
    [`RecordCursorStore`][airflow.providers.lamindb.triggers.cursors.RecordCursorStore] (by default
    ``core.record`` and ``airflow_trigger_cursor``), so give triggers that store cursors in the registry
    they watch the same settings.
    """

    def __init__(
        self,
        registry: LaminDBRegistry | str,
        *,
        events: str | Sequence[str] = ("created",),
        branch: str | int | None = "main",
        filter: FilterExpression | None = None,
        changed_fields: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.registry: str = plain_value(registry)
        self.events = _normalize_events(events)
        self.branch = branch
        self.filter = _restore_filter(filter)
        self.changed_fields = normalize_str_list(changed_fields) if changed_fields else None
        if "deleted" in self.events and not is_local_filter(self._record_filter()):
            raise ValueError(
                "'deleted' events only support filters on direct fields of the record "
                "(no relation or JSON paths), because deleted records can't be queried anymore."
            )
        if (
            isinstance(self.cursor_store, RecordCursorStore)
            and self.cursor_store.reference_type is None
            and normalize_registry_name(self.cursor_store.registry) == normalize_registry_name(self.registry)
        ):
            raise ValueError(
                "This trigger stores its cursor in the registry it watches, so it needs a "
                "reference_type to recognize its own saves instead of reacting to them."
            )
        self._registry: Registry | None = None
        self._branch_id: int | None = None
        # how cursor records look: this trigger's own record cursor store, or the defaults
        self._cursor_settings = (
            self.cursor_store if isinstance(self.cursor_store, RecordCursorStore) else RecordCursorStore()
        )
        self._cursor_record_ids: set[int] = set()
        self._watches_cursor_records = False

    def _trigger_kwargs(self) -> dict[str, Any]:
        return {
            "registry": self.registry,
            "events": list(self.events),
            "branch": self.branch,
            "filter": normalize_filter(self.filter),
            "changed_fields": self.changed_fields,
        }

    def _record_filter(self) -> FilterExpression | None:
        return self.filter

    @property
    def _resolved_registry(self) -> Registry:
        if self._registry is None:
            raise RuntimeError("The trigger has not been set up yet")
        return self._registry

    async def _setup(self, hook: LaminDBHook) -> None:
        self._registry = await hook.aget_registry(self.registry)
        schema = await hook.aget_schema()
        description = schema.root.get(self._registry.module, {}).get(self._registry.model)
        fields = description.fields if description is not None else {}
        if self.branch is None:
            self._branch_id = None
        elif "branch" not in fields:
            self.log.info("Registry %s has no branches; ignoring branch=%r", self._registry.name, self.branch)
            self._branch_id = None
        else:
            self._branch_id = await resolve_branch_id(hook, self.branch)
        cursor_registry = await hook.aget_registry(self._cursor_settings.registry)
        self._watches_cursor_records = (
            self._cursor_settings.reference_type is not None and cursor_registry.name == self._registry.name
        )

    def _created_update_conditions(self) -> list[FilterExpression]:
        """Conditions on ``UPDATE`` writes that can turn into ``created`` events."""
        if self._branch_id is None:
            return []
        return [F("data")["branch_id"].is_not_null()]

    def _dbwrite_filter(self) -> FilterExpression:
        branch_id = self._branch_id

        def on_branch(condition: FilterExpression) -> FilterExpression:
            return And(condition, F("branch_id").eq(branch_id)) if branch_id is not None else condition

        clauses: list[FilterExpression] = []
        if "created" in self.events:
            clauses.append(on_branch(F("event_type").eq("INSERT")))
            conditions = self._created_update_conditions()
            if conditions and "updated" not in self.events:
                data_condition = conditions[0] if len(conditions) == 1 else Or(*conditions)
                clauses.append(on_branch(And(F("event_type").eq("UPDATE"), data_condition)))
        if "updated" in self.events:
            clauses.append(on_branch(F("event_type").eq("UPDATE")))
        if "deleted" in self.events:
            clauses.append(on_branch(F("event_type").eq("DELETE")))
            if branch_id is not None:
                clauses.append(And(F("event_type").eq("UPDATE"), F("data")["branch_id"].eq(branch_id)))
        return And(
            F("table_name").eq(self._resolved_registry.table_name),
            clauses[0] if len(clauses) == 1 else Or(*clauses),
        )

    def _classify(self, write: dict[str, Any]) -> RecordEvent | None:
        return classify_record_write(write, self._branch_id)

    def _is_candidate(self, write: dict[str, Any], kind: RecordEvent) -> bool:
        return kind in self.events

    async def _refine(
        self,
        hook: LaminDBHook,
        candidates: list[_Candidate],
        records: dict[int, dict[str, Any]],
    ) -> list[_Candidate]:
        """Hook for subclasses to drop or reclassify candidates once the current records are known."""
        return candidates

    def _keep(self, write: dict[str, Any], kind: RecordEvent) -> bool:
        if kind not in self.events:
            return False
        if kind == "updated" and self.changed_fields:
            return bool(set(changed_fields(write)) & set(self.changed_fields))
        return True

    async def _should_commit(self, hook: LaminDBHook, writes: list[dict[str, Any]]) -> bool:
        if not self._watches_cursor_records:
            return True
        # Saving the cursor after a batch of cursor writes would itself be a cursor write, so two
        # such triggers would keep reacting to each other's saves.
        unknown = {w["sqlrecord_id"] for w in writes} - self._cursor_record_ids
        if not unknown:
            return False
        cursor_records = await hook.aget_records_by_ids(
            self._resolved_registry,
            unknown,
            filter=F(RecordField.REFERENCE_TYPE).eq(self._cursor_settings.reference_type),
        )
        self._cursor_record_ids |= cursor_records.keys()
        return bool(unknown - cursor_records.keys())

    def _is_cursor_write(self, write: dict[str, Any], record: dict[str, Any] | None) -> bool:
        if not self._watches_cursor_records:
            return False
        if write["sqlrecord_id"] in self._cursor_record_ids:
            return True
        row = record if record is not None else previous_values(write)
        return is_cursor_record(row, self._cursor_settings.reference_type)

    def _matches_deleted_row(self, row: dict[str, Any]) -> bool:
        try:
            return matches_filter(self._record_filter(), row)
        except ValueError as err:
            self.log.warning("Can't evaluate the filter on deleted record %s: %s", row.get("id"), err)
            return False

    async def _events_for(
        self, hook: LaminDBHook, instance: LaminDBInstance, writes: list[dict[str, Any]]
    ) -> list[TriggerEvent]:
        registry = self._resolved_registry
        candidates: list[_Candidate] = []
        for write in writes:
            kind = self._classify(write)
            if kind is not None and self._is_candidate(write, kind):
                candidates.append((write, kind))
        if not candidates:
            return []

        ids = {w["sqlrecord_id"] for w, kind in candidates if w["event_type"] != "DELETE"}
        records = await hook.aget_records_by_ids(registry, ids, filter=self._record_filter()) if ids else {}
        candidates = [
            (w, k) for w, k in candidates if not self._is_cursor_write(w, records.get(w["sqlrecord_id"]))
        ]
        candidates = [(w, k) for w, k in await self._refine(hook, candidates, records) if self._keep(w, k)]
        await self._load_users(hook, [w for w, _ in candidates])

        events = []
        for write, kind in candidates:
            record_id = write["sqlrecord_id"]
            if write["event_type"] == "DELETE":
                if not self._matches_deleted_row(previous_values(write)):
                    continue
                record = None
            else:
                record = records.get(record_id)
                # gone since (or not matching the filter); a record that was moved off the branch and
                # deleted since is still reported if there is no filter to check
                if record is None and not (kind == "deleted" and self._record_filter() is None):
                    continue
            events.append(
                TriggerEvent(
                    self._payload(
                        instance,
                        write,
                        event=kind,
                        registry=registry.name,
                        table_name=registry.table_name,
                        record_id=record_id,
                        record=record,
                        previous=previous_values(write) or None,
                        changed_fields=changed_fields(write),
                    )
                )
            )
        return events


class LaminDBArtifactEventTrigger(LaminDBRecordEventTrigger):
    """
    Fire an event for every LaminDB artifact that is created, updated or deleted.

    A specialization of [`LaminDBRecordEventTrigger`][] for ``core.artifact``. By default,
    the ``created`` event fires only once the artifact's upload to storage has completed
    (``wait_for_upload``), so that downstream tasks can load it right away. Artifacts that LaminDB
    creates internally (run logs, environments) are ignored unless ``include_internal`` is set.

    :param key: Only artifacts with this exact key.
    :param key_prefix: Only artifacts whose key starts with this prefix, e.g. ``"raw/"``.
    :param suffix: Only artifacts with this suffix (or any of these suffixes), e.g. ``".parquet"``.
    :param kind: Only artifacts of this kind (or any of these kinds), e.g. ``ArtifactKind.DATASET``.
    :param include_internal: Also report artifacts LaminDB creates internally.
    :param wait_for_upload: Report ``created`` only once the upload to storage has completed and
        skip updates of artifacts that are being uploaded.
    :param events: See [`LaminDBRecordEventTrigger`][].
    :param branch: See [`LaminDBRecordEventTrigger`][].
    :param filter: Additional filter, see [`LaminDBRecordEventTrigger`][].
    :param changed_fields: See [`LaminDBRecordEventTrigger`][].
    """

    def __init__(
        self,
        *,
        key: str | None = None,
        key_prefix: str | None = None,
        suffix: str | Sequence[str] | None = None,
        kind: str | Sequence[str] | None = None,
        include_internal: bool = False,
        wait_for_upload: bool = True,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("registry", None)
        self.key = key
        self.key_prefix = key_prefix
        self.suffix = normalize_str_list(suffix)
        self.kind = normalize_str_list(kind)
        self.include_internal = include_internal
        self.wait_for_upload = wait_for_upload
        super().__init__("core.artifact", **kwargs)

    def _trigger_kwargs(self) -> dict[str, Any]:
        kwargs = super()._trigger_kwargs()
        del kwargs["registry"]
        return {
            **kwargs,
            "key": self.key,
            "key_prefix": self.key_prefix,
            "suffix": self.suffix,
            "kind": self.kind,
            "include_internal": self.include_internal,
            "wait_for_upload": self.wait_for_upload,
        }

    def _record_filter(self) -> FilterExpression | None:
        return artifact_filter(
            key=self.key,
            key_prefix=self.key_prefix,
            suffix=self.suffix,
            kind=self.kind,
            include_internal=self.include_internal,
            extra=self.filter,
        )

    def _created_update_conditions(self) -> list[FilterExpression]:
        conditions = super()._created_update_conditions()
        if self.wait_for_upload:
            conditions.append(F("data")["_aux"].is_not_null())
        return conditions

    def _classify(self, write: dict[str, Any]) -> RecordEvent | None:
        kind = super()._classify(write)
        if self.wait_for_upload and kind == "updated" and is_upload_completion(write):
            # the initial upload completed; re-uploads are reclassified in _refine
            return "created"
        return kind

    def _is_candidate(self, write: dict[str, Any], kind: RecordEvent) -> bool:
        if kind == "created" and is_upload_completion(write):
            return "created" in self.events or "updated" in self.events
        return super()._is_candidate(write, kind)

    async def _refine(
        self,
        hook: LaminDBHook,
        candidates: list[_Candidate],
        records: dict[int, dict[str, Any]],
    ) -> list[_Candidate]:
        refined: list[_Candidate] = []
        metadata = {key: ArtifactRecordResponse.model_validate(row) for key, row in records.items()}
        completions = await self._upload_completions(hook, candidates) if self.wait_for_upload else {}
        for write, kind in candidates:
            record = metadata.get(write["sqlrecord_id"])
            if record is not None and not self.include_internal and is_internal_artifact(record):
                continue
            refined_kind = (
                self._refined_kind(write, kind, record, completions) if self.wait_for_upload else kind
            )
            if refined_kind is not None:
                refined.append((write, refined_kind))
        return refined

    @staticmethod
    def _refined_kind(
        write: dict[str, Any],
        kind: RecordEvent,
        record: ArtifactRecordResponse | None,
        completions: Mapping[int, list[int]],
    ) -> RecordEvent | None:
        """Apply the upload semantics of ``wait_for_upload``; ``None`` drops the write."""
        record_completions = completions.get(write["sqlrecord_id"], [])
        uploading = record is not None and storage_ongoing(record.aux)
        if write["event_type"] == "DELETE":
            # artifacts whose upload failed are deleted again; they were never reported as created
            return None if storage_ongoing(previous_values(write).get("_aux")) else kind
        if kind == "created" and write["event_type"] == "INSERT":
            # reported once the upload completes
            if uploading or any(c > write["id"] for c in record_completions):
                return None
            return kind
        if kind == "created" and is_upload_completion(write):
            # a previous completion means this is a re-upload, e.g. after artifact.replace()
            return "updated" if any(c < write["id"] for c in record_completions) else kind
        if kind == "updated" and uploading:
            return None  # reported when the upload completes
        return kind

    async def _upload_completions(
        self, hook: LaminDBHook, candidates: list[_Candidate]
    ) -> dict[int, list[int]]:
        """Return the ids of upload-completion writes per artifact id."""
        ids = {w["sqlrecord_id"] for w, kind in candidates if kind == "created"}
        completions: dict[int, list[int]] = {}
        for chunk in chunk_ids(ids):
            aux_filter = And(
                F("table_name").eq(self._resolved_registry.table_name),
                F("event_type").eq("UPDATE"),
                F("sqlrecord_id").is_in(chunk),
                F("data")["_aux"].is_not_null(),
            )
            # every _aux update of the chunk: a capped page could miss another artifact's completion
            after_id = None
            while True:
                writes = await hook.aquery_dbwrites(aux_filter, after_id=after_id, limit=MAX_PAGE_SIZE)
                for write in writes:
                    if is_upload_completion(write):
                        completions.setdefault(write["sqlrecord_id"], []).append(write["id"])
                if len(writes) < MAX_PAGE_SIZE:
                    break
                after_id = writes[-1]["id"]
        return completions


class LaminDBRecordSensorTrigger(BaseTrigger):
    """
    Wait until records matching a filter exist in a LaminDB registry.

    Used by [`LaminDBRecordSensor`][airflow.providers.lamindb.sensors.records.LaminDBRecordSensor] in
    deferrable mode.
    Yields ``{"status": "success", "records": [...]}`` or ``{"status": "error", "message": ...}``.

    :param registry: Registry to query, e.g. ``core.artifact``.
    :param filter: Filter built with [`F`][airflow.providers.lamindb.client.filters.F].
    :param branch: Only records on this branch (name or id); ``None`` for all branches.
    :param min_count: Number of matching records required.
    :param limit: Maximum number of records returned.
    :param wait_for_upload: Ignore artifacts whose upload is still in progress.
    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param poll_interval: Seconds between checks.
    """

    def __init__(
        self,
        registry: LaminDBRegistry | str,
        filter: FilterExpression | None = None,
        *,
        branch: str | int | None = "main",
        min_count: int = 1,
        limit: int = 100,
        wait_for_upload: bool = False,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        poll_interval: float = 60.0,
    ) -> None:
        super().__init__()
        self.registry: str = plain_value(registry)
        self.filter = _restore_filter(filter)
        self.branch = branch
        self.min_count = min_count
        self.limit = limit
        self.wait_for_upload = wait_for_upload
        self.lamindb_conn_id = lamindb_conn_id
        self.instance = instance
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "registry": self.registry,
                "filter": normalize_filter(self.filter),
                "branch": self.branch,
                "min_count": self.min_count,
                "limit": self.limit,
                "wait_for_upload": self.wait_for_upload,
                "lamindb_conn_id": self.lamindb_conn_id,
                "instance": self.instance,
                "poll_interval": self.poll_interval,
            },
        )

    def _get_hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self._get_hook()
        query = combine_filters(self.filter, branch_scope_filter(self.branch))
        try:
            while True:
                try:
                    records = await hook.aquery_records(
                        self.registry, query, order_by=["-id"], limit=self.limit
                    )
                except (LaminDBApiError, ValueError) as err:
                    if not (isinstance(err, LaminDBApiError) and err.transient):
                        yield TriggerEvent(SensorError(message=str(err)).model_dump())
                        return
                    self.log.warning("LaminHub is unavailable, checking again later: %s", err)
                else:
                    matching = ready_records(records, wait_for_upload=self.wait_for_upload)
                    if len(matching) >= self.min_count:
                        yield TriggerEvent(RecordSensorSuccess(records=matching).model_dump())
                        return
                    self.log.info(
                        "Found %d of %d records in %s", len(matching), self.min_count, self.registry
                    )
                await asyncio.sleep(self.poll_interval)
        finally:
            await hook.aclose()
