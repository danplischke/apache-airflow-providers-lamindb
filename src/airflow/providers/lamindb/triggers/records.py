from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import (
    MAX_PAGE_SIZE,
    LaminDBHook,
    LaminDBInstance,
    Registry,
    chunk_ids,
)
from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger
from airflow.providers.lamindb.utils.dbwrite import (
    RECORD_EVENTS,
    RecordEvent,
    changed_fields,
    classify_record_write,
    is_upload_completion,
    previous_values,
)
from airflow.providers.lamindb.utils.enums import plain_value
from airflow.providers.lamindb.utils.filters import (
    combine_filters,
    is_local_filter,
    matches_filter,
    normalize_filter,
)
from airflow.providers.lamindb.utils.lamindb import (
    SPECIAL_BRANCH_IDS,
    artifact_filter,
    is_internal_artifact,
    normalize_str_list,
    storage_ongoing,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from collections.abc import Iterable

    from airflow.providers.lamindb.utils.filters import Filter, FilterLike

_Candidate = tuple[dict[str, Any], RecordEvent]


def _normalize_events(events: str | Iterable[str]) -> list[RecordEvent]:
    values = [events] if isinstance(events, str) else list(events)
    invalid = [event for event in values if event not in RECORD_EVENTS]
    if invalid or not values:
        raise ValueError(f"Invalid events {values}; expected a non-empty subset of {list(RECORD_EVENTS)}")
    return [event for event in RECORD_EVENTS if event in values]


async def resolve_branch_id(hook: LaminDBHook, branch: str | int) -> int:
    """Resolve a branch name (or id) to its id."""
    if isinstance(branch, int):
        return branch
    if branch in SPECIAL_BRANCH_IDS:
        return SPECIAL_BRANCH_IDS[branch]
    record = await hook.aget_branch(branch)
    if record is None:
        raise ValueError(f"Branch {branch!r} does not exist on {hook.instance_slug or 'the instance'}")
    return int(record["id"])


def branch_scope_filter(branch: str | int | None) -> dict[str, Any] | None:
    """Filter restricting a record query to a branch given by name or id."""
    if branch is None:
        return None
    if isinstance(branch, int):
        return {"branch_id": {"eq": branch}}
    if branch in SPECIAL_BRANCH_IDS:
        return {"branch_id": {"eq": SPECIAL_BRANCH_IDS[branch]}}
    return {"branch.name": {"eq": branch}}


def ready_records(records: Iterable[dict[str, Any]], *, wait_for_upload: bool) -> list[dict[str, Any]]:
    """Drop artifacts whose upload is still in progress (if ``wait_for_upload``)."""
    if not wait_for_upload:
        return list(records)
    return [record for record in records if not storage_ongoing(record.get("_aux"))]


class LaminDBRecordEventTrigger(LaminDBDbWriteEventTrigger):
    """
    Fire an event for every record of a LaminDB registry that is created, updated or deleted.

    Use it in an :class:`~airflow.sdk.AssetWatcher` to schedule DAGs on LaminDB changes. Each event
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
        :class:`~airflow.providers.lamindb.utils.filters.F`, e.g.
        ``F(ArtifactField.KEY).startswith("raw/")``, or a LaminHub REST filter such as
        ``{"key": {"startswith": "raw/"}}``. It's evaluated against the current record; for hard
        deletes against the deleted row, which only supports direct fields.
    :param changed_fields: Only report updates that change at least one of these fields, e.g.
        ``[ArtifactField.KEY]``.
    """

    def __init__(
        self,
        registry: str,
        *,
        events: str | Sequence[str] = ("created",),
        branch: str | int | None = "main",
        filter: FilterLike | None = None,
        changed_fields: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.registry: str = plain_value(registry)
        self.events = _normalize_events(events)
        self.branch = branch
        self.filter = normalize_filter(filter)
        self.changed_fields = normalize_str_list(changed_fields) if changed_fields else None
        if "deleted" in self.events and not is_local_filter(self._record_filter()):
            raise ValueError(
                "'deleted' events only support filters on direct fields of the record "
                "(no relation or JSON paths), because deleted records can't be queried anymore."
            )
        self._registry: Registry | None = None
        self._branch_id: int | None = None

    def _trigger_kwargs(self) -> dict[str, Any]:
        return {
            "registry": self.registry,
            "events": list(self.events),
            "branch": self.branch,
            "filter": self.filter,
            "changed_fields": self.changed_fields,
        }

    def _record_filter(self) -> dict[str, Any] | None:
        return self.filter

    @property
    def _resolved_registry(self) -> Registry:
        if self._registry is None:
            raise RuntimeError("The trigger has not been set up yet")
        return self._registry

    async def _setup(self, hook: LaminDBHook) -> None:
        self._registry = await hook.aget_registry(self.registry)
        schema = await hook.aget_schema()
        fields = schema.get(self._registry.module, {}).get(self._registry.model, {}).get("fields", {})
        if self.branch is None:
            self._branch_id = None
        elif "branch" not in fields:
            self.log.info("Registry %s has no branches; ignoring branch=%r", self._registry.name, self.branch)
            self._branch_id = None
        else:
            self._branch_id = await resolve_branch_id(hook, self.branch)

    def _created_update_conditions(self) -> list[dict[str, Any]]:
        """Conditions on ``UPDATE`` writes that can turn into ``created`` events."""
        if self._branch_id is None:
            return []
        return [{'data["branch_id"]': {"isnull": False}}]

    def _dbwrite_filter(self) -> Filter:
        branch_id = self._branch_id

        def on_branch(condition: dict[str, Any]) -> dict[str, Any]:
            return combine_filters(
                condition, {"branch_id": {"eq": branch_id}} if branch_id is not None else None
            ) or dict(condition)

        clauses: list[dict[str, Any]] = []
        if "created" in self.events:
            clauses.append(on_branch({"event_type": {"eq": "INSERT"}}))
            conditions = self._created_update_conditions()
            if conditions and "updated" not in self.events:
                data_condition = conditions[0] if len(conditions) == 1 else {"or": conditions}
                clauses.append(on_branch({"and": [{"event_type": {"eq": "UPDATE"}}, data_condition]}))
        if "updated" in self.events:
            clauses.append(on_branch({"event_type": {"eq": "UPDATE"}}))
        if "deleted" in self.events:
            clauses.append(on_branch({"event_type": {"eq": "DELETE"}}))
            if branch_id is not None:
                clauses.append(
                    {"and": [{"event_type": {"eq": "UPDATE"}}, {'data["branch_id"]': {"eq": branch_id}}]}
                )
        return {
            "and": [
                {"table_name": {"eq": self._resolved_registry.table_name}},
                clauses[0] if len(clauses) == 1 else {"or": clauses},
            ]
        }

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

    A specialization of :class:`LaminDBRecordEventTrigger` for ``core.artifact``. By default, the
    ``created`` event fires only once the artifact's upload to storage has completed
    (``wait_for_upload``), so that downstream tasks can load it right away. Artifacts that LaminDB
    creates internally (run logs, environments) are ignored unless ``include_internal`` is set.

    :param key: Only artifacts with this exact key.
    :param key_prefix: Only artifacts whose key starts with this prefix, e.g. ``"raw/"``.
    :param suffix: Only artifacts with this suffix (or any of these suffixes), e.g. ``".parquet"``.
    :param kind: Only artifacts of this kind (or any of these kinds), e.g. ``ArtifactKind.DATASET``.
    :param include_internal: Also report artifacts LaminDB creates internally.
    :param wait_for_upload: Report ``created`` only once the upload to storage has completed and
        skip updates of artifacts that are being uploaded.
    :param events: See :class:`LaminDBRecordEventTrigger`.
    :param branch: See :class:`LaminDBRecordEventTrigger`.
    :param filter: Additional filter, see :class:`LaminDBRecordEventTrigger`.
    :param changed_fields: See :class:`LaminDBRecordEventTrigger`.
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

    def _record_filter(self) -> dict[str, Any] | None:
        return artifact_filter(
            key=self.key,
            key_prefix=self.key_prefix,
            suffix=self.suffix,
            kind=self.kind,
            include_internal=self.include_internal,
            extra=self.filter,
        )

    def _created_update_conditions(self) -> list[dict[str, Any]]:
        conditions = super()._created_update_conditions()
        if self.wait_for_upload:
            conditions.append({'data["_aux"]': {"isnull": False}})
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
        completions = await self._upload_completions(hook, candidates) if self.wait_for_upload else {}
        for write, kind in candidates:
            record = records.get(write["sqlrecord_id"])
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
        record: dict[str, Any] | None,
        completions: Mapping[int, list[int]],
    ) -> RecordEvent | None:
        """Apply the upload semantics of ``wait_for_upload``; ``None`` drops the write."""
        record_completions = completions.get(write["sqlrecord_id"], [])
        uploading = record is not None and storage_ongoing(record.get("_aux"))
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
            aux_filter = {
                "and": [
                    {"table_name": {"eq": self._resolved_registry.table_name}},
                    {"event_type": {"eq": "UPDATE"}},
                    {"sqlrecord_id": {"in": chunk}},
                    {'data["_aux"]': {"isnull": False}},
                ]
            }
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

    Used by :class:`~airflow.providers.lamindb.sensors.records.LaminDBRecordSensor` in deferrable mode.
    Yields ``{"status": "success", "records": [...]}`` or ``{"status": "error", "message": ...}``.

    :param registry: Registry to query, e.g. ``core.artifact``.
    :param filter: Filter built with :class:`~airflow.providers.lamindb.utils.filters.F` or a
        LaminHub REST filter.
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
        registry: str,
        filter: FilterLike | None = None,
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
        self.filter = normalize_filter(filter)
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
                "filter": self.filter,
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
                    yield TriggerEvent({"status": "error", "message": str(err)})
                    return
                matching = ready_records(records, wait_for_upload=self.wait_for_upload)
                if len(matching) >= self.min_count:
                    yield TriggerEvent({"status": "success", "records": matching})
                    return
                self.log.info("Found %d of %d records in %s", len(matching), self.min_count, self.registry)
                await asyncio.sleep(self.poll_interval)
        finally:
            await hook.aclose()
