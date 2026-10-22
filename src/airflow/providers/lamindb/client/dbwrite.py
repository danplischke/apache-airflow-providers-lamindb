"""Interpretation of LaminHub database write log (``hubmodule.dbwrite``) entries."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any, Literal

from airflow.providers.lamindb.client.artifacts import storage_ongoing
from airflow.providers.lamindb.client.models import DbWriteResponse

RecordEvent = Literal["created", "updated", "deleted"]
RECORD_EVENTS: tuple[RecordEvent, ...] = ("created", "updated", "deleted")

_RAW_EVENTS: dict[str, RecordEvent] = {"INSERT": "created", "UPDATE": "updated", "DELETE": "deleted"}

_SUMMARY_FIELDS = set(DbWriteResponse.model_fields) - {"data"}


def previous_values(write: DbWriteResponse | Mapping[str, Any]) -> dict[str, Any]:
    """
    Return the previous values logged with a write.

    ``UPDATE``: the previous values of the changed fields. ``DELETE``: the full previous row.
    ``INSERT``: empty.
    """
    data = DbWriteResponse.model_validate(write).data
    return dict(data) if data is not None else {}


def changed_fields(write: DbWriteResponse | Mapping[str, Any]) -> list[str]:
    """Return the fields changed by an ``UPDATE`` write."""
    parsed = DbWriteResponse.model_validate(write)
    if parsed.event_type != "UPDATE":
        return []
    return sorted(previous_values(parsed))


def dbwrite_summary(write: DbWriteResponse | Mapping[str, Any]) -> dict[str, Any]:
    """The known fields of a write log entry except ``data``, as included in trigger event payloads."""
    return DbWriteResponse.model_validate(write).model_dump(include=_SUMMARY_FIELDS)


def classify_record_write(
    write: DbWriteResponse | Mapping[str, Any], branch_id: int | None
) -> RecordEvent | None:
    """
    Classify a write to a record into a semantic record event.

    Without ``branch_id``, ``INSERT``/``UPDATE``/``DELETE`` map to ``created``/``updated``/``deleted``.

    With ``branch_id``, events are relative to that branch: a record is ``created`` when it is inserted
    on the branch or moves onto it (merge of a contribution branch, restore from trash or archive),
    ``updated`` when it changes on the branch, and ``deleted`` when it is deleted from the branch or
    moves off it (``.delete()`` moves records to the trash branch).
    """
    parsed = DbWriteResponse.model_validate(write)
    event_type = parsed.event_type
    if branch_id is None:
        return _RAW_EVENTS.get(str(event_type))
    current_branch = parsed.branch_id
    data = previous_values(parsed)
    if event_type == "INSERT":
        return "created" if current_branch == branch_id else None
    if event_type == "UPDATE":
        if "branch_id" in data and data["branch_id"] != current_branch:
            if current_branch == branch_id:
                return "created"
            if data["branch_id"] == branch_id:
                return "deleted"
            return None
        return "updated" if current_branch == branch_id else None
    if event_type == "DELETE":
        return "deleted" if data.get("branch_id", current_branch) == branch_id else None
    return None


def is_upload_completion(write: DbWriteResponse | Mapping[str, Any]) -> bool:
    """
    Whether an artifact ``UPDATE`` marks the completion of an upload to storage.

    LaminDB flags artifacts whose upload is in progress with ``_aux = {"so": 1}`` and removes the flag
    once the upload succeeded (the artifact record is deleted if the upload fails).
    """
    parsed = DbWriteResponse.model_validate(write)
    data = previous_values(parsed)
    return parsed.event_type == "UPDATE" and "_aux" in data and storage_ongoing(data["_aux"])


def resolve_status_transitions(
    writes: Iterable[Mapping[str, Any]],
    *,
    later_codes: Mapping[int, int],
    current_codes: Mapping[int, int],
    field: str = "_status_code",
) -> list[tuple[Mapping[str, Any], int, int | None]]:
    """
    Resolve ``(write, previous_code, new_code)`` for ``UPDATE`` writes that changed ``field``.

    The write log only stores the previous value, so the new value is the previous value of the next
    write that changed the field on the same record: first within ``writes``, then from
    ``later_codes`` (the previous value of the first such write after the batch), falling back to the
    record's current value in ``current_codes``. ``new_code`` is ``None`` if it can't be resolved,
    for example when the record was deleted.
    """
    status_writes = sorted(
        (w for w in writes if field in previous_values(w)),
        key=lambda w: w["id"],
    )
    next_code: dict[int, int | None] = {}
    transitions: list[tuple[Mapping[str, Any], int, int | None]] = []
    for write in reversed(status_writes):
        record_id = write["sqlrecord_id"]
        if record_id in next_code:
            new_code = next_code[record_id]
        elif record_id in later_codes:
            new_code = later_codes[record_id]
        else:
            new_code = current_codes.get(record_id)
        previous_code = previous_values(write)[field]
        transitions.append((write, previous_code, new_code))
        next_code[record_id] = previous_code
    transitions.reverse()
    return transitions


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
