"""Validated trigger event and persistence envelopes."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from airflow.providers.lamindb.client.branches import BranchStatus  # noqa: TC001

# Pydantic resolves these field annotations at runtime.
from airflow.providers.lamindb.client.models import BranchBlockRecordResponse, DbWriteResponse  # noqa: TC001


class CursorValue(BaseModel):
    """A persisted write-log position bound to one instance."""

    model_config = ConfigDict(strict=True, extra="ignore", hide_input_in_errors=True)

    dbwrite_id: int = Field(ge=0)
    instance_id: str = Field(min_length=1)


class _TriggerModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="allow", hide_input_in_errors=True)


class BranchSummary(_TriggerModel):
    """Projected branch identity and its human-readable workflow status."""

    id: int | None = None
    uid: str | None = None
    name: str | None = None
    description: str | None = None
    status: BranchStatus | None = None
    created_by_id: int | None = None
    created_at: str | None = None


class UserSummary(_TriggerModel):
    """The actor included in write-log event payloads."""

    id: int
    handle: str | None = None
    name: str | None = None


class DbWriteEventPayload(_TriggerModel):
    """Extensible common envelope for built-in and custom write-log events."""

    instance: str
    instance_id: str
    event: str
    changed_by: UserSummary | None
    dbwrite: DbWriteResponse


class RecordEventPayload(DbWriteEventPayload):
    """A registry record was created, updated or deleted."""

    event: Literal["created", "updated", "deleted"]
    registry: str
    table_name: str
    record_id: int
    record: dict[str, Any] | None
    previous: dict[str, Any] | None
    changed_fields: list[str]


class BranchStatusEventPayload(DbWriteEventPayload):
    """A branch changed workflow status, even if its current row is gone."""

    event: Literal["status_changed"] = "status_changed"
    branch: BranchSummary | None
    from_status: BranchStatus
    to_status: BranchStatus


class BranchBlockEventPayload(DbWriteEventPayload):
    """A branch comment or readme was created."""

    event: Literal["block_created"] = "block_created"
    kind: str | None
    block: BranchBlockRecordResponse
    branch: BranchSummary | None


class SensorError(_TriggerModel):
    """A non-transient error reported by either sensor trigger."""

    status: Literal["error"] = "error"
    message: str


class RecordSensorSuccess(_TriggerModel):
    """Matching records, whose registry-specific columns remain dynamic."""

    status: Literal["success"] = "success"
    records: list[dict[str, Any]]


class BranchSensorSuccess(_TriggerModel):
    """A branch that reached a target status."""

    status: Literal["success"] = "success"
    branch: BranchSummary
    message: str | None = None


class BranchSensorFailure(_TriggerModel):
    """A terminal branch status; legacy events may omit the branch summary."""

    status: Literal["failure"] = "failure"
    branch: BranchSummary | None = None
    message: str | None = None


RecordSensorOutcome = Annotated[RecordSensorSuccess | SensorError, Field(discriminator="status")]
BranchSensorOutcome = Annotated[
    BranchSensorSuccess | BranchSensorFailure | SensorError, Field(discriminator="status")
]
RECORD_SENSOR_OUTCOME: TypeAdapter[RecordSensorOutcome] = TypeAdapter(
    RecordSensorOutcome, config=ConfigDict(hide_input_in_errors=True)
)
BRANCH_SENSOR_OUTCOME: TypeAdapter[BranchSensorOutcome] = TypeAdapter(
    BranchSensorOutcome, config=ConfigDict(hide_input_in_errors=True)
)
