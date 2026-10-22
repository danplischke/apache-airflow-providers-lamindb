"""Validated wire models for the LaminHub REST API."""

from __future__ import annotations

import json
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, JsonValue, RootModel, field_validator

from airflow.providers.lamindb.client.enums import INTERNAL_ARTIFACT_KINDS


class _RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True, allow_inf_nan=False)


class RecordQueryParams(_RequestModel):
    """Query-string parameters; ``limit`` may span multiple HTTP pages."""

    limit: int = Field(default=50, ge=0)
    offset: int = Field(default=0, ge=0)
    include_foreign_keys: bool = True


class RecordOrder(_RequestModel):
    """One normalized sort entry."""

    field: str = Field(min_length=1)
    descending: bool = False


class RecordScope(_RequestModel):
    """An explicit branch scope, including an explicitly empty list."""

    branch_ids: list[int]


class RecordQueryBody(_RequestModel):
    """Record query body after filter and sort normalization."""

    order_by: list[RecordOrder]
    filter: dict[str, JsonValue] | None = None
    select: list[str] | None = None
    search: str | None = None
    scope: RecordScope | None = None


class RecordInsertBody(RootModel[list[dict[str, JsonValue]]]):
    """Insert rows with registry-specific JSON fields."""

    model_config = ConfigDict(strict=True, hide_input_in_errors=True, allow_inf_nan=False)


class RecordUpdateBody(RootModel[dict[str, JsonValue]]):
    """Partial update; explicit nulls clear fields and must be retained."""

    model_config = ConfigDict(strict=True, hide_input_in_errors=True, allow_inf_nan=False)


class TokenRequest(_RequestModel):
    """API-key exchange body; the key is never included in the model repr."""

    api_key: str = Field(min_length=1, repr=False)


class _ResponseModel(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True, hide_input_in_errors=True)


class TokenResponse(_ResponseModel):
    """Only the access token is retained from the exchange response."""

    access_token: str = Field(alias="accessToken", min_length=1, repr=False)


class InstanceSettingsResponse(_ResponseModel):
    """Non-sensitive instance settings; credentials and unknown fields are discarded."""

    id: Annotated[str, Field(min_length=1)] | Annotated[int, Field(gt=0)]
    owner: str | None = None
    name: str | None = None
    lnid: str | None = None
    api_url: str | None = None
    schema_str: str | None = None
    public: bool | None = None
    lamindb_version: str | None = None


class _PartialRecordResponse(_ResponseModel):
    model_config = ConfigDict(extra="allow")


class ArtifactAux(_PartialRecordResponse):
    """Known artifact upload metadata, retaining other auxiliary JSON fields."""

    so: int | bool | None = None

    @property
    def storage_ongoing(self) -> bool:
        return self.so == 1


class ArtifactRecordResponse(_PartialRecordResponse):
    """Known artifact fields; projections may omit them and other columns are retained."""

    id: int | None = None
    uid: str | None = None
    key: str | None = None
    suffix: str | None = None
    kind: str | None = None
    branch_id: int | None = None
    aux: ArtifactAux | None = Field(default=None, alias="_aux")

    @field_validator("aux", mode="before")
    @classmethod
    def _decode_aux(cls, value: Any) -> Any:
        if isinstance(value, str):
            return json.loads(value)
        return value

    @property
    def is_internal(self) -> bool:
        return self.kind in INTERNAL_ARTIFACT_KINDS


class DbWriteResponse(_PartialRecordResponse):
    """Known database-write fields, with dynamic previous record values in ``data``."""

    id: int | None = None
    uid: str | None = None
    event_type: str | None = None
    table_name: str | None = None
    sqlrecord_id: int | None = None
    created_at: str | None = None
    created_by_id: int | None = None
    branch_id: int | None = None
    space_id: int | None = None
    run_id: int | None = None
    data: dict[str, JsonValue] | None = None


class BranchRecordResponse(_PartialRecordResponse):
    """Known branch fields, including its numeric workflow status."""

    id: int | None = None
    uid: str | None = None
    name: str | None = None
    description: str | None = None
    status_code: int | None = Field(default=None, alias="_status_code")
    created_by_id: int | None = None
    created_at: str | None = None


class BranchBlockRecordResponse(_PartialRecordResponse):
    """Known comment/readme fields; content and extension fields are retained."""

    id: int | None = None
    uid: str | None = None
    branch_id: int | None = None
    kind: str | None = None
    content: str | None = None


class UserRecordResponse(_PartialRecordResponse):
    """Known user identity fields used in event summaries."""

    id: int | None = None
    uid: str | None = None
    handle: str | None = None
    name: str | None = None


class RegistrySchemaResponse(_PartialRecordResponse):
    """Known registry schema envelope; field descriptions are extensible JSON metadata."""

    table_name: str | None = None
    fields: dict[str, JsonValue] = Field(default_factory=dict)


class InstanceSchemaResponse(RootModel[dict[str, dict[str, RegistrySchemaResponse]]]):
    """Schema modules mapped to their model descriptions."""

    model_config = ConfigDict(strict=True, hide_input_in_errors=True)
