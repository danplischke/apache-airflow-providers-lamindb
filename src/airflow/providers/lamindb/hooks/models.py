"""Validated wire models for the LaminHub REST API."""

from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, JsonValue, RootModel


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
