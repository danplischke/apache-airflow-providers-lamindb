"""LaminDB artifacts: internal kinds, upload state and artifact filters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from airflow.providers.lamindb.client.enums import INTERNAL_ARTIFACT_KINDS, ArtifactField, normalize_str_list
from airflow.providers.lamindb.client.filters import F, FilterExpression, Or, combine_filters
from airflow.providers.lamindb.client.models import ArtifactAux, ArtifactRecordResponse

if TYPE_CHECKING:
    from pydantic import JsonValue

__all__ = ["INTERNAL_ARTIFACT_KINDS", "artifact_filter", "is_internal_artifact", "storage_ongoing"]


def storage_ongoing(aux: ArtifactAux | JsonValue) -> bool:
    """
    Whether an artifact's ``_aux`` marks its upload to storage as ongoing (``{"so": 1}``).

    ``aux`` may be JSON-encoded, as in the database write log. Malformed values are not uploads in
    progress: one unreadable write must not stop a trigger.
    """
    if isinstance(aux, ArtifactAux):
        return aux.storage_ongoing
    try:
        parsed = ArtifactRecordResponse.model_validate({"_aux": aux}).aux
    except ValidationError:
        return False
    return parsed is not None and parsed.storage_ongoing


def is_internal_artifact(record: ArtifactRecordResponse | Mapping[str, Any]) -> bool:
    """Whether an artifact is one LaminDB creates internally (run logs, environments, configs)."""
    try:
        return ArtifactRecordResponse.model_validate(record).is_internal
    except ValidationError:
        return False


def artifact_filter(
    *,
    key: str | None = None,
    key_prefix: str | None = None,
    suffix: str | Iterable[str] | None = None,
    kind: str | Iterable[str] | None = None,
    include_internal: bool = False,
    extra: FilterExpression | None = None,
) -> FilterExpression | None:
    """Build a filter for artifacts from common criteria, combined with ``extra``."""
    suffixes = normalize_str_list(suffix)
    kinds = normalize_str_list(kind)
    return combine_filters(
        extra,
        F(ArtifactField.KEY).eq(key) if key is not None else None,
        F(ArtifactField.KEY).startswith(key_prefix) if key_prefix is not None else None,
        F(ArtifactField.SUFFIX).is_in(suffixes) if suffixes is not None else None,
        F(ArtifactField.KIND).is_in(kinds) if kinds is not None else None,
        None
        if include_internal or kinds is not None
        else Or(F(ArtifactField.KIND).is_null(), F(ArtifactField.KIND).not_in(INTERNAL_ARTIFACT_KINDS)),
    )
