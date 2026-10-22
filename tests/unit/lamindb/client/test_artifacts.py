from __future__ import annotations

import pytest

from airflow.providers.lamindb.client.artifacts import (
    INTERNAL_ARTIFACT_KINDS,
    artifact_filter,
    is_internal_artifact,
    storage_ongoing,
)
from airflow.providers.lamindb.client.dbwrite import is_upload_completion
from airflow.providers.lamindb.client.filters import F, FilterExpression
from airflow.providers.lamindb.client.models import ArtifactRecordResponse
from unit.lamindb.fakes import dbwrite


@pytest.mark.parametrize(
    ("aux", "expected"), [({"so": 1}, True), ('{"so": 1}', True), (None, False), ("null", False)]
)
def test_storage_ongoing(aux, expected):
    assert storage_ongoing(aux) is expected


@pytest.mark.parametrize("aux", ["not json", '{"so": "pending"}', [1], {"so": [1]}])
def test_storage_ongoing_is_false_for_malformed_aux(aux):
    assert storage_ongoing(aux) is False


def test_is_upload_completion_is_false_for_malformed_aux():
    write = dbwrite(1, "UPDATE", "lamindb_artifact", 1, data={"_aux": '{"so": "pending"}'})
    assert is_upload_completion(write) is False


def test_is_internal_artifact_is_false_for_malformed_kind():
    assert is_internal_artifact({"kind": 5}) is False


def test_internal_kinds_match_the_server_filter():
    # the local check and the server-side filter of artifact_filter must agree
    assert artifact_filter() == {
        "or": [{"kind": {"isnull": True}}, {"kind": {"notin": list(INTERNAL_ARTIFACT_KINDS)}}]
    }
    assert all(ArtifactRecordResponse(kind=kind).is_internal for kind in INTERNAL_ARTIFACT_KINDS)
    assert not ArtifactRecordResponse(kind="__lamindb_future__").is_internal


def test_is_internal_artifact():
    assert is_internal_artifact({"kind": "__lamindb_run__"})
    assert not is_internal_artifact({"kind": "dataset"})
    assert not is_internal_artifact({"kind": None})


def test_artifact_filter():
    assert artifact_filter(include_internal=True) is None
    assert isinstance(artifact_filter(), FilterExpression)
    with pytest.raises(TypeError, match=r"FilterExpression\.from_dict"):
        artifact_filter(extra={"size": {"gt": 0}})  # type: ignore[arg-type]
    assert artifact_filter() == {
        "or": [{"kind": {"isnull": True}}, {"kind": {"notin": ["__lamindb_run__", "__lamindb_config__"]}}]
    }
    assert artifact_filter(key_prefix="raw/", suffix=".csv", kind="dataset", extra=F("size") > 0) == {
        "and": [
            {"size": {"gt": 0}},
            {"key": {"startswith": "raw/"}},
            {"suffix": {"in": [".csv"]}},
            {"kind": {"in": ["dataset"]}},
        ]
    }
    assert artifact_filter(key="a.csv", include_internal=True) == {"key": {"eq": "a.csv"}}


def test_artifact_filter_validates_constructed_conditions():
    with pytest.raises((TypeError, ValueError), match="startswith"):
        artifact_filter(key_prefix=123)
