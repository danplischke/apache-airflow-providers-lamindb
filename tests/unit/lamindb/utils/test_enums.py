from __future__ import annotations

import copy
import json
import pickle

import pytest

from airflow.providers.lamindb.utils.enums import (
    REGISTRY_FIELDS,
    ArtifactField,
    ArtifactKind,
    FilterOperator,
    LaminDBRegistry,
    RunField,
    RunStatus,
    plain_value,
)

FIELD_ENUMS = list(REGISTRY_FIELDS.values())


def test_str_enums_behave_like_their_values():
    assert LaminDBRegistry.RUN == "core.run"
    assert str(LaminDBRegistry.RUN) == "core.run"
    assert f"{ArtifactField.KEY}" == "key"
    assert format(FilterOperator.NOT_IN) == "notin"
    assert json.dumps({ArtifactField.KIND: ArtifactKind.DATASET}) == '{"kind": "dataset"}'
    assert LaminDBRegistry("core.artifact") is LaminDBRegistry.ARTIFACT
    assert ArtifactField("created_by") is ArtifactField.CREATED_BY


def test_plain_value():
    assert type(plain_value(LaminDBRegistry.RUN)) is str
    assert plain_value(LaminDBRegistry.RUN) == "core.run"
    assert plain_value(RunStatus.ERRORED) == 1
    assert type(plain_value(RunStatus.ERRORED)) is int
    assert plain_value("core.run") == "core.run"


@pytest.mark.parametrize("member", [ArtifactField.CREATED_BY, ArtifactField.KEY, LaminDBRegistry.RUN])
def test_members_survive_pickling_and_copying(member):
    assert pickle.loads(pickle.dumps(member)) is member
    assert copy.deepcopy(member) is member


def test_relations():
    assert ArtifactField.CREATED_BY.related is LaminDBRegistry.USER
    assert ArtifactField.ULABELS.related is LaminDBRegistry.ULABEL
    assert RunField.TRANSFORM.related is LaminDBRegistry.TRANSFORM
    assert ArtifactField.KEY.related is None
    assert ArtifactField.CREATED_BY_ID.related is None


def test_registry_fields_cover_the_core_registries():
    core = {registry for registry in LaminDBRegistry if registry.startswith("core.")}
    assert set(REGISTRY_FIELDS) == core
    assert all(registry.value.count(".") == 1 for registry in LaminDBRegistry)


@pytest.mark.parametrize("fields", FIELD_ENUMS, ids=lambda fields: fields.__name__)
def test_field_enums_are_consistent(fields):
    # duplicated values would silently turn members into aliases of each other
    assert len(fields.__members__) == len(fields)
    assert fields.ID == "id"
    for member in fields:
        assert member.name == member.value.upper().lstrip("_")
        assert member.related is None or member.related in REGISTRY_FIELDS
        if member.name.endswith("_ID"):
            # foreign keys belong to relations; others, like ReferenceField.PUBMED_ID, are plain fields
            relation = fields.__members__.get(member.name.removesuffix("_ID"))
            assert relation is None or relation.related is not None, member


def test_run_status_codes():
    assert [(status.name, status.value) for status in RunStatus] == [
        ("SCHEDULED", -3),
        ("RESTARTED", -2),
        ("STARTED", -1),
        ("COMPLETED", 0),
        ("ERRORED", 1),
        ("ABORTED", 2),
    ]


def test_filter_operators():
    assert [op for op in FilterOperator if op.is_count] == [
        FilterOperator.COUNT_EQ,
        FilterOperator.COUNT_NE,
        FilterOperator.COUNT_GT,
        FilterOperator.COUNT_GTE,
        FilterOperator.COUNT_LT,
        FilterOperator.COUNT_LTE,
    ]
    assert len(FilterOperator) == 18
