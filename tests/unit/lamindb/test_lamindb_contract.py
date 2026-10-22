"""Checks the copies of lamindb facts that the worker keeps, as it has no lamindb, against lamindb."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from airflow.providers.lamindb.client.branches import BRANCH_STATUS_TO_CODE
from airflow.providers.lamindb.client.enums import REGISTRY_FIELDS, LaminDBRegistry, RegistryField

lamindb_types = pytest.importorskip("lamindb.base.types")


def _models() -> dict[str, type]:
    import lamindb.models
    from django.db.models import Model

    return {
        f"core.{model._meta.model_name}": model
        for model in vars(lamindb.models).values()
        if isinstance(model, type) and issubclass(model, Model) and model._meta.app_label == "lamindb"
    }


def test_branch_status_codes_match_lamindb() -> None:
    assert BRANCH_STATUS_TO_CODE == lamindb_types.BRANCH_STATUS_TO_CODE


def test_fake_run_status_codes_match_lamindb(fake_lamindb: MagicMock) -> None:
    assert fake_lamindb.base.types.RUN_STATUS_TO_CODE == lamindb_types.RUN_STATUS_TO_CODE


@pytest.mark.parametrize(("registry", "field_enum"), REGISTRY_FIELDS.items(), ids=str)
def test_field_enums_match_lamindb_models(registry: LaminDBRegistry, field_enum: type[RegistryField]) -> None:
    model = _models()[registry.value]
    fields = {field.name: field for field in model._meta.get_fields()}
    fields.update({field.attname: field for field in model._meta.concrete_fields})
    unknown = [member.value for member in field_enum if member.value not in fields]
    assert not unknown, f"{field_enum.__name__} has fields that {model.__name__} lacks"
    for member in field_enum:
        if member.related is None:
            continue
        related_model = fields[member.value].related_model
        assert related_model is not None, f"{member!r} is no relation"
        assert f"core.{related_model._meta.model_name}" == member.related.value, member
