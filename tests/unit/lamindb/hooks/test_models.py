from __future__ import annotations

import pytest
from pydantic import ValidationError

from airflow.providers.lamindb.hooks.models import (
    InstanceSettingsResponse,
    RecordInsertBody,
    RecordOrder,
    RecordQueryBody,
    RecordQueryParams,
    RecordScope,
    RecordUpdateBody,
    TokenRequest,
    TokenResponse,
)


def test_query_serialization_preserves_null_filter_operands_and_empty_scope():
    body = RecordQueryBody(
        order_by=[RecordOrder(field="id")],
        filter={"description": {"eq": None}},
        scope=RecordScope(branch_ids=[]),
    )
    assert body.model_dump(mode="json", exclude_none=True) == {
        "order_by": [{"field": "id", "descending": False}],
        "filter": {"description": {"eq": None}},
        "scope": {"branch_ids": []},
    }


def test_query_params_allow_zero_limit_and_more_than_one_page():
    assert RecordQueryParams(limit=0).limit == 0
    assert RecordQueryParams(limit=450, offset=7).model_dump() == {
        "limit": 450,
        "offset": 7,
        "include_foreign_keys": True,
    }


@pytest.mark.parametrize(
    ("model", "payload"),
    [
        (RecordQueryParams, {"unknown": 1}),
        (RecordOrder, {"field": ""}),
        (RecordScope, {"branch_ids": ["1"]}),
        (RecordQueryBody, {"order_by": [], "select": [1]}),
        (RecordQueryBody, {"order_by": [], "search": 1}),
    ],
)
def test_request_models_reject_wrong_types_and_unknown_fields(model, payload):
    with pytest.raises(ValidationError):
        model.model_validate(payload)


def test_dynamic_write_fields_and_explicit_nulls_are_preserved():
    record = {"custom_field": {"values": [None, True, 1, 1.5, "value"]}, "description": None}
    assert RecordInsertBody.model_validate([record]).model_dump(mode="json") == [record]
    assert RecordUpdateBody.model_validate(record).model_dump(mode="json") == record
    assert RecordInsertBody.model_validate([]).model_dump(mode="json") == []
    assert RecordUpdateBody.model_validate({}).model_dump(mode="json") == {}


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_write_bodies_reject_non_json_numbers(value):
    with pytest.raises(ValidationError):
        RecordInsertBody.model_validate([{"value": value}])
    with pytest.raises(ValidationError):
        RecordUpdateBody.model_validate({"value": value})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_query_body_rejects_non_json_numbers(value):
    with pytest.raises(ValidationError):
        RecordQueryBody(order_by=[RecordOrder(field="id")], filter={"size": {"eq": value}})


def test_authentication_aliases_and_reprs():
    request = TokenRequest(api_key="private-api-key")
    response = TokenResponse.model_validate({"accessToken": "private-token", "other": "discard"})
    assert request.model_dump() == {"api_key": "private-api-key"}
    assert response.access_token == "private-token"
    assert response.model_dump(by_alias=True) == {"accessToken": "private-token"}
    assert "private-api-key" not in repr(request)
    assert "private-token" not in repr(response)


@pytest.mark.parametrize("model", [TokenRequest, TokenResponse, InstanceSettingsResponse])
def test_validation_error_strings_do_not_expose_input(model):
    with pytest.raises(ValidationError) as err:
        model.model_validate("sensitive-payload")
    assert "sensitive-payload" not in str(err.value)


def test_instance_settings_drop_unknown_credentials():
    settings = InstanceSettingsResponse.model_validate(
        {"id": "instance-id", "db_user_password": "private-password", "future_field": 1}
    )
    assert settings.owner is None
    assert settings.name is None
    assert "private-password" not in repr(settings)
    assert "db_user_password" not in settings.model_dump()
    assert "future_field" not in settings.model_dump()
