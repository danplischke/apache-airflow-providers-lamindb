from __future__ import annotations

import base64
import json
import time

import httpx
import pytest
import respx
from airflow.sdk import Connection
from pydantic import ValidationError

from airflow.providers.lamindb.client.client import DEFAULT_HUB_API_URL, parse_instance_slug
from airflow.providers.lamindb.client.filters import ArtifactField, Condition, F, LaminDBRegistry
from airflow.providers.lamindb.client.models import (
    BranchRecordResponse,
    InstanceSchemaResponse,
    UserRecordResponse,
)
from airflow.providers.lamindb.client.registries import DBWRITE_REGISTRY, Registry, resolve_registry
from airflow.providers.lamindb.exceptions import LaminDBApiError, LaminDBVersionError
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook

HUB = "https://hub.example.com/api"
API = "https://api.example.com/api"
INSTANCE_ID = "037ba1e0-8d80-4f91-a902-75a47735076a"
SETTINGS = {
    "id": INSTANCE_ID,
    "owner": "owner",
    "name": "name",
    "lnid": "abc",
    "api_url": API,
    "public": False,
    "db_user_password": "secret",
}
SCHEMA = {
    "core": {
        "artifact": {"table_name": "lamindb_artifact"},
        "branch": {"table_name": "lamindb_branch"},
        "branchblock": {"table_name": "lamindb_branchblock"},
        "user": {"table_name": "lamindb_user"},
        "ulabel": {"table_name": "lamindb_ulabel"},
    },
    "bionty": {"gene": {"table_name": "bionty_gene"}},
}
RECORDS_URL = f"{API}/instances/{INSTANCE_ID}/modules"


def make_jwt(exp: float) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp, "sub": "user"}).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


@pytest.fixture
def connection(monkeypatch):
    conn = Connection(
        conn_id="lamindb_test",
        conn_type="lamindb",
        host=HUB,
        password="api-key",
        extra=json.dumps({"instance": "owner/name"}),
    )
    monkeypatch.setenv("AIRFLOW_CONN_LAMINDB_TEST", conn.as_json())
    return conn


@pytest.fixture
def hook(connection):
    hook = LaminDBHook("lamindb_test", retry_backoff=0)
    yield hook
    hook.close()


@pytest.fixture
def mock_api():
    with respx.mock(assert_all_called=False) as router:
        router.post(f"{HUB}/account/jwt").mock(
            return_value=httpx.Response(200, json={"accessToken": make_jwt(time.time() + 3600)})
        )
        router.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json=SETTINGS)
        )
        router.get(f"{API}/instances/{INSTANCE_ID}/schema").mock(
            return_value=httpx.Response(200, json=SCHEMA)
        )
        yield router


def request_body(call) -> dict:
    return json.loads(call.request.content)


class TestConfiguration:
    def test_from_connection(self, hook):
        config = hook._get_config()
        assert (config.hub_api_url, config.api_key, config.owner, config.name) == (
            HUB,
            "api-key",
            "owner",
            "name",
        )
        assert "api-key" not in repr(config)

    def test_defaults_and_overrides(self):
        conn = Connection(conn_id="c", conn_type="lamindb", host="hub.example.com/api/", extra="{}")
        config = LaminDBHook._config_from_connection(conn, "other/instance")
        assert config.hub_api_url == "https://hub.example.com/api"
        assert config.api_key is None
        assert (config.owner, config.name) == ("other", "instance")

    def test_prefixed_extra(self):
        conn = Connection(
            conn_id="c",
            conn_type="lamindb",
            extra=json.dumps({"extra__lamindb__instance": "a/b", "extra__lamindb__cache_dir": "/cache"}),
        )
        config = LaminDBHook._config_from_connection(conn, None)
        assert config.hub_api_url == DEFAULT_HUB_API_URL
        assert (config.owner, config.name) == ("a", "b")
        assert config.cache_dir == "/cache"

    def test_cache_dir(self):
        conn = Connection(
            conn_id="c", conn_type="lamindb", extra=json.dumps({"instance": "a/b", "cache_dir": "/cache"})
        )
        assert LaminDBHook._config_from_connection(conn, None).cache_dir == "/cache"
        conn = Connection(conn_id="c", conn_type="lamindb", extra=json.dumps({"instance": "a/b"}))
        assert LaminDBHook._config_from_connection(conn, None).cache_dir is None

    def test_anonymous_without_connection(self):
        hook = LaminDBHook(lamindb_conn_id=None, instance="laminlabs/lamindata")
        config = hook._get_config()
        assert config.api_key is None
        assert config.hub_api_url == DEFAULT_HUB_API_URL

    def test_missing_instance(self):
        with pytest.raises(ValueError, match="No LaminDB instance configured"):
            LaminDBHook._config_from_connection(None, None)

    @pytest.mark.parametrize("slug", ["owner", "owner/", "/name", "a/b/c"])
    def test_invalid_slug(self, slug):
        with pytest.raises(ValueError, match="Invalid LaminDB instance"):
            parse_instance_slug(slug)

    async def test_async_config(self, connection):
        hook = LaminDBHook("lamindb_test")
        config = await hook._aget_config()
        assert (config.owner, config.name, config.api_key) == ("owner", "name", "api-key")


class TestRegistry:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("core.artifact", Registry("core", "artifact", "lamindb_artifact")),
            ("Artifact", Registry("core", "artifact", "lamindb_artifact")),
            ("lamindb.Artifact", Registry("core", "artifact", "lamindb_artifact")),
            ("bionty.Gene", Registry("bionty", "gene", "bionty_gene")),
            ("hubmodule.dbwrite", DBWRITE_REGISTRY),
        ],
    )
    def test_resolve(self, name, expected):
        assert resolve_registry(InstanceSchemaResponse.model_validate(SCHEMA), name) == expected

    def test_unknown(self):
        schema = InstanceSchemaResponse.model_validate(SCHEMA)
        with pytest.raises(ValueError, match=r"Unknown registry core\.nope"):
            resolve_registry(schema, "core.nope")
        with pytest.raises(ValueError, match="Unknown schema module 'nope'"):
            resolve_registry(schema, "nope.thing")

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_get_schema_returns_the_model(self, hook, mock_api, async_mode):
        try:
            schema = await hook.aget_schema() if async_mode else hook.get_schema()
            assert isinstance(schema, InstanceSchemaResponse)
            assert schema.root["bionty"]["gene"].table_name == "bionty_gene"
            assert schema == InstanceSchemaResponse.model_validate(SCHEMA)
        finally:
            await hook.aclose()

    def test_get_registry_uses_schema(self, hook, mock_api):
        assert hook.get_registry("bionty.gene").table_name == "bionty_gene"
        assert hook.get_registry("core.branch").table_name == "lamindb_branch"
        assert mock_api.calls.call_count == 3  # jwt, settings, schema (cached)


class TestAuthentication:
    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_settings_fall_back_to_requested_slug_and_hub(self, hook, mock_api, async_mode):
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json={"id": INSTANCE_ID})
        )
        try:
            instance = await hook.aget_instance() if async_mode else hook.get_instance()
            assert instance.slug == "owner/name"
            assert instance.id == INSTANCE_ID
            assert instance.api_url == HUB
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_malformed_token_response_is_sanitized(self, hook, mock_api, async_mode):
        mock_api.post(f"{HUB}/account/jwt").mock(
            return_value=httpx.Response(200, json={"accessToken": {"private": "sensitive-value"}})
        )
        try:
            with pytest.raises(LaminDBApiError, match="access token") as err:
                await hook.aget_instance() if async_mode else hook.get_instance()
            assert "sensitive-value" not in str(err.value)
            assert hook.get_client()._access_token is None
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize(
        "invalid", [{"id": ["sensitive-value"]}, {"owner": ["sensitive-value"]}, {"public": "false"}]
    )
    async def test_malformed_settings_response_is_sanitized(self, hook, mock_api, async_mode, invalid):
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json={**SETTINGS, **invalid})
        )
        try:
            with pytest.raises(LaminDBApiError, match="Unexpected settings response") as err:
                await hook.aget_instance() if async_mode else hook.get_instance()
            assert "sensitive-value" not in str(err.value)
            assert hook.get_client()._instance is None
        finally:
            await hook.aclose()

    def test_token_exchange_and_bearer_header(self, hook, mock_api):
        instance = hook.get_instance()
        assert instance.id == INSTANCE_ID
        assert instance.api_url == API
        jwt_call, settings_call = mock_api.calls
        assert json.loads(jwt_call.request.content) == {"api_key": "api-key"}
        assert "authorization" not in jwt_call.request.headers
        assert settings_call.request.headers["authorization"].startswith("Bearer header.")

    def test_sensitive_settings_are_dropped(self, hook, mock_api):
        assert "secret" not in repr(hook.get_instance())

    def test_token_is_cached_and_refreshed_when_expiring(self, hook, mock_api):
        mock_api.post(f"{HUB}/account/jwt").mock(
            side_effect=[
                httpx.Response(200, json={"accessToken": make_jwt(time.time() + 30)}),
                httpx.Response(200, json={"accessToken": make_jwt(time.time() + 3600)}),
            ]
        )
        hook.get_instance()
        hook.get_schema()
        hook.get_schema()
        # the first token expires within the refresh margin, so it is exchanged again once
        assert mock_api.routes[0].call_count == 2

    def test_reauthenticates_once_on_401(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            side_effect=[httpx.Response(401, json={"detail": "expired"}), httpx.Response(200, json=SETTINGS)]
        )
        assert hook.get_instance().id == INSTANCE_ID
        assert route.call_count == 2
        assert mock_api.routes[0].call_count == 2

    def test_anonymous_requests_have_no_authorization(self, mock_api):
        hook = LaminDBHook(lamindb_conn_id=None, instance="owner/name")
        mock_api.get(f"{DEFAULT_HUB_API_URL}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json=SETTINGS)
        )
        hook.get_instance()
        (call,) = mock_api.calls
        assert "authorization" not in call.request.headers

    def test_missing_token_in_response(self, hook, mock_api):
        mock_api.post(f"{HUB}/account/jwt").mock(return_value=httpx.Response(200, json={}))
        with pytest.raises(LaminDBApiError, match="did not return an access token"):
            hook.get_instance()


class TestRetries:
    def test_retries_retryable_status(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            side_effect=[httpx.Response(503), httpx.Response(429), httpx.Response(200, json=SETTINGS)]
        )
        assert hook.get_instance().id == INSTANCE_ID
        assert route.call_count == 3

    def test_gives_up_after_retries(self, hook, mock_api):
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(502, json={"detail": "bad gateway"})
        )
        with pytest.raises(LaminDBApiError, match="status 502: bad gateway") as err:
            hook.get_instance()
        assert err.value.http_status_code == 502
        assert err.value.detail == "bad gateway"
        assert err.value.transient

    def test_does_not_retry_client_errors(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        with pytest.raises(LaminDBApiError, match="404") as err:
            hook.get_instance()
        assert route.call_count == 1
        assert not err.value.transient

    @pytest.mark.parametrize(("status", "transient"), [(429, True), (501, True), (400, False), (403, False)])
    def test_transient_status_codes(self, hook, mock_api, status, transient):
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(return_value=httpx.Response(status))
        with pytest.raises(LaminDBApiError) as err:
            LaminDBHook("lamindb_test", retry_backoff=0, retries=0).get_instance()
        assert err.value.transient is transient

    def test_retries_transport_errors(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            side_effect=[httpx.ConnectError("boom"), httpx.Response(200, json=SETTINGS)]
        )
        assert hook.get_instance().id == INSTANCE_ID
        assert route.call_count == 2

    def test_backoff_respects_retry_after(self, hook):
        response = httpx.Response(429, headers={"Retry-After": "7"})
        assert hook.get_client()._backoff(0, response) == 7
        assert LaminDBHook(None, "owner/name", retry_backoff=2).get_client()._backoff(3) == 16


class TestQueries:
    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize(
        "schema",
        [
            [],
            {"core": []},
            {"core": {"artifact": {"table_name": 123}}},
            {"core": {"artifact": {"fields": ["invalid"]}}},
        ],
    )
    async def test_schema_envelope_is_validated(self, hook, mock_api, async_mode, schema):
        mock_api.get(f"{API}/instances/{INSTANCE_ID}/schema").mock(
            return_value=httpx.Response(200, json=schema)
        )
        try:
            with pytest.raises(LaminDBApiError, match="Unexpected schema response"):
                await hook.aget_schema() if async_mode else hook.get_schema()
            assert hook.get_client()._schema is None
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize(
        ("registry", "row"),
        [
            ("core.artifact", {"id": 1, "_aux": '{"so": 1, "custom": 2}', "extension": {"field": None}}),
            ("core.artifact", {"created_by": {"handle": "alice"}}),
            ("hubmodule.dbwrite", {"id": 1, "data": {"_aux": "null"}, "extension": "value"}),
            ("hubmodule.dbwrite", {"id": 1}),
        ],
    )
    async def test_partial_record_validation_preserves_dictionary_payloads(
        self, hook, mock_api, async_mode, registry, row
    ):
        module, model = registry.split(".")
        mock_api.post(f"{RECORDS_URL}/{module}/{model}").mock(return_value=httpx.Response(200, json=[row]))
        try:
            method = hook.aquery_records if async_mode else hook.query_records
            result = method(registry)
            records = await result if async_mode else result
            assert records == [row]
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize(
        ("row", "fields"),
        [
            ({"id": 1, "event_type": ["UPDATE"]}, "event_type"),
            ({"id": 1, "sqlrecord_id": "not-an-id"}, "sqlrecord_id"),
            ({"id": 1, "data": ["not-an-object"]}, "data"),
        ],
        ids=["event_type", "sqlrecord_id", "data"],
    )
    async def test_write_log_rows_are_validated(self, hook, mock_api, async_mode, row, fields):
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(return_value=httpx.Response(200, json=[row]))
        try:
            with pytest.raises(LaminDBApiError, match=f"invalid {fields} in write 1") as err:
                await hook.aquery_dbwrites() if async_mode else hook.query_dbwrites()
            assert "not-an" not in str(err.value)  # names the field, not its value
        finally:
            await hook.aclose()

    async def test_an_invalid_write_id_is_not_shown(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            return_value=httpx.Response(200, json=[{"id": "not-an-id"}])
        )
        try:
            with pytest.raises(LaminDBApiError, match="invalid id in a write") as err:
                hook.query_dbwrites()
            assert "not-an-id" not in str(err.value)
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize(
        ("registry", "row"),
        [
            ("core.artifact", {"id": 1, "kind": 123}),
            ("core.artifact", {"id": 1, "_aux": '{"so": "pending"}'}),
            ("core.artifact", {"id": 1, "_aux": "[1]"}),
            ("core.artifact", {"id": 1, "_aux": '{"so": 1.0}'}),
            ("core.branch", {"id": 1, "name": ["unexpected"]}),
            ("core.user", {"id": 1, "handle": ["unexpected"]}),
        ],
    )
    async def test_odd_record_values_do_not_fail_the_query(self, hook, mock_api, async_mode, registry, row):
        """Records stay dicts; one odd value must not fail every query that returns its record."""
        module, model = registry.split(".")
        mock_api.post(f"{RECORDS_URL}/{module}/{model}").mock(
            return_value=httpx.Response(200, json=[row, {"id": 2}])
        )
        try:
            method = hook.aget_records_by_ids if async_mode else hook.get_records_by_ids
            result = method(registry, [1, 2])
            assert (await result if async_mode else result) == {1: row, 2: {"id": 2}}
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize("body", [{"id": 1}, "unexpected", [1, 2]])
    async def test_responses_that_are_not_lists_of_records_fail(self, hook, mock_api, async_mode, body):
        mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=body))
        try:
            with pytest.raises(LaminDBApiError, match="expected a list of records"):
                await hook.aquery_records("core.artifact") if async_mode else hook.query_records(
                    "core.artifact"
                )
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_responses_that_are_not_json_are_transient_errors(self, hook, mock_api, async_mode):
        mock_api.post(f"{RECORDS_URL}/core/artifact").mock(
            return_value=httpx.Response(
                200, text="<html>sign in</html>", headers={"content-type": "text/html"}
            )
        )
        try:
            with pytest.raises(LaminDBApiError, match=r"isn't JSON \(status 200, text/html\)") as err:
                await hook.aquery_records("core.artifact") if async_mode else hook.query_records(
                    "core.artifact"
                )
            assert err.value.transient
            assert "sign in" not in str(err.value)
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_query_optional_fields_keep_their_wire_shape(self, hook, mock_api, async_mode):
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=[]))
        kwargs = {
            "filter": Condition("description", "eq", None),
            "branch_ids": [],
            "select": [],
            "search": "",
        }
        try:
            if async_mode:
                await hook.aquery_records("core.artifact", **kwargs)
            else:
                hook.query_records("core.artifact", **kwargs)
            assert request_body(route.calls.last) == {
                "filter": {"description": {"eq": None}},
                "order_by": [{"field": "id", "descending": False}],
                "scope": {"branch_ids": []},
            }
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("kwargs", [{"offset": -1}, {"limit": -1}, {"include_foreign_keys": "false"}])
    async def test_query_params_are_validated_before_http(self, hook, mock_api, kwargs):
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=[]))
        try:
            with pytest.raises(ValidationError):
                hook.query_records("core.artifact", **kwargs)
            with pytest.raises(ValidationError):
                await hook.aquery_records("core.artifact", **kwargs)
            assert not mock_api.calls
            assert not route.called
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_query_records_paginates(self, hook, mock_api, async_mode):
        pages = [[{"id": i} for i in range(200)], [{"id": i} for i in range(200, 400)], [{"id": 400}]]
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(
            side_effect=[httpx.Response(200, json=page) for page in pages]
        )
        method = hook.aquery_records if async_mode else hook.query_records
        try:
            result = method(
                "core.artifact",
                F(ArtifactField.SUFFIX) == ".csv",
                order_by=["-created_at"],
                limit=450,
                branch_ids=[1],
            )
            records = await result if async_mode else result
        finally:
            await hook.aclose()
        assert len(records) == 401
        params = [dict(call.request.url.params) for call in route.calls]
        assert [(p["limit"], p["offset"]) for p in params] == [("200", "0"), ("200", "200"), ("50", "400")]
        assert params[0]["include_foreign_keys"] == "true"
        assert request_body(route.calls[0]) == {
            "filter": {"suffix": {"eq": ".csv"}},
            "order_by": [{"field": "created_at", "descending": True}],
            "scope": {"branch_ids": [1]},
        }

    def test_query_records_with_filter_builder(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=[]))
        hook.query_records(
            LaminDBRegistry.ARTIFACT,
            F(ArtifactField.SUFFIX).is_in((".csv", ".tsv")) & (F(ArtifactField.SIZE) > 0),
            order_by=[ArtifactField.CREATED_AT],
        )
        assert request_body(route.calls[0]) == {
            "filter": {"and": [{"suffix": {"in": [".csv", ".tsv"]}}, {"size": {"gt": 0}}]},
            "order_by": [{"field": "created_at", "descending": False}],
        }
        records = hook.get_records_by_ids("core.artifact", [1], filter=F(ArtifactField.KIND) == "model")
        assert records == {}
        assert request_body(route.calls[1])["filter"] == {
            "and": [{"id": {"in": [1]}}, {"kind": {"eq": "model"}}]
        }

    def test_query_records_validates_filters(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=[]))
        with pytest.raises(TypeError, match=r"FilterExpression\.from_dict"):
            hook.query_records("core.artifact", {"suffix": {"eq": ".csv"}})  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="Sort entries are strings or RecordOrder"):
            hook.query_records("core.artifact", order_by=[{"field": "size"}])  # type: ignore[list-item]
        assert not route.called

    def test_query_records_default_order(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/core/branch").mock(return_value=httpx.Response(200, json=[]))
        assert hook.query_records("core.branch", select=["id", "created_by(handle)"]) == []
        assert request_body(route.calls[0]) == {
            "order_by": [{"field": "id", "descending": False}],
            "select": ["id", "created_by(handle)"],
        }

    def test_unexpected_response(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/core/branch").mock(return_value=httpx.Response(200, json={"a": 1}))
        with pytest.raises(LaminDBApiError, match="Unexpected response"):
            hook.query_records("core.branch")

    def test_get_records_by_ids_chunks(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(
            side_effect=lambda request: httpx.Response(
                200, json=[{"id": i} for i in json.loads(request.content)["filter"]["and"][0]["id"]["in"]]
            )
        )
        records = hook.get_records_by_ids(
            "core.artifact", range(250), filter=F(ArtifactField.KIND) == "dataset"
        )
        assert sorted(records) == list(range(250))
        assert route.call_count == 2
        assert request_body(route.calls[0])["filter"]["and"][1] == {"kind": {"eq": "dataset"}}

    def test_query_dbwrites(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            return_value=httpx.Response(200, json=[{"id": 11}])
        )
        assert hook.query_dbwrites(F("table_name") == "lamindb_branch", after_id=10) == [{"id": 11}]
        assert request_body(route.calls[0]) == {
            "filter": {"and": [{"id": {"gt": 10}}, {"table_name": {"eq": "lamindb_branch"}}]},
            "order_by": [{"field": "id", "descending": False}],
        }
        # the dbwrite registry doesn't need the schema
        assert not any(call.request.url.path.endswith("/schema") for call in mock_api.calls)

    def test_latest_dbwrite_id(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            side_effect=[httpx.Response(200, json=[{"id": 99}]), httpx.Response(200, json=[])]
        )
        assert hook.get_latest_dbwrite_id() == 99
        assert hook.get_latest_dbwrite_id() == 0
        assert request_body(route.calls[0])["order_by"] == [{"field": "id", "descending": True}]
        assert route.calls[0].request.url.params["limit"] == "1"

    @pytest.mark.parametrize(
        ("branch", "expected_filter"), [("main", {"name": {"eq": "main"}}), (5, {"id": {"eq": 5}})]
    )
    def test_get_branch(self, hook, mock_api, branch, expected_filter):
        route = mock_api.post(f"{RECORDS_URL}/core/branch").mock(
            return_value=httpx.Response(200, json=[{"id": 5, "name": "main"}])
        )
        record = hook.get_branch(branch)
        assert isinstance(record, BranchRecordResponse)
        assert (record.id, record.name) == (5, "main")
        assert request_body(route.calls[0])["filter"] == expected_filter

    def test_get_branch_missing(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/core/branch").mock(return_value=httpx.Response(200, json=[]))
        assert hook.get_branch("nope") is None


class TestWrites:
    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize("operation", ["insert", "update"])
    async def test_written_rows_with_odd_values_are_returned(self, hook, mock_api, async_mode, operation):
        """The write succeeded: raising would make a retried task write the record again."""
        mock_api.put(f"{RECORDS_URL}/core/artifact").mock(
            return_value=httpx.Response(200, json=[{"id": 1, "_aux": "[1]"}])
        )
        mock_api.patch(f"{RECORDS_URL}/core/artifact/u1").mock(
            return_value=httpx.Response(200, json={"id": 1, "kind": 123})
        )
        if operation == "insert":
            method = hook.ainsert_records if async_mode else hook.insert_records
            args, expected = ("core.artifact", [{"key": "a.csv"}]), [{"id": 1, "_aux": "[1]"}]
        else:
            method = hook.aupdate_record if async_mode else hook.update_record
            args, expected = ("core.artifact", "u1", {"key": "a.csv"}), {"id": 1, "kind": 123}
        try:
            result = method(*args)
            assert (await result if async_mode else result) == expected
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_write_responses_that_are_not_records_fail(self, hook, mock_api, async_mode):
        mock_api.put(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json="unexpected"))
        try:
            method = hook.ainsert_records if async_mode else hook.insert_records
            with pytest.raises(LaminDBApiError, match=r"Unexpected response when writing core\.artifact"):
                await method("core.artifact", [{"key": "a.csv"}]) if async_mode else method(
                    "core.artifact", [{"key": "a.csv"}]
                )
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    async def test_write_bodies_preserve_dynamic_fields_and_nulls(self, hook, mock_api, async_mode):
        record = {"description": None, "custom_field": {"values": [None, True, 1, 1.5, "value"]}}
        insert = mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(
            return_value=httpx.Response(200, json=[record])
        )
        update = mock_api.patch(f"{RECORDS_URL}/core/ulabel/u1").mock(return_value=httpx.Response(204))
        try:
            if async_mode:
                assert await hook.ainsert_records("core.ulabel", [record]) == [record]
                assert await hook.aupdate_record("core.ulabel", "u1", record) is None
            else:
                assert hook.insert_records("core.ulabel", [record]) == [record]
                assert hook.update_record("core.ulabel", "u1", record) is None
            assert request_body(insert.calls.last) == [record]
            assert request_body(update.calls.last) == record
        finally:
            await hook.aclose()

    @pytest.mark.parametrize("async_mode", [False, True])
    @pytest.mark.parametrize("operation", ["insert", "update"])
    async def test_write_bodies_are_validated_before_http(self, hook, mock_api, async_mode, operation):
        invalid = {"description": object()}
        mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(return_value=httpx.Response(200, json=[]))
        mock_api.patch(f"{RECORDS_URL}/core/ulabel/u1").mock(return_value=httpx.Response(204))
        if operation == "insert":
            method = hook.ainsert_records if async_mode else hook.insert_records
            args = ("core.ulabel", [invalid])
        else:
            method = hook.aupdate_record if async_mode else hook.update_record
            args = ("core.ulabel", "u1", invalid)
        try:
            with pytest.raises(ValidationError):
                await method(*args) if async_mode else method(*args)
            assert not mock_api.calls
        finally:
            await hook.aclose()

    @pytest.mark.parametrize(
        "response", [[{"id": 1, "uid": "u1", "name": "treated"}], {"id": 1, "uid": "u1", "name": "treated"}]
    )
    def test_insert_records(self, hook, mock_api, response):
        route = mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(
            return_value=httpx.Response(200, json=response)
        )
        assert hook.insert_records("core.ulabel", [{"name": "treated"}]) == [
            {"id": 1, "uid": "u1", "name": "treated"}
        ]
        assert request_body(route.calls.last) == [{"name": "treated"}]

    def test_update_record(self, hook, mock_api):
        route = mock_api.patch(f"{RECORDS_URL}/core/ulabel/u1").mock(
            return_value=httpx.Response(200, json=[{"uid": "u1", "description": "new"}])
        )
        assert hook.update_record("core.ulabel", "u1", {"description": "new"}) == {
            "uid": "u1",
            "description": "new",
        }
        assert request_body(route.calls.last) == {"description": "new"}
        mock_api.patch(f"{RECORDS_URL}/core/ulabel/u2").mock(return_value=httpx.Response(204))
        assert hook.update_record("core.ulabel", "u2", {"description": "new"}) is None

    def test_unexpected_write_response(self, hook, mock_api):
        mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(return_value=httpx.Response(200, json="created"))
        with pytest.raises(LaminDBApiError, match=r"Unexpected response when writing core\.ulabel"):
            hook.insert_records("core.ulabel", [{"name": "treated"}])

    @pytest.mark.parametrize(
        "failure", [httpx.Response(503), httpx.Response(500), httpx.ReadTimeout("slow")], ids=str
    )
    def test_inserts_are_not_retried_once_they_may_have_reached_laminhub(self, hook, mock_api, failure):
        route = mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(
            side_effect=[failure, httpx.Response(200, json=[{"id": 1}])]
        )
        with pytest.raises(LaminDBApiError):
            hook.insert_records("core.ulabel", [{"name": "treated"}])
        assert route.call_count == 1

    @pytest.mark.parametrize("failure", [httpx.Response(429), httpx.ConnectError("refused")], ids=str)
    def test_inserts_are_retried_when_laminhub_did_not_process_them(self, hook, mock_api, failure):
        route = mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(
            side_effect=[failure, httpx.Response(200, json=[{"id": 1}])]
        )
        assert hook.insert_records("core.ulabel", [{"name": "treated"}]) == [{"id": 1}]
        assert route.call_count == 2

    async def test_async_writes(self, connection, mock_api):
        hook = LaminDBHook("lamindb_test", retry_backoff=0)
        insert = mock_api.put(f"{RECORDS_URL}/core/ulabel").mock(
            side_effect=[httpx.Response(503), httpx.Response(200, json=[{"id": 1, "uid": "u1"}])]
        )
        update = mock_api.patch(f"{RECORDS_URL}/core/ulabel/u1").mock(
            side_effect=[httpx.Response(503), httpx.Response(200, json={"uid": "u1", "name": "b"})]
        )
        try:
            with pytest.raises(LaminDBApiError, match="503"):
                await hook.ainsert_records("core.ulabel", [{"name": "a"}])
            assert await hook.ainsert_records("core.ulabel", [{"name": "a"}]) == [{"id": 1, "uid": "u1"}]
            assert await hook.aupdate_record("core.ulabel", "u1", {"name": "b"}) == {"uid": "u1", "name": "b"}
            assert (insert.call_count, update.call_count) == (2, 2)  # updates are idempotent: retried
        finally:
            await hook.aclose()


class TestTestConnection:
    def test_success(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(return_value=httpx.Response(200, json=[]))
        assert hook.test_connection() == (True, f"Connected to LaminDB instance owner/name ({API}).")

    def test_failure(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            return_value=httpx.Response(403, json={"detail": "forbidden"})
        )
        ok, message = hook.test_connection()
        assert not ok
        assert "forbidden" in message


class TestLamindbVersion:
    @staticmethod
    def _settings(mock_api, lamindb_version):
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json={**SETTINGS, "lamindb_version": lamindb_version})
        )

    @pytest.mark.parametrize("lamindb_version", ["2.8.1", "2.10.4", "2.11.0", "3.0.0", None, ""])
    def test_supported_and_unknown_versions_pass(self, hook, mock_api, lamindb_version):
        self._settings(mock_api, lamindb_version)
        assert hook.get_instance().lamindb_version == lamindb_version

    @pytest.mark.parametrize("lamindb_version", ["2.8.0", "1.17.0"])
    def test_older_instances_fail(self, hook, mock_api, lamindb_version):
        self._settings(mock_api, lamindb_version)
        with pytest.raises(LaminDBVersionError, match=f"owner/name runs lamindb {lamindb_version}, but"):
            hook.get_instance()
        ok, message = hook.test_connection()
        assert not ok
        assert "needs lamindb 2.8.1 or newer" in message

    async def test_older_instances_fail_async(self, connection, mock_api):
        self._settings(mock_api, "2.8.0")
        hook = LaminDBHook("lamindb_test", retry_backoff=0)
        try:
            with pytest.raises(LaminDBVersionError, match=r"needs lamindb 2\.8\.1 or newer"):
                await hook.aget_instance()
        finally:
            await hook.aclose()


class TestAsync:
    async def test_async_queries(self, connection, mock_api):
        hook = LaminDBHook("lamindb_test", retry_backoff=0)
        route = mock_api.post(f"{RECORDS_URL}/core/branch").mock(
            side_effect=[httpx.Response(503), httpx.Response(200, json=[{"id": 1, "name": "main"}])]
        )
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            return_value=httpx.Response(200, json=[{"id": 42}])
        )
        mock_api.post(f"{RECORDS_URL}/core/user").mock(
            return_value=httpx.Response(200, json=[{"id": 7, "handle": "alice"}])
        )
        try:
            assert (await hook.aget_instance()).id == INSTANCE_ID
            assert await hook.aget_branch("main") == BranchRecordResponse(id=1, name="main")
            assert route.call_count == 2
            assert await hook.aget_latest_dbwrite_id() == 42
            assert await hook.aquery_dbwrites(after_id=1) == [{"id": 42}]
            users = await hook.aget_users([7])
            assert users == {7: UserRecordResponse(id=7, handle="alice")}
            assert hook.get_users([7]) == users
            assert (await hook.aget_registry("bionty.gene")).table_name == "bionty_gene"
            auth_headers = {call.request.headers.get("authorization") for call in mock_api.calls[1:]}
            assert len(auth_headers) == 1
            assert next(iter(auth_headers)).startswith("Bearer ")
        finally:
            await hook.aclose()
        assert hook.get_client()._async_client is None

    async def test_async_error(self, connection, mock_api):
        hook = LaminDBHook("lamindb_test", retry_backoff=0, retries=0)
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(LaminDBApiError, match="slow") as err:
            await hook.aget_instance()
        assert err.value.transient  # a network error
        await hook.aclose()


class TestTriggersWithTheClient:
    """Triggers against the real client, whose response checks the triggers' fake hooks skip."""

    async def test_an_odd_artifact_value_does_not_stop_the_artifact_trigger(self, hook, mock_api):
        from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger
        from unit.lamindb.fakes import dbwrite

        schema = {**SCHEMA, "core": {**SCHEMA["core"], "record": {"table_name": "lamindb_record"}}}
        mock_api.get(f"{API}/instances/{INSTANCE_ID}/schema").mock(
            return_value=httpx.Response(200, json=schema)
        )
        writes = [dbwrite(1, "UPDATE", "lamindb_artifact", 2, data={"key": "raw/old.csv"})]
        mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(return_value=httpx.Response(200, json=writes))
        mock_api.post(f"{RECORDS_URL}/core/user").mock(return_value=httpx.Response(200, json=[]))
        # LaminDB sets _aux itself; an unexpected value used to fail the query, and the trigger then
        # retried the same batch forever
        odd = {"id": 2, "uid": "a2", "key": "raw/a.csv", "kind": "dataset", "_aux": '{"so": "pending"}'}
        mock_api.post(f"{RECORDS_URL}/core/artifact").mock(return_value=httpx.Response(200, json=[odd]))
        trigger = LaminDBArtifactEventTrigger(events=["updated"], lamindb_conn_id="lamindb_test")
        try:
            await trigger._setup(hook)
            instance = await hook.aget_instance()
            events = await trigger._events_for(hook, instance, await hook.aquery_dbwrites())
        finally:
            await hook.aclose()

        assert [(e.payload["event"], e.payload["record_id"]) for e in events] == [("updated", 2)]
