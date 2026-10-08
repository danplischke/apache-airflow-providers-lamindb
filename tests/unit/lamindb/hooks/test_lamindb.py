from __future__ import annotations

import base64
import json
import time

import httpx
import pytest
import respx
from airflow.sdk import Connection

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import (
    DBWRITE_REGISTRY,
    DEFAULT_HUB_API_URL,
    LaminDBHook,
    Registry,
    parse_instance_slug,
    resolve_registry,
)
from airflow.providers.lamindb.utils.filters import ArtifactField, F, LaminDBRegistry

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
        "user": {"table_name": "lamindb_user"},
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
            conn_id="c", conn_type="lamindb", extra=json.dumps({"extra__lamindb__instance": "a/b"})
        )
        config = LaminDBHook._config_from_connection(conn, None)
        assert config.hub_api_url == DEFAULT_HUB_API_URL
        assert (config.owner, config.name) == ("a", "b")

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
        assert resolve_registry(SCHEMA, name) == expected

    def test_unknown(self):
        with pytest.raises(ValueError, match=r"Unknown registry core\.nope"):
            resolve_registry(SCHEMA, "core.nope")
        with pytest.raises(ValueError, match="Unknown schema module 'nope'"):
            resolve_registry(SCHEMA, "nope.thing")

    def test_get_registry_uses_schema(self, hook, mock_api):
        assert hook.get_registry("bionty.gene").table_name == "bionty_gene"
        assert hook.get_registry("core.branch").table_name == "lamindb_branch"
        assert mock_api.calls.call_count == 3  # jwt, settings, schema (cached)


class TestAuthentication:
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

    def test_does_not_retry_client_errors(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        with pytest.raises(LaminDBApiError, match="404"):
            hook.get_instance()
        assert route.call_count == 1

    def test_retries_transport_errors(self, hook, mock_api):
        route = mock_api.get(f"{HUB}/instances/owner/name/settings").mock(
            side_effect=[httpx.ConnectError("boom"), httpx.Response(200, json=SETTINGS)]
        )
        assert hook.get_instance().id == INSTANCE_ID
        assert route.call_count == 2

    def test_backoff_respects_retry_after(self, hook):
        response = httpx.Response(429, headers={"Retry-After": "7"})
        assert hook._backoff(0, response) == 7
        assert LaminDBHook(retry_backoff=2)._backoff(3) == 16


class TestQueries:
    def test_query_records_paginates(self, hook, mock_api):
        pages = [[{"id": i} for i in range(200)], [{"id": i} for i in range(200, 400)], [{"id": 400}]]
        route = mock_api.post(f"{RECORDS_URL}/core/artifact").mock(
            side_effect=[httpx.Response(200, json=page) for page in pages]
        )
        records = hook.query_records(
            "core.artifact", {"suffix": {"eq": ".csv"}}, order_by=["-created_at"], limit=450, branch_ids=[1]
        )
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
        with pytest.raises(ValueError, match="Unknown filter operator 'equals'"):
            hook.query_records("core.artifact", {"suffix": {"equals": ".csv"}})
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
        records = hook.get_records_by_ids("core.artifact", range(250), filter={"kind": {"eq": "dataset"}})
        assert sorted(records) == list(range(250))
        assert route.call_count == 2
        assert request_body(route.calls[0])["filter"]["and"][1] == {"kind": {"eq": "dataset"}}

    def test_query_dbwrites(self, hook, mock_api):
        route = mock_api.post(f"{RECORDS_URL}/hubmodule/dbwrite").mock(
            return_value=httpx.Response(200, json=[{"id": 11}])
        )
        assert hook.query_dbwrites({"table_name": {"eq": "lamindb_branch"}}, after_id=10) == [{"id": 11}]
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
        assert hook.get_branch(branch) == {"id": 5, "name": "main"}
        assert request_body(route.calls[0])["filter"] == expected_filter

    def test_get_branch_missing(self, hook, mock_api):
        mock_api.post(f"{RECORDS_URL}/core/branch").mock(return_value=httpx.Response(200, json=[]))
        assert hook.get_branch("nope") is None


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
            assert await hook.aget_branch("main") == {"id": 1, "name": "main"}
            assert route.call_count == 2
            assert await hook.aget_latest_dbwrite_id() == 42
            assert await hook.aquery_dbwrites(after_id=1) == [{"id": 42}]
            assert await hook.aget_users([7]) == {7: {"id": 7, "handle": "alice"}}
            assert (await hook.aget_registry("bionty.gene")).table_name == "bionty_gene"
            auth_headers = {call.request.headers.get("authorization") for call in mock_api.calls[1:]}
            assert len(auth_headers) == 1
            assert next(iter(auth_headers)).startswith("Bearer ")
        finally:
            await hook.aclose()
        assert hook._async_client is None

    async def test_async_error(self, connection, mock_api):
        hook = LaminDBHook("lamindb_test", retry_backoff=0, retries=0)
        mock_api.get(f"{HUB}/instances/owner/name/settings").mock(side_effect=httpx.ReadTimeout("slow"))
        with pytest.raises(LaminDBApiError, match="slow"):
            await hook.aget_instance()
        await hook.aclose()
