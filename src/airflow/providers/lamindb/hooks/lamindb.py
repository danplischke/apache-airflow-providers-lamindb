from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx

from airflow.providers.lamindb import __version__
from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.utils.filters import (
    FilterLike,
    combine_filters,
    normalize_filter,
    normalize_order_by,
)
from airflow.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.sdk import Connection

DEFAULT_HUB_API_URL = "https://aws.us-east-1.lamin.ai/api"
MAX_PAGE_SIZE = 200
"""Maximum number of records the LaminHub REST API returns per request."""

_RETRY_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
# a write that may have reached the server is not retried; these errors mean it did not
_WRITE_RETRY_STATUS_CODES = frozenset({429})
_WRITE_RETRY_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)
_RETRY_POLICY: dict[bool, tuple[type[Exception] | tuple[type[Exception], ...], frozenset[int]]] = {
    True: (httpx.TransportError, _RETRY_STATUS_CODES),
    False: (_WRITE_RETRY_ERRORS, _WRITE_RETRY_STATUS_CODES),
}
"""Errors and status codes retried for idempotent requests (``True``) and writes (``False``)."""
_TOKEN_REFRESH_MARGIN = 60.0
_DEFAULT_TOKEN_LIFETIME = 600.0
_MAX_RETRY_DELAY = 60.0
_USER_AGENT = f"lamindb-airflow/{__version__}"
# Only non-sensitive instance settings are kept (the settings endpoint also returns DB credentials).
_KEPT_SETTINGS = ("id", "owner", "name", "lnid", "api_url", "schema_str", "public", "lamindb_version")


@dataclass(frozen=True)
class LaminDBInstance:
    """A LaminDB instance registered on LaminHub."""

    owner: str
    name: str
    id: str
    api_url: str
    lnid: str | None = None
    schema_str: str | None = None
    public: bool | None = None
    lamindb_version: str | None = None

    @property
    def slug(self) -> str:
        return f"{self.owner}/{self.name}"


@dataclass(frozen=True)
class Registry:
    """A LaminDB registry (a Django model), addressed by schema module and model name."""

    module: str
    model: str
    table_name: str

    @property
    def name(self) -> str:
        return f"{self.module}.{self.model}"


DBWRITE_REGISTRY = Registry(module="hubmodule", model="dbwrite", table_name="hubmodule_dbwrite")
"""The LaminHub database write log ("Changes → Database writes" in the LaminHub UI)."""


@dataclass(frozen=True)
class _HubConfig:
    hub_api_url: str
    api_key: str | None = field(repr=False)
    owner: str
    name: str


def parse_instance_slug(slug: str) -> tuple[str, str]:
    """Split an ``owner/name`` instance slug."""
    owner, _, name = slug.strip().strip("/").partition("/")
    if not owner or not name or "/" in name:
        raise ValueError(f"Invalid LaminDB instance {slug!r}; expected the form 'owner/name'.")
    return owner, name


def _registry_url(instance: LaminDBInstance, registry: Registry) -> str:
    return f"{instance.api_url}/instances/{instance.id}/modules/{registry.module}/{registry.model}"


def _written_rows(data: Any, registry: Registry) -> list[dict[str, Any]]:
    """The rows a write endpoint returns: a list of rows, a single row, or nothing."""
    if data is None:
        return []
    rows = [data] if isinstance(data, dict) else data
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise LaminDBApiError(f"Unexpected response when writing {registry.name}: {data!r:.200}")
    return rows


def chunk_ids(ids: Iterable[int]) -> list[list[int]]:
    """Split ``ids`` into sorted, de-duplicated chunks that fit an ``in`` filter of one page."""
    unique = sorted(set(ids))
    return [unique[i : i + MAX_PAGE_SIZE] for i in range(0, len(unique), MAX_PAGE_SIZE)]


def normalize_registry_name(registry: str) -> tuple[str, str]:
    """``(module, model)`` of a registry name in lower case, with ``core`` as the default module."""
    module, _, model = registry.strip().rpartition(".")
    module = (module or "core").lower()
    return ("core" if module == "lamindb" else module), model.lower()


def resolve_registry(schema: Mapping[str, Any], registry: str | Registry) -> Registry:
    """
    Resolve ``module.model`` (for example ``core.artifact`` or ``bionty.Gene``) using an instance schema.

    Without a module prefix, ``core`` is assumed. Model names are case-insensitive.
    """
    if isinstance(registry, Registry):
        return registry
    module, model = normalize_registry_name(registry)
    if (module, model) == (DBWRITE_REGISTRY.module, DBWRITE_REGISTRY.model):
        return DBWRITE_REGISTRY
    models = schema.get(module)
    if models is None:
        raise ValueError(f"Unknown schema module {module!r}; available modules: {sorted(schema)}")
    info = models.get(model)
    if info is None:
        raise ValueError(f"Unknown registry {module}.{model!s}; available models: {sorted(models)}")
    return Registry(module=module, model=model, table_name=info.get("table_name") or f"{module}_{model}")


def _jwt_expiry(token: str) -> float | None:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])
    except (IndexError, KeyError, TypeError, ValueError):
        return None


def _error_detail(response: httpx.Response) -> Any:
    try:
        body = response.json()
    except ValueError:
        return response.text[:1000]
    if isinstance(body, dict) and "detail" in body:
        return body["detail"]
    return body


class LaminDBHook(BaseHook):
    """
    Interact with LaminDB instances hosted on LaminHub via the LaminHub REST API.

    The connection's password is a Lamin API key; it is exchanged for a short-lived access token that
    is refreshed automatically. Without an API key, requests are anonymous, which works for public
    instances. The instance (``owner/name``) is read from the connection's ``instance`` extra and can
    be overridden per hook.

    :param lamindb_conn_id: Airflow connection of type ``lamindb``. ``None`` means anonymous access
        with default settings, which requires ``instance`` to be passed.
    :param instance: LaminDB instance slug ``owner/name``. Overrides the connection's instance.
    :param timeout: Timeout for HTTP requests in seconds.
    :param retries: Number of retries for connection errors and retryable HTTP status codes
        (429 and 5xx).
    :param retry_backoff: Base delay in seconds for the exponential retry backoff.
    """

    conn_name_attr = "lamindb_conn_id"
    default_conn_name = "lamindb_default"
    conn_type = "lamindb"
    hook_name = "LaminDB"

    def __init__(
        self,
        lamindb_conn_id: str | None = default_conn_name,
        instance: str | None = None,
        *,
        timeout: float = 30.0,
        retries: int = 3,
        retry_backoff: float = 1.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.lamindb_conn_id = lamindb_conn_id
        self.instance_slug = instance
        self.timeout = timeout
        self.retries = retries
        self.retry_backoff = retry_backoff
        self._config: _HubConfig | None = None
        self._access_token: str | None = None
        self._access_token_expiry = 0.0
        self._instance: LaminDBInstance | None = None
        self._schema: dict[str, Any] | None = None
        self._client: httpx.Client | None = None
        self._async_client: httpx.AsyncClient | None = None

    # -- configuration ---------------------------------------------------------------------------

    @staticmethod
    def _config_from_connection(conn: Connection | None, instance: str | None) -> _HubConfig:
        extra = conn.extra_dejson if conn is not None else {}
        hub_api_url = (conn.host if conn is not None else None) or DEFAULT_HUB_API_URL
        if "://" not in hub_api_url:
            hub_api_url = f"https://{hub_api_url}"
        api_key = (conn.password if conn is not None else None) or None
        slug = instance or extra.get("instance") or extra.get("extra__lamindb__instance")
        if not slug:
            raise ValueError(
                "No LaminDB instance configured: set the 'instance' extra of the connection "
                "or pass instance='owner/name'."
            )
        owner, name = parse_instance_slug(slug)
        return _HubConfig(hub_api_url=hub_api_url.rstrip("/"), api_key=api_key, owner=owner, name=name)

    def _get_config(self) -> _HubConfig:
        if self._config is None:
            conn = self.get_connection(self.lamindb_conn_id) if self.lamindb_conn_id else None
            self._config = self._config_from_connection(conn, self.instance_slug)
        return self._config

    async def _aget_config(self) -> _HubConfig:
        if self._config is None:
            conn = await self.aget_connection(self.lamindb_conn_id) if self.lamindb_conn_id else None
            self._config = self._config_from_connection(conn, self.instance_slug)
        return self._config

    # -- HTTP plumbing -----------------------------------------------------------------------------

    def get_conn(self) -> httpx.Client:
        """Return the (lazily created) synchronous HTTP client."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout, headers={"User-Agent": _USER_AGENT})
        return self._client

    def get_async_conn(self) -> httpx.AsyncClient:
        """Return the (lazily created) asynchronous HTTP client."""
        if self._async_client is None:
            self._async_client = httpx.AsyncClient(timeout=self.timeout, headers={"User-Agent": _USER_AGENT})
        return self._async_client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def aclose(self) -> None:
        if self._async_client is not None:
            await self._async_client.aclose()
            self._async_client = None

    def _token_is_valid(self) -> bool:
        return self._access_token is not None and time.time() < (
            self._access_token_expiry - _TOKEN_REFRESH_MARGIN
        )

    def _store_token(self, payload: Any) -> str:
        token = payload.get("accessToken") if isinstance(payload, dict) else None
        if not token:
            raise LaminDBApiError("LaminHub did not return an access token for the API key.")
        self._access_token = token
        self._access_token_expiry = _jwt_expiry(token) or time.time() + _DEFAULT_TOKEN_LIFETIME
        return token

    def _backoff(self, attempt: int, response: httpx.Response | None = None) -> float:
        if response is not None and (retry_after := response.headers.get("Retry-After")):
            try:
                return min(float(retry_after), _MAX_RETRY_DELAY)
            except ValueError:
                pass
        return min(self.retry_backoff * 2**attempt, _MAX_RETRY_DELAY)

    @staticmethod
    def _parse_response(response: httpx.Response, method: str, url: str) -> Any:
        if response.is_error:
            detail = _error_detail(response)
            raise LaminDBApiError(
                f"LaminHub API request {method} {url} failed with status {response.status_code}: {detail}",
                http_status_code=response.status_code,
                detail=detail,
            )
        if not response.content:
            return None
        return response.json()

    def _send(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        authenticate: bool = True,
        idempotent: bool = True,
    ) -> Any:
        client = self.get_conn()
        retry_errors, retry_status_codes = _RETRY_POLICY[idempotent]
        attempt = 0
        reauthenticated = False
        while True:
            headers = {}
            if authenticate and (token := self._get_access_token()):
                headers["Authorization"] = f"Bearer {token}"
            try:
                response = client.request(method, url, params=params, json=json, headers=headers)
            except httpx.TransportError as err:
                if not isinstance(err, retry_errors) or attempt >= self.retries:
                    raise LaminDBApiError(f"LaminHub API request {method} {url} failed: {err}") from err
                time.sleep(self._backoff(attempt))
                attempt += 1
                continue
            if response.status_code == 401 and headers and not reauthenticated:
                self._access_token = None
                reauthenticated = True
                continue
            if response.status_code in retry_status_codes and attempt < self.retries:
                time.sleep(self._backoff(attempt, response))
                attempt += 1
                continue
            return self._parse_response(response, method, url)

    async def _asend(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
        authenticate: bool = True,
        idempotent: bool = True,
    ) -> Any:
        client = self.get_async_conn()
        retry_errors, retry_status_codes = _RETRY_POLICY[idempotent]
        attempt = 0
        reauthenticated = False
        while True:
            headers = {}
            if authenticate and (token := await self._aget_access_token()):
                headers["Authorization"] = f"Bearer {token}"
            try:
                response = await client.request(method, url, params=params, json=json, headers=headers)
            except httpx.TransportError as err:
                if not isinstance(err, retry_errors) or attempt >= self.retries:
                    raise LaminDBApiError(f"LaminHub API request {method} {url} failed: {err}") from err
                await asyncio.sleep(self._backoff(attempt))
                attempt += 1
                continue
            if response.status_code == 401 and headers and not reauthenticated:
                self._access_token = None
                reauthenticated = True
                continue
            if response.status_code in retry_status_codes and attempt < self.retries:
                await asyncio.sleep(self._backoff(attempt, response))
                attempt += 1
                continue
            return self._parse_response(response, method, url)

    def _get_access_token(self) -> str | None:
        config = self._get_config()
        if config.api_key is None:
            return None
        if not self._token_is_valid():
            payload = self._send(
                "POST",
                f"{config.hub_api_url}/account/jwt",
                json={"api_key": config.api_key},
                authenticate=False,
            )
            return self._store_token(payload)
        return self._access_token

    async def _aget_access_token(self) -> str | None:
        config = await self._aget_config()
        if config.api_key is None:
            return None
        if not self._token_is_valid():
            payload = await self._asend(
                "POST",
                f"{config.hub_api_url}/account/jwt",
                json={"api_key": config.api_key},
                authenticate=False,
            )
            return self._store_token(payload)
        return self._access_token

    # -- request builders --------------------------------------------------------------------------

    @staticmethod
    def _settings_url(config: _HubConfig) -> str:
        return f"{config.hub_api_url}/instances/{config.owner}/{config.name}/settings"

    @staticmethod
    def _instance_from_settings(config: _HubConfig, settings: Any) -> LaminDBInstance:
        if not isinstance(settings, dict) or not settings.get("id"):
            raise LaminDBApiError(f"Unexpected settings response for instance {config.owner}/{config.name}.")
        kept = {key: settings.get(key) for key in _KEPT_SETTINGS}
        return LaminDBInstance(
            owner=kept["owner"] or config.owner,
            name=kept["name"] or config.name,
            id=str(kept["id"]),
            api_url=(kept["api_url"] or config.hub_api_url).rstrip("/"),
            lnid=kept["lnid"],
            schema_str=kept["schema_str"],
            public=kept["public"],
            lamindb_version=kept["lamindb_version"],
        )

    @staticmethod
    def _records_request(
        instance: LaminDBInstance,
        registry: Registry,
        *,
        filter: FilterLike | None,
        select: Sequence[str] | None,
        order_by: Sequence[str | Mapping[str, Any]] | None,
        limit: int,
        offset: int,
        include_foreign_keys: bool,
        search: str | None,
        branch_ids: Sequence[int] | None,
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        url = _registry_url(instance, registry)
        params = {"limit": limit, "offset": offset, "include_foreign_keys": include_foreign_keys}
        body: dict[str, Any] = {"order_by": normalize_order_by(order_by)}
        if normalized := normalize_filter(filter):
            body["filter"] = normalized
        if select:
            body["select"] = list(select)
        if search:
            body["search"] = search
        if branch_ids is not None:
            body["scope"] = {"branch_ids": list(branch_ids)}
        return url, params, body

    @staticmethod
    def _check_page(page: Any, registry: Registry) -> list[dict[str, Any]]:
        if not isinstance(page, list):
            raise LaminDBApiError(f"Unexpected response when querying {registry.name}: {page!r:.200}")
        return page

    @staticmethod
    def _dbwrite_filter(filter: FilterLike | None, after_id: int | None) -> dict[str, Any] | None:
        return combine_filters({"id": {"gt": after_id}} if after_id is not None else None, filter)

    @staticmethod
    def _branch_filter(branch: str | int) -> dict[str, Any]:
        if isinstance(branch, int):
            return {"id": {"eq": branch}}
        return {"name": {"eq": branch}}

    # -- synchronous API ---------------------------------------------------------------------------

    def get_instance_slug(self) -> str:
        """Return the configured instance ``owner/name`` without contacting LaminHub."""
        config = self._get_config()
        return f"{config.owner}/{config.name}"

    def get_api_key(self) -> str | None:
        """Return the connection's Lamin API key, or ``None`` for anonymous access."""
        return self._get_config().api_key

    def get_instance(self) -> LaminDBInstance:
        """Return the configured instance (resolves its id and regional API URL on first use)."""
        if self._instance is None:
            config = self._get_config()
            settings = self._send("GET", self._settings_url(config))
            self._instance = self._instance_from_settings(config, settings)
        return self._instance

    def get_schema(self) -> dict[str, Any]:
        """Return the instance schema: ``{module: {model: {"table_name": ..., "fields": ...}}}``."""
        if self._schema is None:
            instance = self.get_instance()
            self._schema = self._send("GET", f"{instance.api_url}/instances/{instance.id}/schema")
        return self._schema

    def get_registry(self, registry: str | Registry) -> Registry:
        """Resolve a registry name such as ``core.artifact`` or ``bionty.gene``."""
        if isinstance(registry, Registry):
            return registry
        if registry.lower().startswith("hubmodule."):
            return resolve_registry({}, registry)
        return resolve_registry(self.get_schema(), registry)

    def query_records(
        self,
        registry: str | Registry,
        filter: FilterLike | None = None,
        *,
        select: Sequence[str] | None = None,
        order_by: Sequence[str | Mapping[str, Any]] | None = None,
        limit: int = 50,
        offset: int = 0,
        include_foreign_keys: bool = True,
        search: str | None = None,
        branch_ids: Sequence[int] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query records of a registry.

        :param registry: Registry name such as ``core.artifact``, ``core.run`` or ``bionty.gene``,
            or a :class:`~airflow.providers.lamindb.utils.enums.LaminDBRegistry` member.
        :param filter: Filter built with :class:`~airflow.providers.lamindb.utils.filters.F`, e.g.
            ``F(ArtifactField.SUFFIX) == ".csv"``, or a LaminHub REST filter such as
            ``{"suffix": {"eq": ".csv"}}``, see https://docs.lamin.ai/rest
        :param select: Fields (and relations such as ``created_by(handle)``) to return.
        :param order_by: Sort order, for example ``["-created_at"]``. Defaults to ascending ``id``.
        :param limit: Maximum number of records; requests are paginated in pages of 200.
        :param offset: Number of records to skip.
        :param include_foreign_keys: Include foreign key columns (``run_id``, ``branch_id``, ...).
        :param search: Full-text search string.
        :param branch_ids: Restrict the query scope to these branch ids.
        """
        reg = self.get_registry(registry)
        instance = self.get_instance()
        results: list[dict[str, Any]] = []
        while len(results) < limit:
            page_size = min(MAX_PAGE_SIZE, limit - len(results))
            url, params, body = self._records_request(
                instance,
                reg,
                filter=filter,
                select=select,
                order_by=order_by,
                limit=page_size,
                offset=offset + len(results),
                include_foreign_keys=include_foreign_keys,
                search=search,
                branch_ids=branch_ids,
            )
            page = self._check_page(self._send("POST", url, params=params, json=body), reg)
            results.extend(page)
            if len(page) < page_size:
                break
        return results

    def get_records_by_ids(
        self, registry: str | Registry, ids: Iterable[int], filter: FilterLike | None = None
    ) -> dict[int, dict[str, Any]]:
        """Fetch records by id, optionally restricted by an additional filter. Returns ``{id: record}``."""
        records: dict[int, dict[str, Any]] = {}
        for chunk in chunk_ids(ids):
            combined = combine_filters({"id": {"in": chunk}}, filter)
            for record in self.query_records(registry, combined, limit=len(chunk)):
                records[record["id"]] = record
        return records

    def query_dbwrites(
        self,
        filter: FilterLike | None = None,
        *,
        after_id: int | None = None,
        limit: int = MAX_PAGE_SIZE,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Query the LaminHub database write log, ordered by id.

        Every ``INSERT``, ``UPDATE`` and ``DELETE`` on the instance database is logged with
        ``table_name``, ``sqlrecord_id``, ``event_type``, ``branch_id`` and ``data``. For updates,
        ``data`` holds the *previous* values of the changed fields; for deletes the full previous
        row; for inserts it is ``null``.
        """
        return self.query_records(
            DBWRITE_REGISTRY,
            self._dbwrite_filter(filter, after_id),
            order_by=[{"field": "id", "descending": descending}],
            limit=limit,
        )

    def get_latest_dbwrite_id(self) -> int:
        """Return the id of the most recent database write (``0`` if there is none)."""
        rows = self.query_dbwrites(limit=1, descending=True)
        return int(rows[0]["id"]) if rows else 0

    def get_branch(self, branch: str | int) -> dict[str, Any] | None:
        """Return a branch record by name or id, or ``None`` if it doesn't exist."""
        rows = self.query_records("core.branch", self._branch_filter(branch), limit=1)
        return rows[0] if rows else None

    def get_users(self, ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        """Return ``{id: user}`` for the given user ids."""
        return self.get_records_by_ids("core.user", ids)

    def insert_records(
        self, registry: str | Registry, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Insert records into a registry and return the created rows.

        Needs write access to the instance. Fields are column names, e.g. ``{"name": "treated"}`` or
        ``{"type_id": 3}``. Inserts are not retried after errors that may have reached LaminHub, so a
        failed request never creates a record twice.
        """
        reg = self.get_registry(registry)
        url = _registry_url(self.get_instance(), reg)
        return _written_rows(self._send("PUT", url, json=list(records), idempotent=False), reg)

    def update_record(
        self, registry: str | Registry, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Update fields of the record with ``uid`` and return the updated row (if LaminHub returns it)."""
        reg = self.get_registry(registry)
        url = f"{_registry_url(self.get_instance(), reg)}/{uid}"
        rows = _written_rows(self._send("PATCH", url, json=dict(values)), reg)
        return rows[0] if rows else None

    def test_connection(self) -> tuple[bool, str]:
        """Test the connection by resolving the instance and reading its database write log."""
        try:
            instance = self.get_instance()
            self.query_dbwrites(limit=1, descending=True)
        except Exception as err:
            return False, str(err)
        finally:
            self.close()
        return True, f"Connected to LaminDB instance {instance.slug} ({instance.api_url})."

    # -- asynchronous API --------------------------------------------------------------------------

    async def aget_instance(self) -> LaminDBInstance:
        """Async version of :meth:`get_instance`."""
        if self._instance is None:
            config = await self._aget_config()
            settings = await self._asend("GET", self._settings_url(config))
            self._instance = self._instance_from_settings(config, settings)
        return self._instance

    async def aget_schema(self) -> dict[str, Any]:
        """Async version of :meth:`get_schema`."""
        if self._schema is None:
            instance = await self.aget_instance()
            self._schema = await self._asend("GET", f"{instance.api_url}/instances/{instance.id}/schema")
        return self._schema

    async def aget_registry(self, registry: str | Registry) -> Registry:
        """Async version of :meth:`get_registry`."""
        if isinstance(registry, Registry):
            return registry
        if registry.lower().startswith("hubmodule."):
            return resolve_registry({}, registry)
        return resolve_registry(await self.aget_schema(), registry)

    async def aquery_records(
        self,
        registry: str | Registry,
        filter: FilterLike | None = None,
        *,
        select: Sequence[str] | None = None,
        order_by: Sequence[str | Mapping[str, Any]] | None = None,
        limit: int = 50,
        offset: int = 0,
        include_foreign_keys: bool = True,
        search: str | None = None,
        branch_ids: Sequence[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Async version of :meth:`query_records`."""
        reg = await self.aget_registry(registry)
        instance = await self.aget_instance()
        results: list[dict[str, Any]] = []
        while len(results) < limit:
            page_size = min(MAX_PAGE_SIZE, limit - len(results))
            url, params, body = self._records_request(
                instance,
                reg,
                filter=filter,
                select=select,
                order_by=order_by,
                limit=page_size,
                offset=offset + len(results),
                include_foreign_keys=include_foreign_keys,
                search=search,
                branch_ids=branch_ids,
            )
            page = self._check_page(await self._asend("POST", url, params=params, json=body), reg)
            results.extend(page)
            if len(page) < page_size:
                break
        return results

    async def aget_records_by_ids(
        self, registry: str | Registry, ids: Iterable[int], filter: FilterLike | None = None
    ) -> dict[int, dict[str, Any]]:
        """Async version of :meth:`get_records_by_ids`."""
        records: dict[int, dict[str, Any]] = {}
        for chunk in chunk_ids(ids):
            combined = combine_filters({"id": {"in": chunk}}, filter)
            for record in await self.aquery_records(registry, combined, limit=len(chunk)):
                records[record["id"]] = record
        return records

    async def aquery_dbwrites(
        self,
        filter: FilterLike | None = None,
        *,
        after_id: int | None = None,
        limit: int = MAX_PAGE_SIZE,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Async version of :meth:`query_dbwrites`."""
        return await self.aquery_records(
            DBWRITE_REGISTRY,
            self._dbwrite_filter(filter, after_id),
            order_by=[{"field": "id", "descending": descending}],
            limit=limit,
        )

    async def aget_latest_dbwrite_id(self) -> int:
        """Async version of :meth:`get_latest_dbwrite_id`."""
        rows = await self.aquery_dbwrites(limit=1, descending=True)
        return int(rows[0]["id"]) if rows else 0

    async def aget_branch(self, branch: str | int) -> dict[str, Any] | None:
        """Async version of :meth:`get_branch`."""
        rows = await self.aquery_records("core.branch", self._branch_filter(branch), limit=1)
        return rows[0] if rows else None

    async def aget_users(self, ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        """Async version of :meth:`get_users`."""
        return await self.aget_records_by_ids("core.user", ids)

    async def ainsert_records(
        self, registry: str | Registry, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """Async version of :meth:`insert_records`."""
        reg = await self.aget_registry(registry)
        url = _registry_url(await self.aget_instance(), reg)
        return _written_rows(await self._asend("PUT", url, json=list(records), idempotent=False), reg)

    async def aupdate_record(
        self, registry: str | Registry, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Async version of :meth:`update_record`."""
        reg = await self.aget_registry(registry)
        url = f"{_registry_url(await self.aget_instance(), reg)}/{uid}"
        rows = _written_rows(await self._asend("PATCH", url, json=dict(values)), reg)
        return rows[0] if rows else None
