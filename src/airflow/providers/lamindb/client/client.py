"""Client for the LaminHub REST API."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import httpx
from pydantic import ValidationError

from airflow.providers.lamindb import __version__
from airflow.providers.lamindb.client.filters import (
    BranchField,
    F,
    FilterExpression,
    combine_filters,
    normalize_filter,
    normalize_order_by,
)
from airflow.providers.lamindb.client.models import (
    BranchRecordResponse,
    DbWriteResponse,
    InstanceSchemaResponse,
    InstanceSettingsResponse,
    RecordInsertBody,
    RecordOrder,
    RecordQueryBody,
    RecordQueryParams,
    RecordScope,
    RecordUpdateBody,
    TokenRequest,
    TokenResponse,
    UserRecordResponse,
)
from airflow.providers.lamindb.client.registries import DBWRITE_REGISTRY, Registry, resolve_registry
from airflow.providers.lamindb.exceptions import LaminDBApiError, LaminDBVersionError
from airflow.providers.lamindb.execution.runtime import MIN_LAMINDB_VERSION, lamindb_version_supported

if TYPE_CHECKING:
    from airflow.providers.lamindb.client.enums import LaminDBRegistry

DEFAULT_HUB_API_URL = "https://aws.us-east-1.lamin.ai/api"
_NO_SCHEMA = InstanceSchemaResponse({})
"""Registries that aren't part of the instance schema (``hubmodule.*``) resolve without one."""
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
_USER_AGENT = f"apache-airflow-providers-lamindb/{__version__}"


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
class LaminHubConfig:
    """Where a [`LaminHubClient`][] connects: the LaminHub API, the API key and the instance."""

    hub_api_url: str
    api_key: str | None = field(repr=False)
    owner: str
    name: str
    cache_dir: str | None = None


def parse_instance_slug(slug: str) -> tuple[str, str]:
    """Split an ``owner/name`` instance slug."""
    owner, _, name = slug.strip().strip("/").partition("/")
    if not owner or not name or "/" in name:
        raise ValueError(f"Invalid LaminDB instance {slug!r}; expected the form 'owner/name'.")
    return owner, name


def _registry_url(instance: LaminDBInstance, registry: Registry) -> str:
    return f"{instance.api_url}/instances/{instance.id}/modules/{registry.module}/{registry.model}"


def _check_rows(rows: Any, registry: Registry, action: str) -> list[dict[str, Any]]:
    """Check that a response is a list of records, and validate rows of the database write log.

    Records of other registries stay unvalidated dicts: their columns depend on the registry and
    ``select``, and one odd value, such as a malformed ``_aux``, must not fail every query that
    returns its record. Callers parse the fields they use. The write log is LaminHub's own metadata,
    and its ``id`` and ``sqlrecord_id`` drive trigger cursors, so its rows are validated.
    """
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise LaminDBApiError(
            f"Unexpected response when {action} {registry.name}: expected a list of records."
        )
    if registry == DBWRITE_REGISTRY:
        for row in rows:
            try:
                DbWriteResponse.model_validate(row)
            except ValidationError as err:
                # field names only: values may be record data
                fields = ", ".join(sorted({".".join(map(str, e["loc"])) for e in err.errors()}))
                write_id = row.get("id")
                write = f"write {write_id}" if type(write_id) is int else "a write"
                raise LaminDBApiError(
                    f"Unexpected response when {action} {registry.name}: invalid {fields} in {write}."
                ) from None
    return rows


def _users(records: Mapping[int, Mapping[str, Any]]) -> dict[int, UserRecordResponse]:
    return {user_id: UserRecordResponse.model_validate(user) for user_id, user in records.items()}


def _written_rows(data: Any, registry: Registry) -> list[dict[str, Any]]:
    """The rows a write endpoint returns: a list of rows, a single row, or nothing."""
    if data is None:
        return []
    return _check_rows([data] if isinstance(data, dict) else data, registry, "writing")


def chunk_ids(ids: Iterable[int]) -> list[list[int]]:
    """Split ``ids`` into sorted, de-duplicated chunks that fit an ``in`` filter of one page."""
    unique = sorted(set(ids))
    return [unique[i : i + MAX_PAGE_SIZE] for i in range(0, len(unique), MAX_PAGE_SIZE)]


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


class LaminHubClient:
    """
    Synchronous and asynchronous client for the LaminHub REST API of one LaminDB instance.

    The API key of ``config`` is exchanged for a short-lived access token that is refreshed
    automatically; without one, requests are anonymous, which works for public instances. The
    instance, its schema and the access token are cached and shared by the sync and async methods.
    Instances on lamindb older than 2.8.1 are rejected with
    [`LaminDBVersionError`][airflow.providers.lamindb.exceptions.LaminDBVersionError].

    :param config: LaminHub URL, API key and instance.
    :param timeout: Timeout for HTTP requests in seconds.
    :param retries: Number of retries for connection errors and retryable HTTP status codes
        (429 and 5xx).
    :param retry_backoff: Base delay in seconds for the exponential retry backoff.
    """

    def __init__(
        self,
        config: LaminHubConfig,
        *,
        timeout: float = 30.0,
        retries: int = 3,
        retry_backoff: float = 1.0,
    ) -> None:
        self.config = config
        self.timeout = timeout
        self.retries = retries
        self.retry_backoff = retry_backoff
        self._access_token: str | None = None
        self._access_token_expiry = 0.0
        self._instance: LaminDBInstance | None = None
        self._schema: InstanceSchemaResponse | None = None
        self._client: httpx.Client | None = None
        self._async_client: httpx.AsyncClient | None = None

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout, headers={"User-Agent": _USER_AGENT})
        return self._client

    def _async_http(self) -> httpx.AsyncClient:
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
        try:
            token = TokenResponse.model_validate(payload).access_token
        except ValidationError:
            raise LaminDBApiError("LaminHub did not return an access token for the API key.") from None
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
                transient=response.status_code == 429 or response.status_code >= 500,
            )
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            content_type = response.headers.get("content-type", "no content type")
            # e.g. a proxy's HTML page or a truncated body; the next request may succeed
            raise LaminDBApiError(
                f"LaminHub API request {method} {url} returned a response that isn't JSON "
                f"(status {response.status_code}, {content_type}).",
                http_status_code=response.status_code,
                transient=True,
            ) from None

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
        client = self._http()
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
                    raise LaminDBApiError(
                        f"LaminHub API request {method} {url} failed: {err}", transient=True
                    ) from err
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
        client = self._async_http()
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
                    raise LaminDBApiError(
                        f"LaminHub API request {method} {url} failed: {err}", transient=True
                    ) from err
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
        config = self.config
        if config.api_key is None:
            return None
        if not self._token_is_valid():
            payload = self._send(
                "POST",
                f"{config.hub_api_url}/account/jwt",
                json=TokenRequest(api_key=config.api_key).model_dump(),
                authenticate=False,
            )
            return self._store_token(payload)
        return self._access_token

    async def _aget_access_token(self) -> str | None:
        config = self.config
        if config.api_key is None:
            return None
        if not self._token_is_valid():
            payload = await self._asend(
                "POST",
                f"{config.hub_api_url}/account/jwt",
                json=TokenRequest(api_key=config.api_key).model_dump(),
                authenticate=False,
            )
            return self._store_token(payload)
        return self._access_token

    @staticmethod
    def _settings_url(config: LaminHubConfig) -> str:
        return f"{config.hub_api_url}/instances/{config.owner}/{config.name}/settings"

    @staticmethod
    def _instance_from_settings(config: LaminHubConfig, settings: Any) -> LaminDBInstance:
        try:
            parsed = InstanceSettingsResponse.model_validate(settings)
        except ValidationError:
            raise LaminDBApiError(
                f"Unexpected settings response for instance {config.owner}/{config.name}."
            ) from None
        instance = LaminDBInstance(
            owner=parsed.owner or config.owner,
            name=parsed.name or config.name,
            id=str(parsed.id),
            api_url=(parsed.api_url or config.hub_api_url).rstrip("/"),
            lnid=parsed.lnid,
            schema_str=parsed.schema_str,
            public=parsed.public,
            lamindb_version=parsed.lamindb_version,
        )
        # an unknown version passes; lineage tasks check the lamindb they install instead
        if instance.lamindb_version and not lamindb_version_supported(instance.lamindb_version):
            raise LaminDBVersionError(
                f"The LaminDB instance {instance.slug} runs lamindb {instance.lamindb_version}, but "
                f"apache-airflow-providers-lamindb needs lamindb {MIN_LAMINDB_VERSION} or newer. "
                "Migrate the instance first."
            )
        return instance

    @staticmethod
    def _query_request(
        *,
        filter: FilterExpression | None,
        select: Sequence[str] | None,
        order_by: Sequence[str | RecordOrder] | None,
        limit: int,
        offset: int,
        include_foreign_keys: bool,
        search: str | None,
        branch_ids: Sequence[int] | None,
    ) -> tuple[RecordQueryParams, RecordQueryBody]:
        params = RecordQueryParams(limit=limit, offset=offset, include_foreign_keys=include_foreign_keys)
        body = RecordQueryBody(
            order_by=normalize_order_by(order_by),
            filter=normalize_filter(filter),
            select=list(select) if select else None,
            search=search or None,
            scope=RecordScope(branch_ids=list(branch_ids)) if branch_ids is not None else None,
        )
        return params, body

    @staticmethod
    def _check_page(page: Any, registry: Registry) -> list[dict[str, Any]]:
        return _check_rows(page, registry, "querying")

    @staticmethod
    def _dbwrite_filter(filter: FilterExpression | None, after_id: int | None) -> FilterExpression | None:
        return combine_filters(F("id").gt(after_id) if after_id is not None else None, filter)

    @staticmethod
    def _branch_filter(branch: str | int) -> FilterExpression:
        if isinstance(branch, int):
            return F(BranchField.ID).eq(branch)
        return F(BranchField.NAME).eq(branch)

    def get_instance(self) -> LaminDBInstance:
        """
        Return the configured instance (resolves its id and regional API URL on first use).

        Raises [`LaminDBVersionError`][airflow.providers.lamindb.exceptions.LaminDBVersionError] if the
        instance runs lamindb older than 2.8.1.
        """
        if self._instance is None:
            config = self.config
            settings = self._send("GET", self._settings_url(config))
            self._instance = self._instance_from_settings(config, settings)
        return self._instance

    def get_schema(self) -> InstanceSchemaResponse:
        """Return the instance schema; ``schema.root`` maps ``{module: {model: RegistrySchemaResponse}}``."""
        if self._schema is None:
            instance = self.get_instance()
            self._schema = self._check_schema(
                self._send("GET", f"{instance.api_url}/instances/{instance.id}/schema")
            )
        return self._schema

    @staticmethod
    def _check_schema(schema: Any) -> InstanceSchemaResponse:
        try:
            return InstanceSchemaResponse.model_validate(schema)
        except ValidationError:
            raise LaminDBApiError("Unexpected schema response from LaminHub.") from None

    def get_registry(self, registry: LaminDBRegistry | Registry | str) -> Registry:
        """Resolve a registry name such as ``core.artifact`` or ``bionty.gene``."""
        if isinstance(registry, Registry):
            return registry
        if registry.lower().startswith("hubmodule."):
            return resolve_registry(_NO_SCHEMA, registry)
        return resolve_registry(self.get_schema(), registry)

    def query_records(
        self,
        registry: LaminDBRegistry | Registry | str,
        filter: FilterExpression | None = None,
        *,
        select: Sequence[str] | None = None,
        order_by: Sequence[str | RecordOrder] | None = None,
        limit: int = 50,
        offset: int = 0,
        include_foreign_keys: bool = True,
        search: str | None = None,
        branch_ids: Sequence[int] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Query records of a registry.

        :param registry: Registry name such as ``core.artifact``, ``core.run`` or ``bionty.gene``,
            or a [`LaminDBRegistry`][airflow.providers.lamindb.client.enums.LaminDBRegistry] member.
        :param filter: Filter built with [`F`][airflow.providers.lamindb.client.filters.F], e.g.
            ``F(ArtifactField.SUFFIX) == ".csv"``. Convert a LaminHub REST filter dict
            (https://docs.lamin.ai/rest) with
            [`FilterExpression.from_dict`][airflow.providers.lamindb.client.filters.FilterExpression.from_dict].
        :param select: Fields (and relations such as ``created_by(handle)``) to return.
        :param order_by: Sort order, for example ``["-created_at"]`` or
            ``[RecordOrder(field="created_at", descending=True)]``. Defaults to ascending ``id``.
        :param limit: Maximum number of records; requests are paginated in pages of 200.
        :param offset: Number of records to skip.
        :param include_foreign_keys: Include foreign key columns (``run_id``, ``branch_id``, ...).
        :param search: Full-text search string.
        :param branch_ids: Restrict the query scope to these branch ids.

        Invalid request fields raise ``pydantic.ValidationError`` before HTTP requests are made.
        ``limit`` and ``offset`` must be non-negative integers.
        """
        query_params, query_body = self._query_request(
            filter=filter,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
            include_foreign_keys=include_foreign_keys,
            search=search,
            branch_ids=branch_ids,
        )
        reg = self.get_registry(registry)
        instance = self.get_instance()
        url = _registry_url(instance, reg)
        body = query_body.model_dump(mode="json", exclude_none=True)
        results: list[dict[str, Any]] = []
        while len(results) < limit:
            page_size = min(MAX_PAGE_SIZE, limit - len(results))
            params = query_params.model_copy(
                update={"limit": page_size, "offset": offset + len(results)}
            ).model_dump()
            page = self._check_page(self._send("POST", url, params=params, json=body), reg)
            results.extend(page)
            if len(page) < page_size:
                break
        return results

    def get_records_by_ids(
        self,
        registry: LaminDBRegistry | Registry | str,
        ids: Iterable[int],
        filter: FilterExpression | None = None,
    ) -> dict[int, dict[str, Any]]:
        """Fetch records by id, optionally restricted by an additional filter. Returns ``{id: record}``."""
        records: dict[int, dict[str, Any]] = {}
        for chunk in chunk_ids(ids):
            combined = combine_filters(F("id").is_in(chunk), filter)
            for record in self.query_records(registry, combined, limit=len(chunk)):
                records[record["id"]] = record
        return records

    def query_dbwrites(
        self,
        filter: FilterExpression | None = None,
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
            order_by=[RecordOrder(field="id", descending=descending)],
            limit=limit,
        )

    def get_latest_dbwrite_id(self) -> int:
        """Return the id of the most recent database write (``0`` if there is none)."""
        rows = self.query_dbwrites(limit=1, descending=True)
        return int(rows[0]["id"]) if rows else 0

    def get_branch(self, branch: str | int) -> BranchRecordResponse | None:
        """Return a branch record by name or id, or ``None`` if it doesn't exist."""
        rows = self.query_records("core.branch", self._branch_filter(branch), limit=1)
        return BranchRecordResponse.model_validate(rows[0]) if rows else None

    def get_users(self, ids: Iterable[int]) -> dict[int, UserRecordResponse]:
        """Return ``{id: user}`` for the given user ids."""
        return _users(self.get_records_by_ids("core.user", ids))

    def insert_records(
        self, registry: LaminDBRegistry | Registry | str, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Insert records into a registry and return the created rows.

        Needs write access to the instance. Fields are column names, e.g. ``{"name": "treated"}`` or
        ``{"type_id": 3}``. Inserts are not retried after errors that may have reached LaminHub, so a
        failed request never creates a record twice.
        """
        body = RecordInsertBody.model_validate(list(records)).model_dump(mode="json")
        reg = self.get_registry(registry)
        url = _registry_url(self.get_instance(), reg)
        return _written_rows(self._send("PUT", url, json=body, idempotent=False), reg)

    def update_record(
        self, registry: LaminDBRegistry | Registry | str, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Update fields of the record with ``uid`` and return the updated row (if LaminHub returns it)."""
        body = RecordUpdateBody.model_validate(dict(values)).model_dump(mode="json")
        reg = self.get_registry(registry)
        url = f"{_registry_url(self.get_instance(), reg)}/{uid}"
        rows = _written_rows(self._send("PATCH", url, json=body), reg)
        return rows[0] if rows else None

    async def aget_instance(self) -> LaminDBInstance:
        """Async version of [`get_instance`][]."""
        if self._instance is None:
            config = self.config
            settings = await self._asend("GET", self._settings_url(config))
            self._instance = self._instance_from_settings(config, settings)
        return self._instance

    async def aget_schema(self) -> InstanceSchemaResponse:
        """Async version of [`get_schema`][]."""
        if self._schema is None:
            instance = await self.aget_instance()
            self._schema = self._check_schema(
                await self._asend("GET", f"{instance.api_url}/instances/{instance.id}/schema")
            )
        return self._schema

    async def aget_registry(self, registry: LaminDBRegistry | Registry | str) -> Registry:
        """Async version of [`get_registry`][]."""
        if isinstance(registry, Registry):
            return registry
        if registry.lower().startswith("hubmodule."):
            return resolve_registry(_NO_SCHEMA, registry)
        return resolve_registry(await self.aget_schema(), registry)

    async def aquery_records(
        self,
        registry: LaminDBRegistry | Registry | str,
        filter: FilterExpression | None = None,
        *,
        select: Sequence[str] | None = None,
        order_by: Sequence[str | RecordOrder] | None = None,
        limit: int = 50,
        offset: int = 0,
        include_foreign_keys: bool = True,
        search: str | None = None,
        branch_ids: Sequence[int] | None = None,
    ) -> list[dict[str, Any]]:
        """Async version of [`query_records`][]."""
        query_params, query_body = self._query_request(
            filter=filter,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
            include_foreign_keys=include_foreign_keys,
            search=search,
            branch_ids=branch_ids,
        )
        reg = await self.aget_registry(registry)
        instance = await self.aget_instance()
        url = _registry_url(instance, reg)
        body = query_body.model_dump(mode="json", exclude_none=True)
        results: list[dict[str, Any]] = []
        while len(results) < limit:
            page_size = min(MAX_PAGE_SIZE, limit - len(results))
            params = query_params.model_copy(
                update={"limit": page_size, "offset": offset + len(results)}
            ).model_dump()
            page = self._check_page(await self._asend("POST", url, params=params, json=body), reg)
            results.extend(page)
            if len(page) < page_size:
                break
        return results

    async def aget_records_by_ids(
        self,
        registry: LaminDBRegistry | Registry | str,
        ids: Iterable[int],
        filter: FilterExpression | None = None,
    ) -> dict[int, dict[str, Any]]:
        """Async version of [`get_records_by_ids`][]."""
        records: dict[int, dict[str, Any]] = {}
        for chunk in chunk_ids(ids):
            combined = combine_filters(F("id").is_in(chunk), filter)
            for record in await self.aquery_records(registry, combined, limit=len(chunk)):
                records[record["id"]] = record
        return records

    async def aquery_dbwrites(
        self,
        filter: FilterExpression | None = None,
        *,
        after_id: int | None = None,
        limit: int = MAX_PAGE_SIZE,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Async version of [`query_dbwrites`][]."""
        return await self.aquery_records(
            DBWRITE_REGISTRY,
            self._dbwrite_filter(filter, after_id),
            order_by=[RecordOrder(field="id", descending=descending)],
            limit=limit,
        )

    async def aget_latest_dbwrite_id(self) -> int:
        """Async version of [`get_latest_dbwrite_id`][]."""
        rows = await self.aquery_dbwrites(limit=1, descending=True)
        return int(rows[0]["id"]) if rows else 0

    async def aget_branch(self, branch: str | int) -> BranchRecordResponse | None:
        """Async version of [`get_branch`][]."""
        rows = await self.aquery_records("core.branch", self._branch_filter(branch), limit=1)
        return BranchRecordResponse.model_validate(rows[0]) if rows else None

    async def aget_users(self, ids: Iterable[int]) -> dict[int, UserRecordResponse]:
        """Async version of [`get_users`][]."""
        return _users(await self.aget_records_by_ids("core.user", ids))

    async def ainsert_records(
        self, registry: LaminDBRegistry | Registry | str, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """Async version of [`insert_records`][]."""
        body = RecordInsertBody.model_validate(list(records)).model_dump(mode="json")
        reg = await self.aget_registry(registry)
        url = _registry_url(await self.aget_instance(), reg)
        return _written_rows(await self._asend("PUT", url, json=body, idempotent=False), reg)

    async def aupdate_record(
        self, registry: LaminDBRegistry | Registry | str, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Async version of [`update_record`][]."""
        body = RecordUpdateBody.model_validate(dict(values)).model_dump(mode="json")
        reg = await self.aget_registry(registry)
        url = f"{_registry_url(await self.aget_instance(), reg)}/{uid}"
        rows = _written_rows(await self._asend("PATCH", url, json=body), reg)
        return rows[0] if rows else None
