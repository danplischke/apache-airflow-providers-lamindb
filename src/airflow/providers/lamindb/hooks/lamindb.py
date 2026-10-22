from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.client.client import (
    DEFAULT_HUB_API_URL,
    MAX_PAGE_SIZE,
    LaminDBInstance,
    LaminHubClient,
    LaminHubConfig,
    parse_instance_slug,
)
from airflow.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.lamindb.client.enums import LaminDBRegistry
    from airflow.providers.lamindb.client.filters import FilterExpression
    from airflow.providers.lamindb.client.models import (
        BranchRecordResponse,
        InstanceSchemaResponse,
        RecordOrder,
        UserRecordResponse,
    )
    from airflow.providers.lamindb.client.registries import Registry
    from airflow.sdk import Connection


class LaminDBHook(BaseHook):
    """
    Interact with LaminDB instances hosted on LaminHub via the LaminHub REST API.

    An Airflow wrapper around [`LaminHubClient`][airflow.providers.lamindb.client.client.LaminHubClient]:
    the hook reads the connection and delegates requests to one client. The connection's password is
    a Lamin API key; it is exchanged for a short-lived access token that is refreshed automatically.
    Without an API key, requests are anonymous, which works for public instances. The instance
    (``owner/name``) is read from the connection's ``instance`` extra and can be overridden per
    hook. Instances on lamindb older than 2.8.1 are rejected with
    [`LaminDBVersionError`][airflow.providers.lamindb.exceptions.LaminDBVersionError].

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
        self._config: LaminHubConfig | None = None
        self._lamindb_client: LaminHubClient | None = None

    # -- configuration ---------------------------------------------------------------------------

    @staticmethod
    def _config_from_connection(conn: Connection | None, instance: str | None) -> LaminHubConfig:
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
        return LaminHubConfig(
            hub_api_url=hub_api_url.rstrip("/"),
            api_key=api_key,
            owner=owner,
            name=name,
            cache_dir=extra.get("cache_dir") or extra.get("extra__lamindb__cache_dir") or None,
        )

    def _get_config(self) -> LaminHubConfig:
        if self._config is None:
            conn = self.get_connection(self.lamindb_conn_id) if self.lamindb_conn_id else None
            self._config = self._config_from_connection(conn, self.instance_slug)
        return self._config

    async def _aget_config(self) -> LaminHubConfig:
        if self._config is None:
            conn = await self.aget_connection(self.lamindb_conn_id) if self.lamindb_conn_id else None
            self._config = self._config_from_connection(conn, self.instance_slug)
        return self._config

    # -- client ----------------------------------------------------------------------------------

    def _new_client(self, config: LaminHubConfig) -> LaminHubClient:
        self._lamindb_client = LaminHubClient(
            config, timeout=self.timeout, retries=self.retries, retry_backoff=self.retry_backoff
        )
        return self._lamindb_client

    def get_client(self) -> LaminHubClient:
        """Return the (lazily created) LaminHub client for the connection."""
        return self._lamindb_client or self._new_client(self._get_config())

    async def aget_client(self) -> LaminHubClient:
        """Async version of [`get_client`][]: reads the connection without blocking the event loop."""
        return self._lamindb_client or self._new_client(await self._aget_config())

    def get_conn(self) -> LaminHubClient:
        """Return the LaminHub client, as [`get_client`][]."""
        return self.get_client()

    def close(self) -> None:
        """Close the client's synchronous HTTP connections."""
        if self._lamindb_client is not None:
            self._lamindb_client.close()

    async def aclose(self) -> None:
        """Close the client's asynchronous HTTP connections."""
        if self._lamindb_client is not None:
            await self._lamindb_client.aclose()

    # -- synchronous API ---------------------------------------------------------------------------

    def get_instance_slug(self) -> str:
        """Return the configured instance ``owner/name`` without contacting LaminHub."""
        config = self._get_config()
        return f"{config.owner}/{config.name}"

    def get_api_key(self) -> str | None:
        """Return the connection's Lamin API key, or ``None`` for anonymous access."""
        return self._get_config().api_key

    def get_cache_dir(self) -> str | None:
        """Return the connection's ``cache_dir`` extra: the lamindb cache shared by virtualenv tasks."""
        return self._get_config().cache_dir

    def get_instance(self) -> LaminDBInstance:
        """
        Return the configured instance (resolves its id and regional API URL on first use).

        Raises [`LaminDBVersionError`][airflow.providers.lamindb.exceptions.LaminDBVersionError] if the
        instance runs lamindb older than 2.8.1.
        """
        return self.get_client().get_instance()

    def get_schema(self) -> InstanceSchemaResponse:
        """Return the instance schema; ``schema.root`` maps ``{module: {model: RegistrySchemaResponse}}``."""
        return self.get_client().get_schema()

    def get_registry(self, registry: LaminDBRegistry | Registry | str) -> Registry:
        """Resolve a registry name such as ``core.artifact`` or ``bionty.gene``."""
        return self.get_client().get_registry(registry)

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
        return self.get_client().query_records(
            registry,
            filter,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
            include_foreign_keys=include_foreign_keys,
            search=search,
            branch_ids=branch_ids,
        )

    def get_records_by_ids(
        self,
        registry: LaminDBRegistry | Registry | str,
        ids: Iterable[int],
        filter: FilterExpression | None = None,
    ) -> dict[int, dict[str, Any]]:
        """Fetch records by id, optionally restricted by an additional filter. Returns ``{id: record}``."""
        return self.get_client().get_records_by_ids(registry, ids, filter)

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
        return self.get_client().query_dbwrites(filter, after_id=after_id, limit=limit, descending=descending)

    def get_latest_dbwrite_id(self) -> int:
        """Return the id of the most recent database write (``0`` if there is none)."""
        return self.get_client().get_latest_dbwrite_id()

    def get_branch(self, branch: str | int) -> BranchRecordResponse | None:
        """Return a branch record by name or id, or ``None`` if it doesn't exist."""
        return self.get_client().get_branch(branch)

    def get_users(self, ids: Iterable[int]) -> dict[int, UserRecordResponse]:
        """Return ``{id: user}`` for the given user ids."""
        return self.get_client().get_users(ids)

    def insert_records(
        self, registry: LaminDBRegistry | Registry | str, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Insert records into a registry and return the created rows.

        Needs write access to the instance. Fields are column names, e.g. ``{"name": "treated"}`` or
        ``{"type_id": 3}``. Inserts are not retried after errors that may have reached LaminHub, so a
        failed request never creates a record twice.
        """
        return self.get_client().insert_records(registry, records)

    def update_record(
        self, registry: LaminDBRegistry | Registry | str, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Update fields of the record with ``uid`` and return the updated row (if LaminHub returns it)."""
        return self.get_client().update_record(registry, uid, values)

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
        """Async version of [`get_instance`][]."""
        return await (await self.aget_client()).aget_instance()

    async def aget_schema(self) -> InstanceSchemaResponse:
        """Async version of [`get_schema`][]."""
        return await (await self.aget_client()).aget_schema()

    async def aget_registry(self, registry: LaminDBRegistry | Registry | str) -> Registry:
        """Async version of [`get_registry`][]."""
        return await (await self.aget_client()).aget_registry(registry)

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
        return await (await self.aget_client()).aquery_records(
            registry,
            filter,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
            include_foreign_keys=include_foreign_keys,
            search=search,
            branch_ids=branch_ids,
        )

    async def aget_records_by_ids(
        self,
        registry: LaminDBRegistry | Registry | str,
        ids: Iterable[int],
        filter: FilterExpression | None = None,
    ) -> dict[int, dict[str, Any]]:
        """Async version of [`get_records_by_ids`][]."""
        return await (await self.aget_client()).aget_records_by_ids(registry, ids, filter)

    async def aquery_dbwrites(
        self,
        filter: FilterExpression | None = None,
        *,
        after_id: int | None = None,
        limit: int = MAX_PAGE_SIZE,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        """Async version of [`query_dbwrites`][]."""
        return await (await self.aget_client()).aquery_dbwrites(
            filter, after_id=after_id, limit=limit, descending=descending
        )

    async def aget_latest_dbwrite_id(self) -> int:
        """Async version of [`get_latest_dbwrite_id`][]."""
        return await (await self.aget_client()).aget_latest_dbwrite_id()

    async def aget_branch(self, branch: str | int) -> BranchRecordResponse | None:
        """Async version of [`get_branch`][]."""
        return await (await self.aget_client()).aget_branch(branch)

    async def aget_users(self, ids: Iterable[int]) -> dict[int, UserRecordResponse]:
        """Async version of [`get_users`][]."""
        return await (await self.aget_client()).aget_users(ids)

    async def ainsert_records(
        self, registry: LaminDBRegistry | Registry | str, records: Sequence[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        """Async version of [`insert_records`][]."""
        return await (await self.aget_client()).ainsert_records(registry, records)

    async def aupdate_record(
        self, registry: LaminDBRegistry | Registry | str, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Async version of [`update_record`][]."""
        return await (await self.aget_client()).aupdate_record(registry, uid, values)
