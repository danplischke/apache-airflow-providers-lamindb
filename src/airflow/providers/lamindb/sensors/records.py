from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.triggers.records import (
    LaminDBRecordSensorTrigger,
    branch_scope_filter,
    ready_records,
)
from airflow.providers.lamindb.utils.enums import plain_value
from airflow.providers.lamindb.utils.filters import combine_filters, normalize_filter
from airflow.providers.lamindb.utils.lamindb import artifact_filter, normalize_str_list
from airflow.sdk import BaseSensorOperator, PokeReturnValue, conf
from airflow.sdk.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.providers.lamindb.utils.filters import FilterLike
    from airflow.sdk import Context


class LaminDBRecordSensor(BaseSensorOperator):
    """
    Wait until records matching a filter exist in a LaminDB registry.

    Returns the matching records (newest first, at most ``limit``), which are pushed to XCom.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:LaminDBRecordSensor`

    :param registry: Registry to query, e.g. ``LaminDBRegistry.RUN``, ``"core.artifact"`` or
        ``"bionty.celltype"``.
    :param filter: Filter built with :class:`~airflow.providers.lamindb.utils.filters.F`, e.g.
        ``F(ArtifactField.KEY) == "raw/sample1.fastq.gz"``, or a LaminHub REST filter such as
        ``{"key": {"eq": "raw/sample1.fastq.gz"}}``. It's validated when the DAG is parsed.
    :param branch: Only records on this branch (name or id); ``None`` for all branches and for
        registries without branches.
    :param min_count: Number of matching records to wait for.
    :param limit: Maximum number of records returned.
    :param wait_for_upload: Ignore artifacts whose upload to storage is still in progress.
    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param deferrable: Wait in the triggerer instead of occupying a worker slot.
    """

    template_fields: Sequence[str] = ("registry", "filter", "branch", "instance")
    template_fields_renderers = {"filter": "json"}

    def __init__(
        self,
        *,
        registry: str,
        filter: FilterLike | None = None,
        branch: str | int | None = "main",
        min_count: int = 1,
        limit: int = 100,
        wait_for_upload: bool | None = None,
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if min_count < 1 or limit < min_count:
            raise ValueError("min_count must be at least 1 and not larger than limit")
        self.registry: str = plain_value(registry)
        self.filter = normalize_filter(filter)
        self.branch = branch
        self.min_count = min_count
        self.limit = limit
        self.wait_for_upload = wait_for_upload
        self.lamindb_conn_id = lamindb_conn_id
        self.instance = instance
        self.deferrable = deferrable

    @property
    def hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    @property
    def _wait_for_upload(self) -> bool:
        if self.wait_for_upload is not None:
            return self.wait_for_upload
        return self.registry.lower() in ("core.artifact", "artifact", "lamindb.artifact")

    def _query(self) -> dict[str, Any] | None:
        return combine_filters(self.filter, branch_scope_filter(self.branch))

    def poke(self, context: Context) -> PokeReturnValue:
        hook = self.hook
        try:
            records = hook.query_records(self.registry, self._query(), order_by=["-id"], limit=self.limit)
        finally:
            hook.close()
        matching = ready_records(records, wait_for_upload=self._wait_for_upload)
        self.log.info(
            "Found %d matching records in %s (waiting for %d)", len(matching), self.registry, self.min_count
        )
        if len(matching) >= self.min_count:
            return PokeReturnValue(is_done=True, xcom_value=matching)
        return PokeReturnValue(is_done=False)

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        result = self.poke(context)
        if result.is_done:
            return result.xcom_value
        self.defer(
            trigger=LaminDBRecordSensorTrigger(
                registry=self.registry,
                filter=self._query(),
                branch=None,
                min_count=self.min_count,
                limit=self.limit,
                wait_for_upload=self._wait_for_upload,
                lamindb_conn_id=self.lamindb_conn_id,
                instance=self.instance,
                poll_interval=self.poke_interval,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        if event.get("status") == "success":
            return event.get("records")
        raise AirflowException(event.get("message", f"Unexpected trigger event {event!r}"))


class LaminDBArtifactSensor(LaminDBRecordSensor):
    """
    Wait until LaminDB artifacts matching some criteria exist and their upload has completed.

    Returns the matching artifacts (newest first, at most ``limit``), which are pushed to XCom.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:LaminDBArtifactSensor`

    :param key: Artifact key, e.g. ``"raw/sample1.fastq.gz"``.
    :param key_prefix: Key prefix, e.g. ``"raw/"``.
    :param suffix: Suffix (or suffixes), e.g. ``".parquet"``.
    :param kind: Artifact kind (or kinds), e.g. ``ArtifactKind.DATASET``.
    :param include_internal: Also consider artifacts LaminDB creates internally (run logs, ...).
    :param filter: Additional filter, see :class:`LaminDBRecordSensor`.
    :param wait_for_upload: Ignore artifacts whose upload is still in progress (default).
    """

    template_fields: Sequence[str] = (*LaminDBRecordSensor.template_fields, "key", "key_prefix")

    def __init__(
        self,
        *,
        key: str | None = None,
        key_prefix: str | None = None,
        suffix: str | Sequence[str] | None = None,
        kind: str | Sequence[str] | None = None,
        include_internal: bool = False,
        wait_for_upload: bool = True,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("registry", None)
        super().__init__(registry="core.artifact", wait_for_upload=wait_for_upload, **kwargs)
        self.key = key
        self.key_prefix = key_prefix
        self.suffix = normalize_str_list(suffix)
        self.kind = normalize_str_list(kind)
        self.include_internal = include_internal

    def _query(self) -> dict[str, Any] | None:
        return combine_filters(
            artifact_filter(
                key=self.key,
                key_prefix=self.key_prefix,
                suffix=self.suffix,
                kind=self.kind,
                include_internal=self.include_internal,
                extra=self.filter,
            ),
            branch_scope_filter(self.branch),
        )
