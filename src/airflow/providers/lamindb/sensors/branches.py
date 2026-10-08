from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.triggers.branches import (
    LaminDBBranchStatusSensorTrigger,
    branch_summary,
    evaluate_branch_status,
)
from airflow.providers.lamindb.utils.lamindb import normalize_statuses
from airflow.sdk import BaseSensorOperator, PokeReturnValue, conf
from airflow.sdk.exceptions import AirflowException, AirflowFailException

if TYPE_CHECKING:
    from airflow.sdk import Context


class LaminDBBranchStatusSensor(BaseSensorOperator):
    """
    Wait until a LaminDB branch reaches a status, e.g. until its Change Request is merged.

    Returns the branch (with its ``status``), which is pushed to XCom. Fails without retries if the
    branch reaches one of the ``failure_status`` statuses (by default, when the Change Request is
    closed without merging).

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:LaminDBBranchStatusSensor`

    :param branch: Branch name or id.
    :param target_status: Status (or statuses) to wait for, any of ``standalone``, ``draft``,
        ``review``, ``merged`` and ``closed``.
    :param failure_status: Status (or statuses) that fail the sensor.
    :param lamindb_conn_id: Airflow connection of type ``lamindb``.
    :param instance: LaminDB instance ``owner/name``; overrides the connection's instance.
    :param deferrable: Wait in the triggerer instead of occupying a worker slot.
    """

    template_fields: Sequence[str] = ("branch", "instance")

    def __init__(
        self,
        *,
        branch: str | int,
        target_status: str | Sequence[str] = "merged",
        failure_status: str | Sequence[str] = ("closed",),
        lamindb_conn_id: str | None = LaminDBHook.default_conn_name,
        instance: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.branch = branch
        self.target_status = normalize_statuses(target_status, name="target_status") or []
        self.failure_status = normalize_statuses(failure_status, name="failure_status") or []
        if not self.target_status:
            raise ValueError("target_status must not be empty")
        if overlap := set(self.target_status) & set(self.failure_status):
            raise ValueError(f"Statuses {sorted(overlap)} are both target and failure statuses")
        self.lamindb_conn_id = lamindb_conn_id
        self.instance = instance
        self.deferrable = deferrable

    @property
    def hook(self) -> LaminDBHook:
        return LaminDBHook(lamindb_conn_id=self.lamindb_conn_id, instance=self.instance)

    def poke(self, context: Context) -> PokeReturnValue:
        hook = self.hook
        try:
            branch = hook.get_branch(self.branch)
        finally:
            hook.close()
        if branch is None:
            self.log.info("Branch %r does not exist (yet)", self.branch)
            return PokeReturnValue(is_done=False)
        summary = branch_summary(branch)
        outcome = evaluate_branch_status(branch, self.target_status, self.failure_status)
        if outcome == "failure":
            raise AirflowFailException(f"Branch {self.branch!r} has status {summary['status']!r}")  # type: ignore[index]
        if outcome == "success":
            return PokeReturnValue(is_done=True, xcom_value=summary)
        self.log.info("Branch %r has status %r", self.branch, summary["status"])  # type: ignore[index]
        return PokeReturnValue(is_done=False)

    def execute(self, context: Context) -> Any:
        if not self.deferrable:
            return super().execute(context)
        result = self.poke(context)
        if result.is_done:
            return result.xcom_value
        self.defer(
            trigger=LaminDBBranchStatusSensorTrigger(
                branch=self.branch,
                target_status=self.target_status,
                failure_status=self.failure_status,
                lamindb_conn_id=self.lamindb_conn_id,
                instance=self.instance,
                poll_interval=self.poke_interval,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:
        status = event.get("status")
        if status == "success":
            return event.get("branch")
        if status == "failure":
            raise AirflowFailException(event.get("message"))
        raise AirflowException(event.get("message", f"Unexpected trigger event {event!r}"))
