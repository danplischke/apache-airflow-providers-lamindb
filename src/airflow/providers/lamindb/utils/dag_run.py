"""Worker-side helpers that read the Airflow context. No lamindb required."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException

FAILED_STATES = frozenset({"failed", "upstream_failed"})


def flow_run_reference(dag_id: str, run_id: str) -> str:
    """Reference stored on the flow run. ``run_id`` alone is not unique across DAGs."""
    return f"{dag_id}/{run_id}"


def dag_and_run_id(context: Any) -> tuple[str, str]:
    dag = context.get("dag")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    if dag is None or run_id is None:
        raise AirflowException("Airflow context is missing 'dag' or 'run_id'")
    return dag.dag_id, str(run_id)


def context_flow_run_reference(context: Any) -> str:
    return flow_run_reference(*dag_and_run_id(context))


def flow_run_params(context: Any) -> dict[str, Any]:
    """Parameters recorded on the flow run."""
    dag_id, run_id = dag_and_run_id(context)
    dag_run = context["dag_run"]
    params: dict[str, Any] = {"dag_id": dag_id, "run_id": run_id}
    for attr in ("run_type", "logical_date", "data_interval_start", "data_interval_end"):
        value = getattr(dag_run, attr, None)
        if value is not None:
            params[attr] = str(value)
    conf = getattr(dag_run, "conf", None)
    if conf:
        params["conf"] = dict(conf)
    return params


def dag_source(dag: Any) -> str:
    """Source of the DAG file; hashes identically to what ``ln.track(path=...)`` stored."""
    return Path(dag.fileloc).read_text()


def task_instance_url(context: Any) -> str | None:
    """Airflow UI URL of the running task instance, recorded as the step run's reference."""
    return getattr(context.get("ti"), "log_url", None)


def dag_run_failed(context: Any, own_task_id: str, log: Any) -> bool:
    """True if any task of this DAG run other than ``own_task_id`` failed."""
    ti = context["ti"]
    dag_id, run_id = dag_and_run_id(context)
    get_task_states = getattr(ti, "get_task_states", None)
    if get_task_states is None:
        log.warning("Task states unavailable; marking flow run completed")
        return False
    states = get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {})
    return any(state in FAILED_STATES for task_id, state in states.items() if task_id != own_task_id)
