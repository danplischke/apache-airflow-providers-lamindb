"""Worker-side helpers that read the Airflow context. No lamindb required."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException

FAILED_STATES = frozenset({"failed", "upstream_failed"})

MAX_REFERENCE_LENGTH = 255
"""The longest ``Run.reference`` lamindb stores; ``utils.runtime`` enforces it as well."""

DEFAULT_SCOPE = ""
"""``lamindb_scope`` of the default ``LaminDBFlow``. ``None`` means no scope: a standalone step."""

_SCOPE_ID = re.compile(r"[A-Za-z0-9_-]+")
MAX_SCOPE_ID_LENGTH = 64
_RUN_ID_RESERVE = 100
"""Room a flow run reference leaves for the run id when the scope id and DAG id are validated."""


def validate_scope_id(scope_id: str, dag_id: str | None = None) -> str:
    """Return ``scope_id`` if it names a scope, else raise a ``ValueError`` that says why.

    Scope ids become part of task ids and flow run references, so they are restricted to letters,
    digits, ``_`` and ``-``, and must leave room for the run id in the reference.
    """
    if not isinstance(scope_id, str) or not _SCOPE_ID.fullmatch(scope_id):
        raise ValueError(
            f"LaminDBFlow scope_id {scope_id!r} is invalid: use letters, digits, '_' and '-' only, "
            "and at least one character. Omit it for the DAG's default scope."
        )
    if len(scope_id) > MAX_SCOPE_ID_LENGTH:
        raise ValueError(
            f"LaminDBFlow scope_id {scope_id!r} is too long: at most {MAX_SCOPE_ID_LENGTH} characters"
        )
    if dag_id is not None and len(dag_id) + len(scope_id) + 2 + _RUN_ID_RESERVE > MAX_REFERENCE_LENGTH:
        raise ValueError(
            f"LaminDBFlow scope_id {scope_id!r} and DAG id {dag_id!r} are too long for the flow run "
            f"reference '<dag_id>/<run_id>/<scope_id>', which lamindb stores in {MAX_REFERENCE_LENGTH} "
            "characters. Use a shorter scope_id."
        )
    return scope_id


def flow_task_ids(scope: str | None) -> tuple[str, str]:
    """Task ids of the flow init and finish operators of ``scope`` (``DEFAULT_SCOPE`` or a scope id)."""
    if not scope:
        return "lamindb_flow_init", "lamindb_flow_finish"
    return f"lamindb_flow_init_{scope}", f"lamindb_flow_finish_{scope}"


def bounded_reference(reference: str) -> str:
    """``reference`` if lamindb can store it, else a prefix plus a digest of all of it.

    Deterministic, so every task of a DAG run derives the same value, and distinct references stay
    distinct.
    """
    if len(reference) <= MAX_REFERENCE_LENGTH:
        return reference
    digest = hashlib.sha256(reference.encode()).hexdigest()[:32]
    return f"{reference[: MAX_REFERENCE_LENGTH - len(digest) - 1]}~{digest}"


def flow_run_reference(dag_id: str, run_id: str, scope: str | None = None) -> str:
    """Reference stored on the flow run. ``run_id`` alone is not unique across DAGs.

    The default scope is ``<dag_id>/<run_id>``; a scope with an id appends it, so scopes of one DAG
    run have flow runs of their own.
    """
    reference = f"{dag_id}/{run_id}/{scope}" if scope else f"{dag_id}/{run_id}"
    return bounded_reference(reference)


def dag_and_run_id(context: Any) -> tuple[str, str]:
    dag = context.get("dag")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    if dag is None or run_id is None:
        raise AirflowException("Airflow context is missing 'dag' or 'run_id'")
    return dag.dag_id, str(run_id)


def context_flow_run_reference(context: Any, scope: str | None = None) -> str:
    return flow_run_reference(*dag_and_run_id(context), scope)


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
    """Source of the DAG file; hashes identically to what ``ln.track(path=...)`` stored.

    Read as bytes: ``Path.read_text`` translates ``\\r\\n``, which lamindb's file hash does not.
    """
    return Path(dag.fileloc).read_bytes().decode("utf-8")


def task_instance_reference(context: Any) -> str | None:
    """Identity of the running task attempt, recorded as the step run's reference.

    The Airflow UI URL of the task instance (it names the DAG, run, task, map index and attempt) if
    lamindb can store it, else the same parts as ``<dag_id>/<run_id>/<task_id>/<map_index>/<try>``.
    Never contains the DAG run's conf.
    """
    ti = context.get("ti")
    url = getattr(ti, "log_url", None)
    if isinstance(url, str) and url and len(url) <= MAX_REFERENCE_LENGTH:
        return url
    try:
        dag_id, run_id = dag_and_run_id(context)
    except AirflowException:
        return None
    task_id = getattr(ti, "task_id", None)
    if not isinstance(task_id, str):
        return None
    map_index = getattr(ti, "map_index", None)
    try_number = getattr(ti, "try_number", None)
    return bounded_reference(
        f"{dag_id}/{run_id}/{task_id}/{map_index if isinstance(map_index, int) else -1}/"
        f"{try_number if isinstance(try_number, int) else 0}"
    )


def dag_run_failed(context: Any, own_task_id: str, log: Any, task_ids: Sequence[str] | None = None) -> bool:
    """True if a task of this DAG run failed.

    With ``task_ids``, only those tasks count, including all their map indexes (a scope's
    members); without, every task other than ``own_task_id`` does.
    """
    ti = context["ti"]
    dag_id, run_id = dag_and_run_id(context)
    get_task_states = getattr(ti, "get_task_states", None)
    if get_task_states is None:
        log.warning("Task states unavailable; marking flow run completed")
        return False
    if task_ids is not None:
        if not task_ids:
            return False
        # filtered by task id on the server; mapped tasks come back once per map index
        states = get_task_states(dag_id=dag_id, run_ids=[run_id], task_ids=list(task_ids)).get(run_id, {})
        return any(state in FAILED_STATES for state in states.values())
    states = get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {})
    return any(state in FAILED_STATES for task_id, state in states.items() if task_id != own_task_id)
