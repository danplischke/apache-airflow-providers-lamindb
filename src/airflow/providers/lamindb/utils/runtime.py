"""LaminDB run bookkeeping shared by the worker and remote interpreters.

This module is shipped *as source* to virtualenvs and pods, which may have neither
Airflow nor this package installed. Keep it self-contained: standard library and
``lamindb`` only, imported inside functions, so the worker can import it without
lamindb.

Mapping:

- one Airflow DAG run  -> one LaminDB flow run (a ``Run`` of the DAG file's ``Transform``)
- one Airflow task run -> one LaminDB step run (``initiated_by_run`` = the flow run)

The flow run is identified by ``reference="<dag_id>/<run_id>"`` and
``reference_type="airflow_dag_run"``, so any task can look it up without XCom.
"""

from datetime import datetime, timezone

FLOW_RUN_REFERENCE_TYPE = "airflow_dag_run"
STEP_RUN_REFERENCE_TYPE = "airflow_task_instance"
MAX_REFERENCE_LENGTH = 255

STATUS_STARTED = -1
STATUS_RESTARTED = -2
STATUS_COMPLETED = 0
STATUS_ERRORED = 1


def connect(instance=None):
    """Import lamindb, connected to ``instance`` if given.

    Credentials and the default instance follow lamindb's own configuration
    (``LAMIN_API_KEY``, ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``).
    """
    import lamindb as ln

    if instance:
        ln.connect(instance)
    return ln


def get_flow_run(reference):
    """Return the flow run tagged with ``reference``, or None if it was never started."""
    import lamindb as ln

    return (
        ln.Run.filter(reference=reference, reference_type=FLOW_RUN_REFERENCE_TYPE)
        .order_by("-created_at")
        .first()
    )


def require_flow_run(reference):
    flow_run = get_flow_run(reference)
    if flow_run is None:
        raise LookupError(
            f"No LaminDB flow run found for DAG run {reference!r}. "
            "Add a LaminDB flow init operator upstream of every LaminDB step."
        )
    return flow_run


def finish_run(run, success):
    """Close a run the same way lamindb's own decorators do."""
    run.finished_at = datetime.now(timezone.utc)
    run._status_code = STATUS_COMPLETED if success else STATUS_ERRORED
    run.save()


def start_flow_run(reference, path, entrypoint, params, instance=None):
    """Start (or restart, on retry) the flow run for a DAG run; return it.

    Uses ``ln.track`` on the DAG file so the flow run belongs to a versioned
    ``Transform`` of the DAG source. Idempotent per DAG run: a retry of the init
    task restarts the existing run instead of creating a second one.
    """
    from pathlib import Path

    ln = connect(instance)
    existing = get_flow_run(reference)
    if existing is not None:
        existing.started_at = datetime.now(timezone.utc)
        existing.finished_at = None
        existing._status_code = STATUS_RESTARTED
        existing.save()
        return existing

    if ln.context.run is not None:
        raise RuntimeError(
            "LaminDB global run context is already set in this process; "
            "cannot start a flow run. Call ln.finish() first."
        )
    try:
        ln.track(path=path, entrypoint=entrypoint, params=params, new_run=True, stream_tracking=False)
        run = ln.context.run
    finally:
        # leave the process clean for in-process runners like dag.test()
        ln.context._run = None
    if run is None:
        raise RuntimeError("ln.track() did not create a run (read-only connection?)")
    transform = run.transform
    if transform.hash is None:
        # lamindb persists source code at ln.finish(); do it now so steps resolve
        # this transform by hash and DAG edits bump its version.
        transform._update_source_code_from_path(Path(path))
        transform.save()
    run.reference = reference
    run.reference_type = FLOW_RUN_REFERENCE_TYPE
    run.save()
    return run


def finish_flow_run(reference, success, instance=None):
    """Close the flow run of a DAG run; return it."""
    connect(instance)
    flow_run = require_flow_run(reference)
    finish_run(flow_run, success)
    return flow_run


def run_step(fn, args, kwargs, flow_run_reference, source_code, step_reference=None, instance=None):
    """Run ``fn`` as a step of the flow run, via ``ln.track`` with ``initiated_by_run``.

    Passing the DAG source with the flow transform's key makes lamindb resolve the
    same ``Transform`` by hash as the flow run, so no pickled state is needed.
    """
    import inspect

    ln = connect(instance)
    flow_run = require_flow_run(flow_run_reference)
    bound = inspect.signature(fn).bind(*args, **kwargs)
    bound.apply_defaults()
    ln.track(
        key=flow_run.transform.key,
        source_code=source_code,
        kind="script",
        entrypoint=fn.__name__,
        params=dict(bound.arguments),
        initiated_by_run=flow_run,
        new_run=True,
        stream_tracking=False,
    )
    run = ln.context.run
    if step_reference and len(step_reference) <= MAX_REFERENCE_LENGTH:
        run.reference = step_reference
        run.reference_type = STEP_RUN_REFERENCE_TYPE
    try:
        result = fn(*args, **kwargs)
    except BaseException:
        finish_run(run, False)
        raise
    finish_run(run, True)
    ln.context._run = None
    return result
