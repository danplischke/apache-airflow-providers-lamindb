"""In-process LaminDB helpers. These need ``lamindb`` installed on the worker."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

from airflow.providers.lamindb.utils import runtime
from airflow.providers.lamindb.utils.dag_run import context_flow_run_reference, dag_and_run_id, flow_run_params

if TYPE_CHECKING:
    from lamindb import Run


def require_lamindb() -> None:
    """Fail with an actionable message when an in-process operator runs without lamindb."""
    try:
        import lamindb  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "This operator runs LaminDB in the worker process and needs lamindb installed there: "
            'pip install "lamindb-airflow[lamindb]". To keep lamindb off the worker, use '
            "LaminDBVenvFlowInitOperator, LaminDBVenvFlowFinishOperator and @task.lamindb_venv "
            "or @task.lamindb_k8s instead."
        ) from e


def require_flow_run(context: Any) -> Run:
    """Resolve the flow run for the current task or raise a clear error."""
    try:
        return runtime.require_flow_run(context_flow_run_reference(context))
    except LookupError as e:
        raise AirflowException(str(e)) from e


def start_flow_run(context: Any) -> Run:
    """Start (or restart, on retry) the flow run for the current DAG run."""
    dag_id, _ = dag_and_run_id(context)
    try:
        return runtime.start_flow_run(
            reference=context_flow_run_reference(context),
            path=context["dag"].fileloc,
            entrypoint=dag_id,
            params=flow_run_params(context),
        )
    except RuntimeError as e:
        raise AirflowException(str(e)) from e


@contextmanager
def flow_run_context(flow_run: Run) -> Iterator[None]:
    """Make ``flow_run`` the global lamindb run so ``@ln.step`` attaches to it.

    ``@ln.step`` requires the *global* context (``ln.context.run``); a contextvar is
    not enough. Airflow runs each task in its own process, so using the global
    context is safe. Restored on exit.
    """
    import lamindb as ln

    current = ln.context.run
    if current is not None and current.uid != flow_run.uid:
        raise AirflowException(
            f"LaminDB global run context is already set to Run({current.uid!r}); refusing to overwrite it."
        )
    ln.context._run = flow_run
    try:
        yield
    finally:
        ln.context._run = current


def is_lamindb_tracked(fn: Callable[..., Any]) -> bool:
    """True if ``fn`` is already wrapped by ``@ln.step`` / ``@ln.flow``."""
    code = getattr(fn, "__code__", None)
    return (
        getattr(fn, "__wrapped__", None) is not None
        and code is not None
        and code.co_name == "wrapper_tracked"
        and "lamindb" in code.co_filename
    )


def untracked(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return ``fn`` without its ``@ln.step`` / ``@ln.flow`` wrapper, if it has one."""
    return fn.__wrapped__ if is_lamindb_tracked(fn) else fn  # type: ignore[attr-defined]


def as_lamindb_step(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return ``fn`` as a LaminDB step, wrapping with ``ln.step()`` unless already tracked."""
    import lamindb as ln

    return fn if is_lamindb_tracked(fn) else ln.step()(fn)


def run_as_step(fn: Callable[..., Any], flow_run: Run, *args: Any, **kwargs: Any) -> Any:
    """Run ``fn`` as a step of ``flow_run`` in this process."""
    with flow_run_context(flow_run):
        return as_lamindb_step(fn)(*args, **kwargs)
