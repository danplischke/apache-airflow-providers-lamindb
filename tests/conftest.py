from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest

# Keep Airflow from writing to ~/airflow when the tests import it.
if "AIRFLOW_HOME" not in os.environ:
    _airflow_home = tempfile.mkdtemp(prefix="airflow-home-")
    atexit.register(shutil.rmtree, _airflow_home, ignore_errors=True)
    os.environ["AIRFLOW_HOME"] = _airflow_home
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


@pytest.fixture
def make_context() -> Callable[..., dict[str, Any]]:
    """Build a minimal Airflow task context for DAG ``my_dag``."""

    def _make(
        run_id: str = "run_1", task_states: dict[str, str] | None = None, fileloc: str = "/dags/my_dag.py"
    ):
        ti = MagicMock(log_url="http://airflow/dags/my_dag/runs/run_1/tasks/t")
        ti.get_task_states = MagicMock(return_value={run_id: task_states or {}})
        dag_run = MagicMock(run_id=run_id, conf={"x": 1}, run_type="manual")
        dag_run.logical_date = dag_run.data_interval_start = dag_run.data_interval_end = None
        return {
            "dag": MagicMock(dag_id="my_dag", fileloc=fileloc),
            "dag_run": dag_run,
            "run_id": run_id,
            "ti": ti,
        }

    return _make


class _FakeRunContext:
    """Mirrors ``ln.context``: ``run`` is a read-only view of ``_run``."""

    _run: Any = None

    @property
    def run(self) -> Any:
        return self._run


@pytest.fixture
def fake_lamindb(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """A mocked ``lamindb`` module, so tests never touch a real instance.

    ``ln.track`` sets a new mock run as ``ln.context.run``. The flow run lookup
    returns ``fake_lamindb.flow_run`` (None until a test sets it).
    """
    fake = MagicMock()
    fake.context = _FakeRunContext()
    fake.flow_run = None

    def track(**kwargs: Any) -> None:
        run = MagicMock()
        run.transform.hash = "hash"
        fake.context._run = run

    fake.track.side_effect = track
    fake.Run.filter.return_value.order_by.return_value.first.side_effect = lambda: fake.flow_run
    monkeypatch.setitem(sys.modules, "lamindb", fake)
    return fake
