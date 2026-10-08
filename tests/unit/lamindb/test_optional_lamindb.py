"""The package must work on a worker without lamindb: LaminDB only runs in virtualenvs and pods."""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import MagicMock

import pytest

from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator
from airflow.providers.lamindb.utils import remote


@pytest.fixture
def no_lamindb(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("lamindb", "lamindb_setup"):
        monkeypatch.setitem(sys.modules, name, None)  # makes `import lamindb` raise ImportError


def test_import_does_not_need_lamindb() -> None:
    code = (
        "import sys\n"
        "sys.modules['lamindb'] = sys.modules['lamindb_setup'] = None\n"
        "import airflow.providers.lamindb, airflow.providers.lamindb.get_provider_info\n"
        "import airflow.providers.lamindb.decorators.python_virtualenv\n"
        "import airflow.providers.lamindb.decorators.kubernetes\n"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


@pytest.mark.usefixtures("lamindb_connection")
def test_venv_operator_works_without_lamindb_on_worker(no_lamindb: None, make_context) -> None:
    op = LaminDBFlowInitOperator()

    captured = {}

    def fake_venv_execute(self, context):
        captured["source"] = self.get_python_source()
        captured["op_kwargs"] = self.op_kwargs

    from airflow.providers.standard.operators.python import PythonVirtualenvOperator

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(PythonVirtualenvOperator, "execute", fake_venv_execute)
        op.execute(make_context())

    assert captured["op_kwargs"]["lamindb_airflow_config"]["instance"] == "owner/name"
    assert "def lamindb_airflow_flow_task(lamindb_airflow_config):" in captured["source"]
    assert op.requirements[0] == "lamindb-core==2.10.0"


def test_venv_step_works_without_lamindb_on_worker(no_lamindb: None, make_context) -> None:
    from airflow.sdk import dag, task

    @dag
    def test_dag():
        @task.lamindb_venv(lamindb_conn_id=None)
        def step():
            return 1

        step()

    op = test_dag().get_task("step")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(remote, "dag_source", MagicMock(return_value="# dag"))
        mp.setattr(type(op).__mro__[2], "execute", lambda self, context: self.get_python_source())
        source = op.execute(make_context())
    assert "'instance': None" in source
    assert op.requirements[0] == "lamindb-core"
