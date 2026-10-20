from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.operators.flow import (
    FLOW_INSTANCE_XCOM_KEY,
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)
from airflow.providers.lamindb.utils.remote import RemoteLaminDB

CORE = ["lamindb-core==2.11.0", "numpy", "pandas>=2.0.0", "pandera>=0.24.0"]


def test_flow_operators_are_setup_and_teardown() -> None:
    init, finish = LaminDBFlowInitOperator(), LaminDBFlowFinishOperator()
    assert (init.task_id, finish.task_id) == ("lamindb_flow_init", "lamindb_flow_finish")
    assert init.is_setup
    assert not init.is_teardown
    assert finish.is_teardown
    assert finish.trigger_rule == "all_done_setup_success"
    plain = LaminDBFlowFinishOperator(is_teardown=False)
    assert not plain.is_teardown
    assert plain.trigger_rule == "all_done"
    assert not LaminDBFlowInitOperator(is_setup=False).is_setup


def test_requirements_are_resolved_at_execute_time() -> None:
    op = LaminDBFlowInitOperator(requirements=["pandas"])
    assert op.requirements == ["pandas"]  # no lamindb lookup while parsing the DAG
    assert op.expect_airflow is False


def test_lamindb_arguments_are_templated(fake_lamindb: MagicMock, make_context, lamindb_connection) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = LaminDBFlowInitOperator(
        lamindb_conn_id="{{ params.conn_id }}",
        lamindb_instance="{{ params.instance }}",
        lamindb_cache_dir="{{ params.cache_dir }}",
    )
    op.render_template_fields(
        {"params": {"conn_id": "lamindb_default", "instance": "owner/other", "cache_dir": "/shared"}}
    )
    assert (op.lamindb_conn_id, op.lamindb_instance, op.lamindb_cache_dir) == (
        "lamindb_default",
        "owner/other",
        "/shared",
    )

    _, seen = _run_in_fake_venv(op, make_context())
    fake_lamindb.connect.assert_called_once_with("owner/other")
    assert seen["env_vars"]["LAMIN_CACHE_DIR"] == "/shared"


def _run_in_fake_venv(op, context):
    """Execute ``op`` but, instead of a virtualenv, exec its shipped source here.

    Returns the result and what the virtualenv would have been created with.
    """
    seen: dict = {}

    def fake_venv_execute(self, context):
        seen["requirements"] = list(self.requirements)
        seen["env_vars"] = dict(self.env_vars or {})
        settings_dir = seen["env_vars"].get("LAMIN_SETTINGS_DIR")
        seen["settings_dir_existed"] = settings_dir is not None and os.path.isdir(settings_dir)
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace[self.python_callable.__name__](**self.op_kwargs)

    with patch.object(PythonVirtualenvOperator, "execute", fake_venv_execute):
        return op.execute(context), seen


def test_flow_init_uses_the_connection(fake_lamindb: MagicMock, make_context, lamindb_connection) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")  # restart path: returns the existing run
    op = LaminDBFlowInitOperator()
    context = make_context()

    result, seen = _run_in_fake_venv(op, context)

    assert result == "flowuid"
    fake_lamindb.connect.assert_called_once_with("owner/name")
    config = op.op_kwargs["lamindb_airflow_config"]
    assert config["instance"] == "owner/name"
    assert config["reference"] == "my_dag/run_1"
    assert config["path"] == "/dags/my_dag.py"
    assert config["entrypoint"] == "my_dag"
    assert config["params"]["run_id"] == "run_1"
    assert seen["requirements"] == CORE
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"
    assert seen["settings_dir_existed"]
    assert not os.path.exists(seen["env_vars"]["LAMIN_SETTINGS_DIR"])  # removed afterwards
    assert op.env_vars is None  # restored, the API key stays off the operator
    context["ti"].xcom_push.assert_called_once_with(key=FLOW_INSTANCE_XCOM_KEY, value="owner/name")


@pytest.mark.parametrize(
    ("task_states", "status"),
    [({"a": "success", "lamindb_flow_finish": "running"}, 0), ({"a": "success", "b": "upstream_failed"}, 1)],
)
def test_flow_finish_records_dag_run_outcome(
    fake_lamindb: MagicMock, make_context, lamindb_connection, task_states, status
) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    context = make_context(task_states=task_states)

    assert _run_in_fake_venv(LaminDBFlowFinishOperator(), context)[0] == "flowuid"
    assert flow_run._status_code == status
    context["ti"].get_task_states.assert_called_once_with(dag_id="my_dag", run_ids=["run_1"])


def test_flow_finish_of_a_scope_only_looks_at_its_members(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    # a sibling scope's or standalone task failing is none of this scope's business: the server
    # only returns the member tasks (mapped ones once per map index)
    context = make_context(task_states={"a": "success", "m_0": "success", "m_1": "success"})
    op = LaminDBFlowFinishOperator(lamindb_scope="enrich", lamindb_members=["a", "m"])

    _run_in_fake_venv(op, context)

    context["ti"].get_task_states.assert_called_once_with(
        dag_id="my_dag", run_ids=["run_1"], task_ids=["a", "m"]
    )
    assert op.op_kwargs["lamindb_airflow_config"]["reference"] == "my_dag/run_1/enrich"
    assert flow_run._status_code == 0

    context = make_context(task_states={"a": "success", "m_0": "success", "m_1": "failed"})
    _run_in_fake_venv(LaminDBFlowFinishOperator(lamindb_scope="enrich", lamindb_members=["a", "m"]), context)
    assert flow_run._status_code == 1


def test_flow_finish_of_an_empty_scope_completes(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    context = make_context(task_states={"x": "failed"})

    _run_in_fake_venv(LaminDBFlowFinishOperator(lamindb_members=[]), context)

    context["ti"].get_task_states.assert_not_called()
    assert flow_run._status_code == 0


def test_flow_init_of_a_scope_tags_its_flow_run_with_the_scope(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = LaminDBFlowInitOperator(lamindb_scope="enrich")

    _run_in_fake_venv(op, make_context())

    config = op.op_kwargs["lamindb_airflow_config"]
    assert config["reference"] == "my_dag/run_1/enrich"
    assert config["entrypoint"] == "my_dag"
    assert fake_lamindb.Run.filter.call_args.kwargs["reference"] == "my_dag/run_1/enrich"


def test_flow_finish_without_flow_run_raises(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    with pytest.raises(LookupError, match="No LaminDB flow run"):
        _run_in_fake_venv(LaminDBFlowFinishOperator(), make_context())


def test_explicit_instance_overrides_the_connection(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    op = LaminDBFlowFinishOperator(lamindb_instance="owner/explicit")

    assert _run_in_fake_venv(op, make_context(task_states={"b": "failed"}))[0] == "flowuid"
    fake_lamindb.connect.assert_called_once_with("owner/explicit")
    assert op.op_kwargs == {
        "lamindb_airflow_config": {
            "instance": "owner/explicit",
            "reference": "my_dag/run_1",
            "success": False,
        }
    }
    assert flow_run._status_code == 1


def test_explicit_env_vars_take_precedence(fake_lamindb: MagicMock, make_context, lamindb_connection) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = LaminDBFlowFinishOperator(env_vars={"LAMIN_API_KEY": "other-key"})

    _, seen = _run_in_fake_venv(op, make_context())
    assert seen["env_vars"]["LAMIN_API_KEY"] == "other-key"
    assert "LAMIN_SETTINGS_DIR" in seen["env_vars"]
    assert op.env_vars == {"LAMIN_API_KEY": "other-key"}


def test_without_connection_lamindb_uses_its_own_configuration(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = LaminDBFlowInitOperator(lamindb_conn_id=None)
    context = make_context()

    _, seen = _run_in_fake_venv(op, context)
    fake_lamindb.connect.assert_not_called()
    assert seen["env_vars"] == {}
    # nothing to ask LaminHub: any supported version
    assert seen["requirements"] == ["lamindb-core>=2.8.1", *CORE[1:]]
    context["ti"].xcom_push.assert_not_called()


def test_missing_connection_explains_how_to_opt_out(make_context, monkeypatch) -> None:
    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    with pytest.raises(AirflowNotFoundException, match="lamindb_conn_id=None"):
        LaminDBFlowInitOperator().execute(make_context())


@pytest.mark.parametrize(
    ("kwargs", "expected", "looked_up"),
    [
        ({}, ["lamindb-core==2.12.0", *CORE[1:]], True),
        ({"lamindb_version": "2.11.2"}, ["lamindb-core==2.11.2", *CORE[1:]], False),
        ({"requirements": ["lamindb>=2.11"]}, ["lamindb>=2.11"], False),
        ({"requirements": ["lamindb-core==2.11.2"]}, ["lamindb-core==2.11.2", *CORE[1:]], False),
        ({"requirements": "pandas\nlamindb==2.11.2"}, ["pandas\nlamindb==2.11.2"], False),
    ],
)
def test_lamindb_version_lookup_only_when_needed(
    fake_lamindb: MagicMock, make_context, lamindb_connection, monkeypatch, kwargs, expected, looked_up
) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    lookup = MagicMock(return_value="2.12.0")
    monkeypatch.setattr(RemoteLaminDB, "instance_lamindb_version", lookup)

    _, seen = _run_in_fake_venv(LaminDBFlowFinishOperator(**kwargs), make_context())
    assert seen["requirements"] == expected
    assert lookup.called is looked_up


@pytest.mark.parametrize("operator", [LaminDBFlowInitOperator, LaminDBFlowFinishOperator])
def test_older_lamindb_version_fails_when_the_dag_is_parsed(operator) -> None:
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.8\.0 is not supported"):
        operator(lamindb_version="2.8.0")


def test_instance_on_older_lamindb_fails_before_the_virtualenv(
    fake_lamindb: MagicMock, make_context, lamindb_connection, monkeypatch
) -> None:
    def lookup(self):
        raise LaminDBVersionError("The LaminDB instance owner/name runs lamindb 2.8.0")

    monkeypatch.setattr(RemoteLaminDB, "instance_lamindb_version", lookup)
    with (
        patch.object(PythonVirtualenvOperator, "execute") as venv_execute,
        pytest.raises(LaminDBVersionError, match=r"runs lamindb 2\.8\.0"),
    ):
        LaminDBFlowInitOperator().execute(make_context())
    venv_execute.assert_not_called()


@pytest.mark.parametrize("module", ["airflow.providers.lamindb", "airflow.providers.lamindb.operators"])
def test_removed_venv_names_do_not_import(module: str) -> None:
    import importlib

    with pytest.raises(AttributeError):
        _ = importlib.import_module(module).LaminDBVenvFlowInitOperator
