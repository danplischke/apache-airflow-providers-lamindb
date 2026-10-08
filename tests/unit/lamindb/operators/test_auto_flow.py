from __future__ import annotations

from airflow.sdk import DAG, TaskGroup, task

from airflow.providers.lamindb.operators import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

INIT, FINISH = "lamindb_flow_init", "lamindb_flow_finish"


def noop(x=None):
    return x


def test_steps_share_auto_created_flow_tasks() -> None:
    with DAG("d", schedule=None) as dag:
        a = task.lamindb_venv(noop, task_id="a")()
        b = task.lamindb_venv(noop, task_id="b")()
        a >> b

    assert set(dag.task_ids) == {"a", "b", INIT, FINISH}
    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    assert type(init) is LaminDBFlowInitOperator
    assert init.is_setup
    assert type(finish) is LaminDBFlowFinishOperator
    assert finish.is_teardown
    assert init.downstream_task_ids == {"a", "b", FINISH}
    assert finish.upstream_task_ids == {"a", "b", INIT}


def test_existing_flow_tasks_are_reused() -> None:
    with DAG("d", schedule=None) as dag:
        init = LaminDBFlowInitOperator(task_id="my_init")
        finish = LaminDBFlowFinishOperator(task_id="my_finish")
        task.lamindb_venv(noop, task_id="a")()

    assert set(dag.task_ids) == {"a", "my_init", "my_finish"}
    assert init.downstream_task_ids == {"a"}
    assert finish.upstream_task_ids == {"a"}


def test_auto_flow_opt_out() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", auto_flow=False)()
        task.lamindb_venv(noop, task_id="b", track=False)()

    assert set(dag.task_ids) == {"a", "b"}


def test_flow_tasks_land_in_root_task_group() -> None:
    with DAG("d", schedule=None) as dag, TaskGroup("group"):
        task.lamindb_venv(noop, task_id="a")()

    assert set(dag.task_ids) == {"group.a", INIT, FINISH}


def test_venv_step_creates_flow_tasks_with_its_settings() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(
            noop,
            task_id="a",
            requirements=["pandas", "lamindb-core==2.9.0"],
            python_version="3.11",
            system_site_packages=False,
            lamindb_conn_id="lamindb_other",
            lamindb_instance="owner/name",
            lamindb_version="2.9.0",
        )()

    for task_id, cls in ((INIT, LaminDBFlowInitOperator), (FINISH, LaminDBFlowFinishOperator)):
        op = dag.get_task(task_id)
        assert type(op) is cls
        assert op.requirements == ["lamindb-core==2.9.0"]
        assert op.python_version == "3.11"
        assert op.system_site_packages is False
        assert op.lamindb_conn_id == "lamindb_other"
        assert op.lamindb_instance == "owner/name"
        assert op.lamindb_version == "2.9.0"


def test_dag_default_args_reach_lamindb_arguments() -> None:
    with DAG("d", schedule=None, default_args={"lamindb_conn_id": None, "lamindb_instance": "a/b"}) as dag:
        task.lamindb_venv(noop, task_id="a")()
        LaminDBFlowFinishOperator(task_id="my_finish")

    for task_id in ("a", INIT, "my_finish"):
        assert dag.get_task(task_id).lamindb_conn_id is None
        assert dag.get_task(task_id).lamindb_instance == "a/b"


def test_first_step_decides_flow_task_settings() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", python_version="3.11")()
        task.lamindb_venv(noop, task_id="b", python_version="3.12")()

    assert dag.get_task(INIT).python_version == "3.11"
    assert dag.get_task(INIT).downstream_task_ids == {"a", "b", FINISH}


def test_flow_tasks_resolve_lamindb_when_they_run() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a")()

    assert dag.get_task(INIT).requirements == []
    assert dag.get_task(INIT).lamindb_conn_id == "lamindb_default"


def test_mapped_steps_are_not_auto_wired() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a").expand(x=[1, 2])

    assert set(dag.task_ids) == {"a"}
    # unmapping on the worker must not add tasks either
    dag.get_task("a").unmap({"op_kwargs": {"x": 1}})
    assert set(dag.task_ids) == {"a"}
