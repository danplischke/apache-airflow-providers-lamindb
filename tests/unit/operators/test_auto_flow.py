from __future__ import annotations

from importlib.metadata import version

from airflow.sdk import DAG, TaskGroup, task

from lamindb_airflow.operators import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
    LaminDBStepOperator,
    LaminDBVenvFlowFinishOperator,
    LaminDBVenvFlowInitOperator,
)

INIT, FINISH = "lamindb_flow_init", "lamindb_flow_finish"


def noop(x=None):
    return x


def test_steps_share_auto_created_flow_tasks() -> None:
    with DAG("d", schedule=None) as dag:
        a = LaminDBStepOperator(task_id="a", python_callable=noop)
        b = LaminDBStepOperator(task_id="b", python_callable=noop)
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
        LaminDBStepOperator(task_id="a", python_callable=noop)

    assert set(dag.task_ids) == {"a", "my_init", "my_finish"}
    assert init.downstream_task_ids == {"a"}
    assert finish.upstream_task_ids == {"a"}


def test_auto_flow_opt_out() -> None:
    with DAG("d", schedule=None) as dag:
        LaminDBStepOperator(task_id="a", python_callable=noop, auto_flow=False)
        task.lamindb(noop, task_id="b", auto_flow=False)()
        task.lamindb_venv(noop, task_id="c", auto_flow=False)()

    assert set(dag.task_ids) == {"a", "b", "c"}


def test_untracked_steps_add_no_flow_tasks() -> None:
    with DAG("d", schedule=None) as dag:
        LaminDBStepOperator(task_id="a", python_callable=noop, track=False)
        task.lamindb(noop, task_id="b", track=False)()
        task.lamindb_venv(noop, task_id="c", track=False)()

    assert set(dag.task_ids) == {"a", "b", "c"}


def test_untracked_step_does_not_decide_flow_task_kind() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", track=False)()
        LaminDBStepOperator(task_id="b", python_callable=noop)

    assert type(dag.get_task(INIT)) is LaminDBFlowInitOperator
    assert dag.get_task(INIT).downstream_task_ids == {"b", FINISH}


def test_flow_tasks_land_in_root_task_group() -> None:
    with DAG("d", schedule=None) as dag, TaskGroup("group"):
        LaminDBStepOperator(task_id="a", python_callable=noop)

    assert set(dag.task_ids) == {"group.a", INIT, FINISH}


def test_venv_step_creates_venv_flow_tasks_with_its_settings() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(
            noop,
            task_id="a",
            requirements=["pandas"],
            python_version="3.11",
            system_site_packages=False,
            lamindb_instance="owner/name",
            lamindb_version="1.0",
        )()

    for task_id, cls in ((INIT, LaminDBVenvFlowInitOperator), (FINISH, LaminDBVenvFlowFinishOperator)):
        op = dag.get_task(task_id)
        assert type(op) is cls
        assert op.requirements == ["lamindb==1.0"]
        assert op.python_version == "3.11"
        assert op.system_site_packages is False
        assert op.lamindb_instance == "owner/name"


def test_first_step_decides_flow_task_kind() -> None:
    with DAG("d", schedule=None) as dag:
        LaminDBStepOperator(task_id="a", python_callable=noop)
        task.lamindb_venv(noop, task_id="b")()

    assert type(dag.get_task(INIT)) is LaminDBFlowInitOperator
    assert dag.get_task(INIT).downstream_task_ids == {"a", "b", FINISH}


def test_venv_flow_tasks_pin_worker_lamindb_by_default() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a")()

    assert dag.get_task(INIT).requirements == [f"lamindb=={version('lamindb')}"]


def test_mapped_steps_are_not_auto_wired() -> None:
    with DAG("d", schedule=None) as dag:
        LaminDBStepOperator.partial(task_id="a", python_callable=noop).expand(op_args=[[1], [2]])
        task.lamindb(noop, task_id="b").expand(x=[1, 2])

    assert set(dag.task_ids) == {"a", "b"}
    # unmapping on the worker must not add tasks either
    dag.get_task("a").unmap({"op_args": [1]})
    assert set(dag.task_ids) == {"a", "b"}
