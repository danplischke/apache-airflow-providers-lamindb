from __future__ import annotations

from airflow.sdk import DAG, TaskGroup, task

from airflow.providers.lamindb.operators import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

INIT, FINISH = "lamindb_flow_init", "lamindb_flow_finish"


def noop(x=None):
    return x


def pair(x=None, y=None):
    return x, y


def test_steps_are_not_auto_wired_by_default() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a")()
        task.lamindb_venv(noop, task_id="b", track=False)()

    assert set(dag.task_ids) == {"a", "b"}


def test_setup_teardown_context_wires_only_roots_and_leaves() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator() as scope:
        a = task.lamindb_venv(noop, task_id="a")()
        b = task.lamindb_venv(noop, task_id="b")()
        c = task.lamindb_venv(noop, task_id="c")()
        plain = task(noop, task_id="plain")()
        mapped = task.lamindb_venv(noop, task_id="m").expand(x=[1, 2])
        scope.add_task(mapped)  # Airflow only tracks unmapped tasks in the context
        a >> plain >> b
        a >> c >> mapped

    assert dag.get_task(INIT).downstream_task_ids == {"a", FINISH}
    assert dag.get_task(FINISH).upstream_task_ids == {"b", "m", INIT}


def test_steps_share_auto_created_flow_tasks() -> None:
    with DAG("d", schedule=None) as dag:
        a = task.lamindb_venv(noop, task_id="a", auto_flow=True)()
        b = task.lamindb_venv(noop, task_id="b", auto_flow=True)()
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
        task.lamindb_venv(noop, task_id="a", auto_flow=True)()

    assert set(dag.task_ids) == {"a", "my_init", "my_finish"}
    assert init.downstream_task_ids == {"a"}
    assert finish.upstream_task_ids == {"a"}


def test_track_false_is_never_auto_wired() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", auto_flow=True, track=False)()

    assert set(dag.task_ids) == {"a"}


def test_flow_tasks_land_in_root_task_group() -> None:
    with DAG("d", schedule=None) as dag, TaskGroup("group"):
        task.lamindb_venv(noop, task_id="a", auto_flow=True)()

    assert set(dag.task_ids) == {"group.a", INIT, FINISH}


def test_venv_step_creates_flow_tasks_with_its_settings() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(
            noop,
            task_id="a",
            auto_flow=True,
            requirements=["pandas", "lamindb-core==2.11.0"],
            python_version="3.11",
            system_site_packages=False,
            lamindb_conn_id="lamindb_other",
            lamindb_instance="owner/name",
            lamindb_version="2.11.0",
        )()

    for task_id, cls in ((INIT, LaminDBFlowInitOperator), (FINISH, LaminDBFlowFinishOperator)):
        op = dag.get_task(task_id)
        assert type(op) is cls
        assert op.requirements == ["lamindb-core==2.11.0"]
        assert op.python_version == "3.11"
        assert op.system_site_packages is False
        assert op.lamindb_conn_id == "lamindb_other"
        assert op.lamindb_instance == "owner/name"
        assert op.lamindb_version == "2.11.0"


def test_dag_default_args_reach_lamindb_arguments() -> None:
    with DAG("d", schedule=None, default_args={"lamindb_conn_id": None, "lamindb_instance": "a/b"}) as dag:
        task.lamindb_venv(noop, task_id="a", auto_flow=True)()
        LaminDBFlowFinishOperator(task_id="my_finish")

    for task_id in ("a", INIT, "my_finish"):
        assert dag.get_task(task_id).lamindb_conn_id is None
        assert dag.get_task(task_id).lamindb_instance == "a/b"


def test_first_step_decides_flow_task_settings() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", python_version="3.11", auto_flow=True)()
        task.lamindb_venv(noop, task_id="b", python_version="3.12", auto_flow=True)()

    assert dag.get_task(INIT).python_version == "3.11"
    assert dag.get_task(INIT).downstream_task_ids == {"a", "b", FINISH}


def test_flow_tasks_resolve_lamindb_when_they_run() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="a", auto_flow=True)()

    assert dag.get_task(INIT).requirements == []
    assert dag.get_task(INIT).lamindb_conn_id == "lamindb_default"


def test_mapped_steps_are_auto_wired() -> None:
    with DAG("d", schedule=None) as dag:
        produce = task(noop, task_id="produce")()
        task.lamindb_venv(noop, task_id="m", auto_flow=True, system_site_packages=True).expand(x=produce)
        with TaskGroup("group"):
            task.lamindb_venv(noop, task_id="k", auto_flow=True).expand_kwargs([{"x": 1}, {"x": 2}])
        task.lamindb_venv(pair, task_id="partial", auto_flow=True).partial(x=1).expand(y=[1])

    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    steps = {"m", "group.k", "partial"}
    assert init.downstream_task_ids == steps | {FINISH}  # one edge each: finish waits for every map index
    assert finish.upstream_task_ids == steps | {INIT}
    assert dag.get_task("m").is_mapped
    assert init.system_site_packages is True  # settings of the first step, as for unmapped steps
    assert dag.get_task("produce").upstream_task_ids == set()  # plain tasks stay outside the flow
    dag.check_cycle()


def test_mapped_steps_without_auto_flow_or_tracking_are_not_wired() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="m").expand(x=[1, 2])
        task.lamindb_venv(noop, task_id="untracked", auto_flow=True, track=False).expand(x=[1])
        task.lamindb_venv(noop, task_id="connected", auto_flow=True, track=False, connect=True).expand(x=[1])

    assert set(dag.task_ids) == {"m", "untracked", "connected"}


def test_mapped_steps_take_auto_flow_from_default_args() -> None:
    with DAG("d", schedule=None, default_args={"auto_flow": True, "lamindb_space": "lab"}) as dag:
        task.lamindb_venv(noop, task_id="m").expand(x=[1, 2])

    assert dag.get_task(INIT).downstream_task_ids == {"m", FINISH}
    assert dag.get_task(INIT).lamindb_space == "lab"


def test_mapped_and_unmapped_steps_share_the_flow_tasks() -> None:
    with (
        DAG("d", schedule=None) as dag,
        LaminDBFlowInitOperator(task_id="start") >> LaminDBFlowFinishOperator(task_id="end"),
    ):
        task.lamindb_venv(noop, task_id="a", auto_flow=True)()
        task.lamindb_venv(noop, task_id="m", auto_flow=True).expand(x=[1, 2])

    assert set(dag.task_ids) == {"start", "end", "a", "m"}
    assert dag.get_task("start").downstream_task_ids == {"a", "m", "end"}
    assert dag.get_task("end").upstream_task_ids == {"a", "m", "start"}
    dag.check_cycle()


def test_unmapping_on_the_worker_adds_no_tasks() -> None:
    with DAG("d", schedule=None) as dag:
        task.lamindb_venv(noop, task_id="m", auto_flow=True).expand(x=[1, 2])

    before = set(dag.task_ids)
    dag.get_task("m").unmap({"op_kwargs": {"x": 1}})
    assert set(dag.task_ids) == before == {"m", INIT, FINISH}


def test_partial_and_override_keep_the_lamindb_decorator() -> None:
    from airflow.providers.lamindb.decorators.base import LaminDBTaskDecorator

    decorated = task.lamindb_venv(noop, auto_flow=True)
    assert isinstance(decorated, LaminDBTaskDecorator)
    assert isinstance(decorated.partial(x=1), LaminDBTaskDecorator)
    assert isinstance(decorated.override(task_id="other"), LaminDBTaskDecorator)
