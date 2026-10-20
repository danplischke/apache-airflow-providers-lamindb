from __future__ import annotations

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import DAG, TaskGroup, task
from airflow.serialization.serialized_objects import DagSerialization

from airflow.providers.lamindb import LaminDBFlow
from airflow.providers.lamindb.operators import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

INIT, FINISH = "lamindb_flow_init", "lamindb_flow_finish"


def noop(x=None):
    return x


def pair(x=None, y=None):
    return x, y


def venv(task_id: str, **kwargs):
    return task.lamindb_venv(noop, task_id=task_id, **kwargs)


def test_steps_are_standalone_by_default() -> None:
    with DAG("d", schedule=None) as dag:
        venv("a")()
        venv("b", track=False)()
        venv("m").expand(x=[1, 2])

    assert set(dag.task_ids) == {"a", "b", "m"}  # no flow tasks are added
    assert dag.get_task("a").lamindb_scope is None
    assert dag.get_task("m").partial_kwargs.get("lamindb_scope") is None


def test_flow_operators_alone_parent_nothing() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():
        venv("a")()

    assert dag.get_task("a").lamindb_scope is None
    assert dag.get_task(INIT).lamindb_scope == ""  # the operators open the default scope's run, no more


def test_scope_adds_a_wired_setup_teardown_pair() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlow():
        a = venv("a")()
        venv("b")(a)

    assert set(dag.task_ids) == {"a", "b", INIT, FINISH}
    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    assert type(init) is LaminDBFlowInitOperator
    assert init.is_setup
    assert type(finish) is LaminDBFlowFinishOperator
    assert finish.is_teardown
    assert init.downstream_task_ids == {"a", FINISH}
    assert finish.upstream_task_ids == {"b", INIT}
    assert dag.get_task("a").lamindb_scope == dag.get_task("b").lamindb_scope == ""


def test_only_tasks_created_in_the_block_are_members() -> None:
    with DAG("d", schedule=None) as dag:
        outside = venv("outside")()
        with LaminDBFlow():
            venv("inside")(outside)
        after = venv("after")()
        after >> dag.get_task("inside")  # wired in later: still not a member

    assert dag.get_task("outside").lamindb_scope is None
    assert dag.get_task("after").lamindb_scope is None
    assert dag.get_task("inside").lamindb_scope == ""
    assert dag.get_task(INIT).downstream_task_ids == {"inside", FINISH}
    assert dag.get_task(FINISH).lamindb_members == ["inside"]
    assert "outside" not in dag.get_task(FINISH).upstream_task_ids


def test_pre_decorated_callables_are_members_when_called_in_the_block() -> None:
    decorated = task.lamindb_venv(noop)
    with DAG("d", schedule=None) as dag:
        decorated.override(task_id="alone")()
        with LaminDBFlow():
            decorated.override(task_id="scoped")()

    assert dag.get_task("alone").lamindb_scope is None
    assert dag.get_task("scoped").lamindb_scope == ""


def test_plain_and_untracked_tasks_are_in_the_lifecycle_but_not_tracked() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlow():
        a = venv("a")()
        plain = task(noop, task_id="plain")(a)
        untracked = venv("untracked", track=False)(plain)
        venv("b")(untracked)

    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    assert init.downstream_task_ids == {"a", FINISH}
    assert finish.upstream_task_ids == {"b", INIT}
    assert finish.lamindb_members == [
        "a",
        "plain",
        "untracked",
        "b",
    ]  # a plain failure fails the flow run too
    assert dag.get_task("a").lamindb_scope == dag.get_task("b").lamindb_scope == ""
    assert dag.get_task("untracked").lamindb_scope is None  # nothing is recorded, so no parent
    assert not hasattr(dag.get_task("plain"), "lamindb_scope")


def test_roots_and_leaves_only_look_at_members() -> None:
    with DAG("d", schedule=None) as dag:
        outside = task(noop, task_id="outside")()
        with LaminDBFlow():
            a = venv("a")(outside)
            b = venv("b")(a)
            c = venv("c")(a)
            pair_op = task(pair, task_id="join")(b, c)

    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    assert init.downstream_task_ids == {"a", FINISH}  # outside is upstream of a but not a member
    assert finish.upstream_task_ids == {"join", INIT}
    assert pair_op.operator.task_id == "join"
    dag.check_cycle()


def test_mapped_invocations_join_with_no_extra_lifecycle_tasks() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlow():
        produce = task(noop, task_id="produce")()
        venv("m").expand(x=produce)
        with TaskGroup("group"):
            venv("k").expand_kwargs([{"x": 1}, {"x": 2}])
        task.lamindb_venv(pair, task_id="partial").partial(x=1).expand(y=[1])
        venv("override").override(retries=2).expand(x=[1])

    steps = {"m", "group.k", "partial", "override"}
    assert set(dag.task_ids) == steps | {"produce", INIT, FINISH}
    init, finish = dag.get_task(INIT), dag.get_task(FINISH)
    assert init.downstream_task_ids == {"produce", "group.k", "partial", "override", FINISH}
    assert finish.upstream_task_ids == steps | {INIT}
    assert dag.get_task("m").is_mapped
    for step in steps:
        assert dag.get_task(step).partial_kwargs["lamindb_scope"] == ""
    assert finish.lamindb_members == ["produce", "m", "group.k", "partial", "override"]
    dag.check_cycle()


def test_task_groups_keep_their_task_ids_and_the_flow_tasks_the_enclosing_group() -> None:
    with DAG("d", schedule=None) as dag, TaskGroup("outer"), LaminDBFlow(), TaskGroup("inner"):
        venv("a")()

    assert set(dag.task_ids) == {"outer.inner.a", f"outer.{INIT}", f"outer.{FINISH}"}
    assert dag.get_task(f"outer.{INIT}").downstream_task_ids == {"outer.inner.a", f"outer.{FINISH}"}


def test_empty_scope_still_pairs_init_and_finish() -> None:
    with DAG("d", schedule=None) as dag, LaminDBFlow():
        pass

    assert dag.get_task(INIT).downstream_task_ids == {FINISH}
    assert dag.get_task(FINISH).lamindb_members == []


def test_sibling_scopes_need_distinct_ids_and_have_task_ids_of_their_own() -> None:
    with DAG("d", schedule=None) as dag:
        with LaminDBFlow():
            venv("a")()
        with LaminDBFlow("enrich"):
            venv("b")()
        with LaminDBFlow("load-2"):
            venv("c")()

    assert {INIT, FINISH, "lamindb_flow_init_enrich", "lamindb_flow_finish_enrich"} <= set(dag.task_ids)
    assert dag.get_task("a").lamindb_scope == ""
    assert dag.get_task("b").lamindb_scope == "enrich"
    assert dag.get_task("c").lamindb_scope == "load-2"
    assert dag.get_task("lamindb_flow_init_enrich").downstream_task_ids == {"b", "lamindb_flow_finish_enrich"}
    assert dag.get_task("lamindb_flow_finish_enrich").lamindb_members == ["b"]
    assert dag.get_task("lamindb_flow_init_enrich").lamindb_scope == "enrich"


def test_a_second_default_scope_is_rejected() -> None:
    with DAG("d", schedule=None):
        with LaminDBFlow():
            venv("a")()
        with pytest.raises(AirflowException, match=r"already has a default LaminDBFlow.*scope_id"):
            LaminDBFlow().__enter__()


def test_a_reused_scope_id_is_rejected() -> None:
    with DAG("d", schedule=None):
        with LaminDBFlow("enrich"):
            venv("a")()
        with pytest.raises(AirflowException, match="scope_id 'enrich' is already used"):
            LaminDBFlow("enrich").__enter__()


def test_the_same_scope_id_may_be_used_in_another_dag() -> None:
    for dag_id in ("one", "two"):
        with DAG(dag_id, schedule=None) as dag, LaminDBFlow("enrich"):
            venv("a")()
        assert "lamindb_flow_init_enrich" in dag.task_ids


@pytest.mark.parametrize("scope_id", ["", "has space", "dot.ted", "slash/ed", "ü", "x" * 65])
def test_invalid_scope_ids_are_rejected(scope_id) -> None:
    with pytest.raises(ValueError, match="scope_id"):
        LaminDBFlow(scope_id)


def test_scope_ids_that_make_the_reference_too_long_are_rejected_not_truncated() -> None:
    with (
        DAG("d" * 100, schedule=None),
        pytest.raises(ValueError, match="too long for the flow run reference"),
    ):
        LaminDBFlow("s" * 64).__enter__()


def test_nested_scopes_are_rejected_and_the_outer_one_stays_usable() -> None:
    with DAG("d", schedule=None) as dag:
        with LaminDBFlow():
            venv("a")()
            with pytest.raises(AirflowException, match="can't be nested"):
                LaminDBFlow("inner").__enter__()
            venv("b")()
        with LaminDBFlow("later"):
            venv("c")()

    assert dag.get_task(INIT).downstream_task_ids == {"a", "b", FINISH}


def test_a_scope_outside_a_dag_is_rejected() -> None:
    with pytest.raises(AirflowException, match="inside a DAG"):
        LaminDBFlow().__enter__()


def test_reserved_arguments_cannot_be_passed() -> None:
    with pytest.raises(TypeError, match="sets lamindb_scope, task_id itself"):
        LaminDBFlow(task_id="x", lamindb_scope="y")


def test_an_exception_resets_the_scope_and_wires_nothing() -> None:
    def failing_scope() -> None:
        with LaminDBFlow():
            venv("a")()
            raise RuntimeError("boom")

    with DAG("d", schedule=None) as dag:
        with pytest.raises(RuntimeError, match="boom"):
            failing_scope()

        assert set(dag.task_ids) == {"a"}  # not wired, and no lifecycle tasks
        assert dag.get_task("a").lamindb_scope is None
        with LaminDBFlow():  # the failed scope left no state behind
            venv("b")()

    assert dag.get_task("b").lamindb_scope == ""
    assert dag.get_task(INIT).downstream_task_ids == {"b", FINISH}


def test_scope_operators_get_the_scope_arguments_and_nothing_from_the_steps() -> None:
    with (
        DAG("d", schedule=None, default_args={"lamindb_instance": "dag/default"}) as dag,
        LaminDBFlow(
            lamindb_version="2.11.0", lamindb_conn_id=None, python_version="3.11", requirements=["pandas"]
        ),
    ):
        venv("a", lamindb_instance="owner/step", python_version="3.12", lamindb_version="2.11.1")()

    for task_id, cls in ((INIT, LaminDBFlowInitOperator), (FINISH, LaminDBFlowFinishOperator)):
        op = dag.get_task(task_id)
        assert type(op) is cls
        assert op.lamindb_version == "2.11.0"
        assert op.lamindb_conn_id is None
        assert op.python_version == "3.11"
        assert op.requirements == ["pandas"]
        assert op.lamindb_instance == "dag/default"  # DAG default_args, not the step's setting
    assert dag.get_task("a").lamindb_instance == "owner/step"
    assert dag.get_task("a").python_version == "3.12"


def test_scope_operators_are_templated_for_the_scope_and_members() -> None:
    assert {"lamindb_scope"} <= set(LaminDBFlowInitOperator.template_fields)
    assert {"lamindb_scope", "lamindb_members"} <= set(LaminDBFlowFinishOperator.template_fields)
    assert "lamindb_scope" in type(task.lamindb_venv(noop)(noop).operator).template_fields


def test_scope_identity_survives_serialization_and_unmapping() -> None:
    with DAG("d", schedule=None) as dag:
        with LaminDBFlow():
            venv("plain_step")()
            venv("m").expand(x=[1, 2])
        with LaminDBFlow("enrich"):
            task.lamindb_venv(pair, task_id="n").partial(x=1).expand(y=[1])
        venv("alone")()
        venv("alone_mapped").expand(x=[1])

    restored = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))

    assert restored.get_task("plain_step").lamindb_scope == ""
    assert restored.get_task("m").partial_kwargs["lamindb_scope"] == ""
    assert restored.get_task("n").partial_kwargs["lamindb_scope"] == "enrich"
    assert restored.get_task(FINISH).lamindb_members == ["plain_step", "m"]
    assert restored.get_task("lamindb_flow_init_enrich").lamindb_scope == "enrich"
    assert getattr(restored.get_task("alone"), "lamindb_scope", None) is None
    assert "lamindb_scope" not in restored.get_task("alone_mapped").partial_kwargs

    # what the task runner does on the worker: unmap with the scope in the partial kwargs
    before = set(dag.task_ids)
    unmapped = dag.get_task("m").unmap({"op_kwargs": {"x": 1}})
    assert unmapped.lamindb_scope == ""
    assert dag.get_task("n").unmap({"op_kwargs": {"x": 1, "y": 1}}).lamindb_scope == "enrich"
    assert dag.get_task("alone_mapped").unmap({"op_kwargs": {"x": 1}}).lamindb_scope is None
    assert set(dag.task_ids) == before  # unmapping adds no lifecycle tasks


def test_flow_operators_without_a_scope_use_the_default_ids() -> None:
    init, finish = LaminDBFlowInitOperator(), LaminDBFlowFinishOperator()
    assert (init.task_id, finish.task_id) == (INIT, FINISH)
    scoped = LaminDBFlowInitOperator(lamindb_scope="enrich")
    assert scoped.task_id == "lamindb_flow_init_enrich"
    assert LaminDBFlowFinishOperator(lamindb_scope="enrich").task_id == "lamindb_flow_finish_enrich"
    assert LaminDBFlowInitOperator(task_id="mine", lamindb_scope="enrich").task_id == "mine"


def dag_task_operator():
    with DAG("tmp", schedule=None) as tmp:
        task.lamindb_venv(noop, task_id="t")()
    return tmp.get_task("t")


def test_auto_flow_is_rejected_with_a_migration_hint() -> None:
    with pytest.raises(TypeError, match=r"auto_flow was removed.*with LaminDBFlow\(\)"):
        task.lamindb_venv(auto_flow=True)
    with pytest.raises(TypeError, match=r"auto_flow was removed"):
        task.lamindb_venv(auto_flow=False)
    step = dag_task_operator()
    with pytest.raises(TypeError, match=r"auto_flow was removed"):
        type(step)(task_id="b", python_callable=noop, auto_flow=True)


def test_auto_flow_is_rejected_for_pod_steps() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    with pytest.raises(TypeError, match=r"auto_flow was removed"):
        task.lamindb_k8s(image="python:3.12", auto_flow=True)


def test_partial_and_override_stay_standard_task_decorators() -> None:
    from airflow.sdk.bases.decorator import _TaskDecorator

    decorated = task.lamindb_venv(noop)
    assert type(decorated) is _TaskDecorator
    assert type(decorated.partial(x=1)) is _TaskDecorator
    assert type(decorated.override(task_id="other")) is _TaskDecorator
    with pytest.raises(ImportError):
        __import__("airflow.providers.lamindb.decorators.base")
