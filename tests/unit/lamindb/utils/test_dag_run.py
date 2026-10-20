from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow.providers.lamindb.utils import dag_run


def test_default_scope_reference_is_dag_and_run() -> None:
    assert dag_run.flow_run_reference("d", "r") == "d/r"
    assert dag_run.flow_run_reference("d", "r", dag_run.DEFAULT_SCOPE) == "d/r"
    assert dag_run.flow_run_reference("d", "r", None) == "d/r"


def test_scope_ids_make_distinct_references() -> None:
    assert dag_run.flow_run_reference("d", "r", "enrich") == "d/r/enrich"
    assert dag_run.flow_run_reference("d", "r", "enrich") != dag_run.flow_run_reference("d", "r", "load")
    assert dag_run.flow_run_reference("d", "r1", "x") != dag_run.flow_run_reference("d", "r2", "x")


def test_long_references_get_a_deterministic_digest_instead_of_a_truncation() -> None:
    first = dag_run.flow_run_reference("d", "r" * 300, "enrich")
    again = dag_run.flow_run_reference("d", "r" * 300, "enrich")
    other = dag_run.flow_run_reference("d", "r" * 300, "load")
    assert len(first) == dag_run.MAX_REFERENCE_LENGTH
    assert first == again
    assert first != other  # the tail that differs is in the digest
    assert dag_run.flow_run_reference("d", "r" * 300, "enrich") != dag_run.flow_run_reference("d", "r" * 301)


def test_flow_task_ids() -> None:
    assert dag_run.flow_task_ids(dag_run.DEFAULT_SCOPE) == ("lamindb_flow_init", "lamindb_flow_finish")
    assert dag_run.flow_task_ids("enrich") == ("lamindb_flow_init_enrich", "lamindb_flow_finish_enrich")


def test_dag_source_is_byte_exact(tmp_path) -> None:
    path = tmp_path / "dag.py"
    path.write_bytes(b"# a\r\n# b\r\n")
    source = dag_run.dag_source(SimpleNamespace(fileloc=str(path)))
    assert source == "# a\r\n# b\r\n"  # read_text() would have turned \r\n into \n and changed the hash


def test_task_instance_reference_prefers_the_log_url() -> None:
    context = {"ti": SimpleNamespace(log_url="http://airflow/x", task_id="t", map_index=-1, try_number=1)}
    assert dag_run.task_instance_reference(context) == "http://airflow/x"


@pytest.mark.parametrize("url", ["u" * 256, "", None, 5])
def test_task_instance_reference_falls_back_to_the_airflow_identity(url, make_context) -> None:
    context = make_context(run_id="manual__1")
    context["ti"] = SimpleNamespace(log_url=url, task_id="group.t", map_index=3, try_number=2)
    assert dag_run.task_instance_reference(context) == "my_dag/manual__1/group.t/3/2"


def test_task_instance_reference_fallback_is_bounded_and_distinguishes_attempts(make_context) -> None:
    def reference(try_number):
        context = make_context(run_id="r" * 300)
        context["ti"] = SimpleNamespace(log_url=None, task_id="t", map_index=-1, try_number=try_number)
        return dag_run.task_instance_reference(context)

    assert len(reference(1)) <= dag_run.MAX_REFERENCE_LENGTH
    assert reference(1) != reference(2)
    assert reference(1) == reference(1)


def test_task_instance_reference_never_contains_the_dag_run_conf(make_context) -> None:
    context = make_context()
    context["dag_run"].conf = {"password": "s3cret"}
    context["ti"] = SimpleNamespace(log_url=None, task_id="t", map_index=-1, try_number=1)
    assert "s3cret" not in dag_run.task_instance_reference(context)


def test_task_instance_reference_without_task_identity_is_none(make_context) -> None:
    context = make_context()
    context["ti"] = SimpleNamespace(log_url=None)
    assert dag_run.task_instance_reference(context) is None


LOG = logging.getLogger("test")


def test_dag_run_failed_looks_at_every_other_task_without_members(make_context) -> None:
    context = make_context(task_states={"a": "success", "me": "running"})
    assert dag_run.dag_run_failed(context, "me", LOG) is False
    context = make_context(task_states={"a": "upstream_failed", "me": "running"})
    assert dag_run.dag_run_failed(context, "me", LOG) is True
    context["ti"].get_task_states.assert_called_with(dag_id="my_dag", run_ids=["run_1"])


def test_dag_run_failed_asks_the_server_for_the_members_only(make_context) -> None:
    context = make_context(task_states={"a": "success", "m_0": "success", "m_1": "failed"})
    assert dag_run.dag_run_failed(context, "me", LOG, ["a", "m"]) is True
    context["ti"].get_task_states.assert_called_once_with(
        dag_id="my_dag", run_ids=["run_1"], task_ids=["a", "m"]
    )


def test_dag_run_failed_counts_every_state_the_server_returns_for_the_members(make_context) -> None:
    ok = make_context(task_states={"m_0": "success", "m_1": "success", "a": "skipped"})
    assert dag_run.dag_run_failed(ok, "me", LOG, ["a", "m"]) is False
    for state in ("failed", "upstream_failed"):
        bad = make_context(task_states={"m_0": "success", "m_1": state})
        assert dag_run.dag_run_failed(bad, "me", LOG, ["m"]) is True


def test_dag_run_failed_for_an_empty_scope_is_false_without_asking(make_context) -> None:
    context = make_context(task_states={"x": "failed"})
    assert dag_run.dag_run_failed(context, "me", LOG, []) is False
    context["ti"].get_task_states.assert_not_called()


def test_dag_run_failed_without_task_states_api_marks_completed(make_context, caplog) -> None:
    context = make_context()
    context["ti"] = SimpleNamespace()
    assert dag_run.dag_run_failed(context, "me", MagicMock(), ["a"]) is False


@pytest.mark.parametrize(
    ("scope_id", "ok"),
    [
        ("enrich", True),
        ("a-b_c9", True),
        ("a" * 64, True),
        ("", False),
        ("a b", False),
        ("a/b", False),
        ("a" * 65, False),
    ],
)
def test_validate_scope_id(scope_id, ok) -> None:
    if ok:
        assert dag_run.validate_scope_id(scope_id) == scope_id
    else:
        with pytest.raises(ValueError, match="scope_id"):
            dag_run.validate_scope_id(scope_id)


def test_validate_scope_id_checks_the_reference_budget() -> None:
    assert dag_run.validate_scope_id("enrich", "d" * 50) == "enrich"
    with pytest.raises(ValueError, match="too long for the flow run reference"):
        dag_run.validate_scope_id("s" * 64, "d" * 100)
