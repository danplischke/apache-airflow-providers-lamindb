from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.utils import remote


def _single_task(decorator, fn):
    @dag
    def test_dag():
        decorator(fn)()

    return test_dag().get_task("step")


def step():
    return 1


def test_task_decorators_registered() -> None:
    assert all(hasattr(task, name) for name in ("lamindb_venv", "lamindb_k8s"))
    # the in-process variant needed lamindb on the worker and was removed
    assert not hasattr(task, "lamindb")


def test_task_lamindb_venv_keeps_xcom_args_and_dependencies() -> None:
    @dag
    def test_dag():
        @task
        def produce():
            return 1

        @task.lamindb_venv
        def consume(value):
            return value

        consume(produce())

    consume_op = test_dag().get_task("consume")
    assert consume_op.upstream_task_ids == {"produce"}
    assert len(consume_op.op_args) == 1


def test_task_lamindb_venv_builds_operator() -> None:
    op = _single_task(task.lamindb_venv(requirements=["pandas"], lamindb_version="2.11.1"), step)
    assert type(op).__name__ == "LaminDBVenvDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_venv"
    assert op.requirements == ["pandas"]  # lamindb is added when the task runs
    assert op.lamindb_version == "2.11.1"
    assert op.lamindb_conn_id == "lamindb_default"
    assert {"requirements", "lamindb_conn_id", "lamindb_instance", "lamindb_cache_dir"} <= set(
        op.template_fields
    )


def test_task_lamindb_venv_rejects_older_lamindb_when_the_dag_is_parsed() -> None:
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.8\.0 is not supported"):
        _single_task(task.lamindb_venv(lamindb_version="2.8.0"), step)


def test_task_lamindb_venv_passes_the_cache_dir_to_the_flow_operators() -> None:
    op = _single_task(task.lamindb_venv(lamindb_cache_dir="/shared", auto_flow=True), step)
    init = op.get_dag().get_task("lamindb_flow_init")
    assert op.lamindb_cache_dir == init.lamindb_cache_dir == "/shared"


def _execute_in_fake_venv(op, context):
    """Execute the step, but exec its shipped source here instead of in a virtualenv."""
    seen: dict = {}

    def fake_venv_execute(self, context):
        seen["requirements"] = list(self.requirements)
        seen["env_vars"] = dict(self.env_vars or {})
        seen["settings_dir_existed"] = os.path.isdir(seen["env_vars"].get("LAMIN_SETTINGS_DIR", ""))
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace["step"]()

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        return op.execute(context), seen


def test_remote_step_ships_wrapped_source_only_during_execute(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = _single_task(task.lamindb_venv(requirements=["pandas"]), step)
    plain = op.get_python_source()
    assert plain.startswith("def step():")
    assert "_lamindb_airflow_step" not in plain
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"  # instance of the flow run

    result, seen = _execute_in_fake_venv(op, context)

    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["source_code"] == "# dag source"
    assert kwargs["entrypoint"] == "step"
    assert kwargs["initiated_by_run"] is fake_lamindb.flow_run
    assert op.get_python_source() == plain
    assert seen["requirements"] == ["pandas", "lamindb-core==2.11.0", "numpy", "pandera>=0.24.0"]
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"
    assert seen["settings_dir_existed"]
    assert op.env_vars is None


def test_step_on_another_instance_than_the_flow_run_fails_early(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_venv(lamindb_instance="owner/other", auto_flow=True), step)
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"

    with pytest.raises(
        AirflowException, match="'owner/other', but the flow run of this DAG run is in 'owner/name'"
    ):
        _execute_in_fake_venv(op, context)
    context["ti"].xcom_pull.assert_called_once_with(task_ids="lamindb_flow_init", key="lamindb_instance")
    fake_lamindb.track.assert_not_called()


def test_step_instance_is_compared_normalised(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = _single_task(task.lamindb_venv(lamindb_conn_id=None, lamindb_instance=" owner/name/ "), step)
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"

    result, _ = _execute_in_fake_venv(op, context)
    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")


def test_step_without_connection_uses_lamindb_configuration(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")
    op = _single_task(task.lamindb_venv(lamindb_conn_id=None), step)

    result, seen = _execute_in_fake_venv(op, make_context())
    assert result == 1
    fake_lamindb.connect.assert_not_called()
    assert seen["env_vars"] == {}
    assert seen["requirements"][0] == "lamindb-core>=2.8.1"


def test_untracked_steps_are_plain_tasks() -> None:
    @dag
    def test_dag():
        task.lamindb_venv(track=False, requirements=["pandas"], task_id="venv_step")(step)()

    built = test_dag()
    assert built.task_ids == ["venv_step"]
    assert built.get_task("venv_step").requirements == ["pandas"]


def test_untracked_venv_step_ships_source_unchanged(make_context) -> None:
    op = _single_task(task.lamindb_venv(track=False), step)
    seen = {}

    def fake_venv_execute(self, context):
        seen["source"] = self.get_python_source()
        return 1

    with patch.object(type(op).__mro__[2], "execute", fake_venv_execute):
        assert op.execute(make_context()) == 1
    assert seen["source"].startswith("def step():")
    assert "_lamindb_airflow_step" not in seen["source"]


def test_task_lamindb_k8s_builds_operator() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_k8s(image="python:3.12", lamindb_instance="owner/name"), step)
    assert type(op).__name__ == "LaminDBK8sDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_k8s"
    assert op.lamindb_instance == "owner/name"
    assert op.lamindb_conn_id == "lamindb_default"
    assert {"image", "lamindb_conn_id", "lamindb_instance"} <= set(op.template_fields)


def test_task_lamindb_k8s_gets_only_the_instance_from_the_connection(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_k8s(image="python:3.12"), step)
    context = make_context()
    context["ti"].xcom_pull.return_value = None  # flow run instance unknown: no check
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = self.env_vars

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'instance': 'owner/name'" in seen["source"]
    assert "api-key" not in repr(seen)  # the pod's credentials come from a Kubernetes secret


def test_task_lamindb_k8s_without_connection_uses_the_pod_configuration(
    fake_lamindb: MagicMock, make_context, monkeypatch
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)

    @dag
    def test_dag():
        task.lamindb_k8s(image="python:3.12", lamindb_conn_id=None, auto_flow=True)(step)()

    built = test_dag()
    op = built.get_task("step")
    assert built.get_task("lamindb_flow_init").lamindb_conn_id is None
    assert built.get_task("lamindb_flow_finish").lamindb_conn_id is None
    context = make_context()
    context["ti"].xcom_pull.return_value = None
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'instance': None" in seen["source"]


def test_task_lamindb_k8s_fails_without_the_default_connection(make_context, monkeypatch) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.exceptions import AirflowNotFoundException

    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    op = _single_task(task.lamindb_k8s(image="python:3.12"), step)
    with pytest.raises(AirflowNotFoundException, match=r"'lamindb_default' connection.*lamindb_conn_id=None"):
        op.execute(make_context())


def load(user, credentials, day):
    return [user, credentials, day]


def _load_task(decorator, *args, **kwargs):
    @dag
    def test_dag():
        decorator(load)(*args, **kwargs)

    return test_dag().get_task("load")


TEMPLATES = {"var": {"value": {"lims_credentials": "s3cret-credentials"}}, "ds": "2026-10-15"}


def _render_and_execute(op, context):
    """Render the step's templates as Airflow's task runner does, then execute it in a fake venv."""
    context.update(TEMPLATES)
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

    def fake_venv_execute(self, context):
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace[self.python_callable.__name__](*self.op_args, **self.op_kwargs)

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        return op.execute(context)


@pytest.mark.parametrize(
    ("mode", "recorded"),
    [
        ("secrets", {"user": "alice", "day": "2026-10-15"}),
        ("all", {"user": "alice"}),
        ("none", {"user": "alice", "credentials": "s3cret-credentials", "day": "2026-10-15"}),
    ],
)
def test_templated_arguments_are_left_out_of_the_recorded_params(
    fake_lamindb: MagicMock, make_context, mode: str, recorded: dict
) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _load_task(
        task.lamindb_venv(lamindb_conn_id=None, lamindb_exclude_templated=mode),
        "alice",
        "{{ var.value.lims_credentials }}",
        day="{{ ds }}",
    )

    result = _render_and_execute(op, make_context())

    assert result == ["alice", "s3cret-credentials", "2026-10-15"]
    assert fake_lamindb.track.call_args.kwargs["params"] == recorded


def test_secret_templates_are_found_in_nested_values(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _load_task(
        task.lamindb_venv(lamindb_conn_id=None),
        "alice",
        {"auth": ["Bearer {{ var.value.lims_credentials | trim }}"]},
        "{% if ds %}{{ ds }}{% endif %}",
    )

    _render_and_execute(op, make_context())

    assert fake_lamindb.track.call_args.kwargs["params"] == {"user": "alice", "day": "2026-10-15"}


def test_explicitly_excluded_params_are_left_out(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _load_task(
        task.lamindb_venv(lamindb_conn_id=None, lamindb_exclude_params=["user"]), "alice", "c", "d"
    )

    assert _render_and_execute(op, make_context()) == ["alice", "c", "d"]
    assert fake_lamindb.track.call_args.kwargs["params"] == {"credentials": "c", "day": "d"}


def test_arguments_airflow_masks_are_left_out(fake_lamindb: MagicMock, make_context) -> None:
    from airflow.sdk._shared.secrets_masker import mask_secret

    fake_lamindb.flow_run = MagicMock()
    mask_secret("registered-secret-value")
    op = _load_task(
        task.lamindb_venv(lamindb_conn_id=None, lamindb_exclude_templated="none"),
        "alice",
        "registered-secret-value",
        "d",
    )

    _render_and_execute(op, make_context())

    assert fake_lamindb.track.call_args.kwargs["params"] == {"user": "alice", "day": "d"}


def test_unknown_excluded_params_fail_when_the_dag_is_parsed() -> None:
    with pytest.raises(ValueError, match=r"names \['pasword'\], which are not parameters of load\(\)"):
        _load_task(task.lamindb_venv(lamindb_exclude_params=["pasword"]), "a", "b", "c")


def test_excluded_params_may_name_kwargs_keys() -> None:
    def with_options(**options):
        pass

    @dag
    def test_dag():
        task.lamindb_venv(lamindb_exclude_params="token")(with_options)(token="t")

    assert test_dag().get_task("with_options").lamindb_exclude_params == ("token",)


def test_exclude_templated_must_be_a_known_mode() -> None:
    with pytest.raises(ValueError, match="lamindb_exclude_templated must be 'secrets', 'all' or 'none'"):
        _load_task(task.lamindb_venv(lamindb_exclude_templated="secret"), "a", "b", "c")


def test_mapped_steps_leave_out_secret_templates(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock()

    @dag
    def test_dag():
        task.lamindb_venv(lamindb_conn_id=None)(load).partial(user="alice").expand(
            credentials=["{{ var.value.lims_credentials }}", "plain"], day=["{{ ds }}", "{{ ds }}"]
        )

    mapped = test_dag().get_task("load")
    context = make_context()
    context["ti"].map_index = 0
    context.update(TEMPLATES)
    mapped.render_template_fields(context)
    op = context["task"]  # the unmapped task Airflow renders and runs

    assert op._lamindb_templated_params == frozenset({"credentials"})


def test_mapped_xcom_values_are_not_templates(fake_lamindb: MagicMock, make_context, monkeypatch) -> None:
    from airflow.sdk.definitions import xcom_arg

    monkeypatch.setattr(xcom_arg, "get_task_map_length", lambda *args: 1)

    @dag
    def test_dag():
        @task
        def produce():
            return ["{{ var.value.lims_credentials }}"]

        task.lamindb_venv(lamindb_conn_id=None)(load).partial(user="alice", day="d").expand(
            credentials=produce()
        )

    mapped = test_dag().get_task("load")
    context = make_context()
    context["ti"].map_index = 0
    context["ti"].xcom_pull.return_value = ["{{ var.value.lims_credentials }}"]
    context.update(TEMPLATES)
    mapped.render_template_fields(context)
    op = context["task"]

    assert op.op_kwargs["credentials"] == "{{ var.value.lims_credentials }}"  # Airflow didn't render it
    assert op._lamindb_templated_params == frozenset()


def test_task_lamindb_k8s_ships_the_excluded_params(
    fake_lamindb: MagicMock, make_context, monkeypatch
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    op = _load_task(
        task.lamindb_k8s(image="python:3.12", lamindb_conn_id=None, lamindb_exclude_params=["user"]),
        "alice",
        "{{ var.value.lims_credentials }}",
        "{{ ds }}",
    )
    context = make_context()
    context.update(TEMPLATES)
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'exclude_params': ['credentials', 'user']" in seen["source"]


def test_sensitive_looking_names_are_still_recorded(fake_lamindb: MagicMock, make_context) -> None:
    """Airflow matches sensitive names by substring; only values Airflow knows to be secret count."""

    def analyze(tokenizer, secretory_cells, max_tokens):
        return tokenizer

    fake_lamindb.flow_run = MagicMock()

    @dag
    def test_dag():
        task.lamindb_venv(lamindb_conn_id=None)(analyze)("bpe", ["acinar"], 512)

    op = test_dag().get_task("analyze")
    context = make_context()
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

    def fake_venv_execute(self, context):
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace["analyze"](*self.op_args, **self.op_kwargs)

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        op.execute(context)
    assert fake_lamindb.track.call_args.kwargs["params"] == {
        "tokenizer": "bpe",
        "secretory_cells": ["acinar"],
        "max_tokens": 512,
    }
