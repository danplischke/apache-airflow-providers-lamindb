from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException
from airflow.sdk import DAG, dag, task
from pydantic import ValidationError

from airflow.providers.lamindb import LaminDBFlow
from airflow.providers.lamindb.exceptions import LaminDBVersionError
from airflow.providers.lamindb.execution import environment as execution_environment
from airflow.providers.lamindb.execution import parameters as execution_parameters
from airflow.providers.lamindb.execution import setup as execution_setup
from airflow.providers.lamindb.execution import source as execution_source
from airflow.providers.lamindb.operators import step as lamindb_step

# these tests build flow operators on their own, which warns that they group no tasks
pytestmark = pytest.mark.filterwarnings("ignore::airflow.providers.lamindb.exceptions.LaminDBFlowWarning")


def _single_task(decorator, fn):
    @dag
    def test_dag():
        decorator(fn)()

    return test_dag().get_task("step")


def step():
    return 1


def test_task_decorators_registered() -> None:
    assert all(hasattr(task, name) for name in ("lamindb_virtualenv", "lamindb_kubernetes"))
    # the in-process variant needed lamindb on the worker and was removed
    assert not hasattr(task, "lamindb")


def test_step_config_is_validated_before_shipping(make_context, lamindb_connection) -> None:
    op = _single_task(task.lamindb_virtualenv, step)
    with (
        patch.object(lamindb_step, "dag_source", return_value=123),
        patch.object(type(op).__mro__[2], "execute", return_value=1),
        pytest.raises(ValidationError),
    ):
        op.execute(make_context())


def test_task_lamindb_virtualenv_keeps_xcom_args_and_dependencies() -> None:
    @dag
    def test_dag():
        @task
        def produce():
            return 1

        @task.lamindb_virtualenv
        def consume(value):
            return value

        consume(produce())

    consume_op = test_dag().get_task("consume")
    assert consume_op.upstream_task_ids == {"produce"}
    assert len(consume_op.op_args) == 1


def test_task_lamindb_virtualenv_builds_operator() -> None:
    op = _single_task(task.lamindb_virtualenv(requirements=["pandas"], lamindb_version="2.11.1"), step)
    assert type(op).__name__ == "LaminDBVirtualenvDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_virtualenv"
    assert op.requirements == ["pandas"]  # lamindb is added when the task runs
    assert op.lamindb_version == "2.11.1"
    assert op.lamindb_conn_id == "lamindb_default"
    assert {"requirements", "lamindb_conn_id", "lamindb_instance", "lamindb_cache_dir"} <= set(
        op.template_fields
    )


def test_task_lamindb_virtualenv_rejects_older_lamindb_when_the_dag_is_parsed() -> None:
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.8\.0 is not supported"):
        _single_task(task.lamindb_virtualenv(lamindb_version="2.8.0"), step)


def test_flow_scopes_configure_their_own_flow_operators() -> None:
    @dag
    def scoped():
        with LaminDBFlow(lamindb_cache_dir="/shared", lamindb_conn_id=None):
            task.lamindb_virtualenv(step, lamindb_cache_dir="/other")()

    built = scoped()
    init = built.get_task("lamindb_flow_init")
    assert init.lamindb_cache_dir == "/shared"  # nothing is copied from the step
    assert init.lamindb_conn_id is None
    assert built.get_task("step").lamindb_cache_dir == "/other"
    assert built.get_task("step").lamindb_conn_id == "lamindb_default"


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
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        return op.execute(context), seen


def test_remote_step_ships_wrapped_source_only_during_execute(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv(requirements=["pandas"]), step)
    plain = op.get_python_source()
    assert plain.startswith("def step():")
    assert "_lamindb_airflow_step" not in plain
    context = make_context()

    result, seen = _execute_in_fake_venv(op, context)

    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["source_code"] == "# dag source"
    assert kwargs["key"] == "my_dag.py"  # the DAG file, not the wrapper script it runs from
    assert kwargs["entrypoint"] == "step"
    assert kwargs["initiated_by_run"] is None  # standalone: no flow run was looked up
    fake_lamindb.Run.filter.assert_not_called()
    context["ti"].xcom_pull.assert_not_called()
    assert op.get_python_source() == plain
    assert seen["requirements"] == ["pandas", "lamindb==2.11.0"]
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"
    assert seen["settings_dir_existed"]
    assert op.env_vars is None


def test_scoped_step_on_another_instance_than_its_flow_run_fails_early(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    @dag
    def scoped():
        with LaminDBFlow():
            task.lamindb_virtualenv(lamindb_instance="owner/other")(step)()

    op = scoped().get_task("step")
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"

    with pytest.raises(AirflowException, match="'owner/other', but its flow run is in 'owner/name'"):
        _execute_in_fake_venv(op, context)
    context["ti"].xcom_pull.assert_called_once_with(task_ids="lamindb_flow_init", key="lamindb_instance")
    fake_lamindb.track.assert_not_called()


def test_standalone_step_may_use_any_instance(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv(lamindb_instance="owner/other"), step)

    assert _execute_in_fake_venv(op, make_context())[0] == 1
    fake_lamindb.connect.assert_called_once_with("owner/other")


def _scoped_step(scope_id=None, **kwargs):
    @dag
    def scoped():
        with LaminDBFlow(scope_id):
            task.lamindb_virtualenv(**kwargs)(step)()

    return scoped().get_task("step")


def test_scoped_step_is_a_child_of_its_scopes_flow_run(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(**{"transform.key": "team/my_dag.py"})
    op = _scoped_step("enrich")
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"

    assert _execute_in_fake_venv(op, context)[0] == 1

    assert fake_lamindb.Run.filter.call_args.kwargs["reference"] == "my_dag/run_1/enrich"
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["initiated_by_run"] is flow_run
    assert kwargs["key"] == "team/my_dag.py"  # the parent's actual transform key
    fake_lamindb.Transform.filter.assert_not_called()  # no lookup of the DAG file's transform


def test_scoped_step_without_its_flow_run_fails_instead_of_running_standalone(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _scoped_step()
    context = make_context()
    context["ti"].xcom_pull.return_value = None

    with pytest.raises(LookupError, match="No LaminDB flow run found for 'my_dag/run_1'"):
        _execute_in_fake_venv(op, context)
    fake_lamindb.track.assert_not_called()


def test_step_ships_the_dag_file_name_and_its_task_attempt_reference(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv, step)
    context = make_context(fileloc="/dags/team/my_dag.py")
    context["ti"].log_url = "u" * 300  # too long for a reference
    context["ti"].task_id, context["ti"].map_index, context["ti"].try_number = "step", -1, 2
    seen = {}

    def fake_venv_execute(self, context):
        seen["source"] = self.get_python_source()

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        op.execute(context)

    assert "'fileloc': '/dags/team/my_dag.py'" in seen["source"]
    assert "'step_reference': 'my_dag/run_1/step/-1/2'" in seen["source"]
    assert "'flow_run_reference': None" in seen["source"]
    assert "run_1/enrich" not in seen["source"]


def test_standalone_steps_do_not_inspect_flow_operators(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    @dag
    def mixed():
        with LaminDBFlow():
            task.lamindb_virtualenv(step, task_id="inside")()
        task.lamindb_virtualenv(step, task_id="outside")()

    op = mixed().get_task("outside")
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/other-instance"  # a scope's flow run elsewhere

    assert _execute_in_fake_venv(op, context)[0] == 1
    context["ti"].xcom_pull.assert_not_called()


def test_scoped_mapped_steps_resolve_their_scope_when_unmapped(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock(**{"transform.key": "my_dag.py"})

    @dag
    def mapped():
        with LaminDBFlow("enrich"):
            task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["a"])

    unmapped = mapped().get_task("load").unmap({"op_kwargs": {"user": "a"}})
    context = make_context()
    context["ti"].xcom_pull.return_value = None
    seen = {}

    def fake_venv_execute(self, context):
        seen["source"] = self.get_python_source()

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(unmapped).__mro__[2], "execute", fake_venv_execute),
    ):
        unmapped.execute(context)

    assert "'flow_run_reference': 'my_dag/run_1/enrich'" in seen["source"]


def test_step_instance_is_compared_normalised(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock(uid="flowuid")

    @dag
    def scoped():
        with LaminDBFlow():
            task.lamindb_virtualenv(lamindb_conn_id=None, lamindb_instance=" owner/name/ ")(step)()

    op = scoped().get_task("step")
    context = make_context()
    context["ti"].xcom_pull.return_value = "owner/name"

    result, _ = _execute_in_fake_venv(op, context)
    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")


def test_step_without_connection_uses_lamindb_configuration(fake_lamindb: MagicMock, make_context) -> None:
    op = _single_task(task.lamindb_virtualenv(lamindb_conn_id=None), step)

    result, seen = _execute_in_fake_venv(op, make_context())
    assert result == 1
    fake_lamindb.connect.assert_not_called()
    assert seen["env_vars"] == {}
    assert seen["requirements"][0] == "lamindb>=2.8.1"


def test_untracked_steps_are_plain_tasks() -> None:
    @dag
    def test_dag():
        task.lamindb_virtualenv(track=False, requirements=["pandas"], task_id="venv_step")(step)()

    built = test_dag()
    assert built.task_ids == ["venv_step"]
    assert built.get_task("venv_step").requirements == ["pandas"]


def test_untracked_venv_step_ships_source_unchanged(make_context) -> None:
    op = _single_task(task.lamindb_virtualenv(track=False), step)
    seen = {}

    def fake_venv_execute(self, context):
        seen["source"] = self.get_python_source()
        return 1

    with patch.object(type(op).__mro__[2], "execute", fake_venv_execute):
        assert op.execute(make_context()) == 1
    assert seen["source"].startswith("def step():")
    assert "_lamindb_airflow_step" not in seen["source"]


def test_task_lamindb_kubernetes_builds_operator() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_kubernetes(image="python:3.12", lamindb_instance="owner/name"), step)
    assert type(op).__name__ == "LaminDBKubernetesDecoratedOperator"
    assert op.custom_operator_name == "@task.lamindb_kubernetes"
    assert op.lamindb_instance == "owner/name"
    assert op.lamindb_conn_id == "lamindb_default"
    assert {"image", "lamindb_conn_id", "lamindb_instance"} <= set(op.template_fields)


def test_task_lamindb_kubernetes_gets_only_the_instance_from_the_connection(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_kubernetes(image="python:3.12"), step)
    context = make_context()
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = self.env_vars

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'instance': 'owner/name'" in seen["source"]
    assert "api-key" not in repr(seen)  # the pod's credentials come from a Kubernetes secret


def test_task_lamindb_kubernetes_without_connection_uses_the_pod_configuration(
    fake_lamindb: MagicMock, make_context, monkeypatch
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)

    op = _single_task(task.lamindb_kubernetes(image="python:3.12", lamindb_conn_id=None), step)
    context = make_context()
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'instance': None" in seen["source"]


def test_task_lamindb_kubernetes_fails_without_the_default_connection(make_context, monkeypatch) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.exceptions import AirflowNotFoundException

    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    op = _single_task(task.lamindb_kubernetes(image="python:3.12"), step)
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
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
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
        task.lamindb_virtualenv(lamindb_conn_id=None, lamindb_exclude_templated=mode),
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
        task.lamindb_virtualenv(lamindb_conn_id=None),
        "alice",
        {"auth": ["Bearer {{ var.value.lims_credentials | trim }}"]},
        "{% if ds %}{{ ds }}{% endif %}",
    )

    _render_and_execute(op, make_context())

    assert fake_lamindb.track.call_args.kwargs["params"] == {"user": "alice", "day": "2026-10-15"}


def test_explicitly_excluded_params_are_left_out(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _load_task(
        task.lamindb_virtualenv(lamindb_conn_id=None, lamindb_exclude_params=["user"]), "alice", "c", "d"
    )

    assert _render_and_execute(op, make_context()) == ["alice", "c", "d"]
    assert fake_lamindb.track.call_args.kwargs["params"] == {"credentials": "c", "day": "d"}


def test_arguments_airflow_masks_are_left_out(fake_lamindb: MagicMock, make_context) -> None:
    from airflow.sdk._shared.secrets_masker import mask_secret

    fake_lamindb.flow_run = MagicMock()
    mask_secret("registered-secret-value")
    op = _load_task(
        task.lamindb_virtualenv(lamindb_conn_id=None, lamindb_exclude_templated="none"),
        "alice",
        "registered-secret-value",
        "d",
    )

    _render_and_execute(op, make_context())

    assert fake_lamindb.track.call_args.kwargs["params"] == {"user": "alice", "day": "d"}


def test_unknown_excluded_params_fail_when_the_dag_is_parsed() -> None:
    with pytest.raises(ValueError, match=r"names \['pasword'\], which are not parameters of load\(\)"):
        _load_task(task.lamindb_virtualenv(lamindb_exclude_params=["pasword"]), "a", "b", "c")


def test_excluded_params_may_name_kwargs_keys() -> None:
    def with_options(**options):
        pass

    @dag
    def test_dag():
        task.lamindb_virtualenv(lamindb_exclude_params="token")(with_options)(token="t")

    assert test_dag().get_task("with_options").lamindb_exclude_params == ("token",)


def test_exclude_templated_must_be_a_known_mode() -> None:
    with pytest.raises(ValueError, match="lamindb_exclude_templated must be 'secrets', 'all' or 'none'"):
        _load_task(task.lamindb_virtualenv(lamindb_exclude_templated="secret"), "a", "b", "c")


def test_mapped_steps_leave_out_secret_templates(fake_lamindb: MagicMock, make_context) -> None:
    fake_lamindb.flow_run = MagicMock()

    @dag
    def test_dag():
        task.lamindb_virtualenv(lamindb_conn_id=None)(load).partial(user="alice").expand(
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

        task.lamindb_virtualenv(lamindb_conn_id=None)(load).partial(user="alice", day="d").expand(
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


def test_task_lamindb_kubernetes_ships_the_excluded_params(
    fake_lamindb: MagicMock, make_context, monkeypatch
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    op = _load_task(
        task.lamindb_kubernetes(image="python:3.12", lamindb_conn_id=None, lamindb_exclude_params=["user"]),
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
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_pod_execute),
    ):
        op.execute(context)

    assert "'exclude_params': ['credentials', 'user']" in seen["source"]


def _execute_connected(op, context, seen=None):
    """Execute in a fake venv; returns the result and what the virtualenv saw (also into ``seen``)."""
    seen = {} if seen is None else seen

    def fake_venv_execute(self, context):
        seen["requirements"] = list(self.requirements)
        seen["env_vars"] = dict(self.env_vars or {})
        seen["source"] = self.get_python_source()
        namespace: dict = {}
        exec(seen["source"], namespace)
        return namespace[self.python_callable.__name__](*self.op_args, **self.op_kwargs)

    with patch.object(type(op).__mro__[2], "execute", fake_venv_execute):
        try:
            return op.execute(context), seen
        finally:
            settings_dir = seen.get("env_vars", {}).get("LAMIN_SETTINGS_DIR", "")
            seen["settings_dir_exists_after"] = os.path.isdir(settings_dir)


def test_connect_only_step_connects_without_recording(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv(track=False, connect=True), step)
    assert op.get_dag().task_ids == ["step"]  # no flow tasks

    result, seen = _execute_connected(op, make_context())

    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()  # no flow run needed
    assert "run_connected" in seen["source"]
    assert seen["requirements"] == ["lamindb==2.11.0"]
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"
    assert not seen["settings_dir_exists_after"]
    assert op.env_vars is None


def test_connect_only_step_cleans_up_when_the_function_fails(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    def fail():
        raise ValueError("boom")

    op = _single_task(task.lamindb_virtualenv(track=False, connect=True, task_id="step"), fail)

    seen: dict = {}
    with pytest.raises(ValueError, match="boom"):
        _execute_connected(op, make_context(), seen)
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_not_called()
    assert seen["env_vars"]["LAMIN_SETTINGS_DIR"]
    assert not seen["settings_dir_exists_after"]
    assert op.env_vars is None


def test_connect_only_step_needs_its_connection(make_context, monkeypatch) -> None:
    from airflow.exceptions import AirflowNotFoundException

    monkeypatch.delenv("AIRFLOW_CONN_LAMINDB_DEFAULT", raising=False)
    op = _single_task(task.lamindb_virtualenv(track=False, connect=True), step)
    with pytest.raises(AirflowNotFoundException, match="'lamindb_default' connection"):
        op.execute(make_context())


def test_connect_only_step_rejects_instances_on_older_lamindb(
    make_context, lamindb_connection, monkeypatch
) -> None:
    def too_old(self):
        raise LaminDBVersionError("runs lamindb 2.7.0")

    monkeypatch.setattr(execution_environment.RemoteLaminDB, "instance_lamindb_version", too_old)
    op = _single_task(task.lamindb_virtualenv(track=False, connect=True), step)
    with pytest.raises(LaminDBVersionError, match=r"2\.7\.0"):
        _execute_connected(op, make_context())


def test_connect_only_mapped_steps_resolve_their_own_instance(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    @dag
    def test_dag():
        task.lamindb_virtualenv(track=False, connect=True, lamindb_instance="owner/site{{ ti.map_index }}")(
            load
        ).partial(credentials="c", day="d").expand(user=["a", "b"])

    mapped = test_dag().get_task("load")
    for map_index in (0, 1):
        context = make_context()
        context["ti"].map_index = map_index
        mapped.render_template_fields(context)
        result, _ = _execute_connected(context["task"], context)
        assert result == [["a", "b"][map_index], "c", "d"]
    assert [c.args for c in fake_lamindb.connect.call_args_list] == [("owner/site0",), ("owner/site1",)]
    # not part of a flow run, so not held to its instance
    context["ti"].xcom_pull.assert_not_called()


def test_task_lamindb_kubernetes_connect_only_uses_the_pod_credentials(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_kubernetes(image="python:3.12", track=False, connect=True), step)
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = self.env_vars

    with patch.object(type(op).__mro__[2], "execute", fake_pod_execute):
        op.execute(make_context())

    config = "**{'instance': 'owner/name', 'space': None, 'branch': None}"
    assert f"run_connected(_lamindb_airflow_user_fn, args, kwargs, {config})" in seen["source"]
    assert "api-key" not in repr(seen)


def test_templated_lamindb_version_is_checked_once_rendered(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _single_task(task.lamindb_virtualenv(lamindb_version="{{ params.pin }}"), step)  # parses
    assert "lamindb_version" in op.template_fields

    context = make_context()
    context["params"] = {"pin": " 2.10.1 "}
    op.render_template_fields(context)
    _, seen = _execute_in_fake_venv(op, context)
    assert seen["requirements"][0] == "lamindb==2.10.1"


@pytest.mark.parametrize(
    ("pin", "error"),
    [("2.8.0", r"lamindb 2\.8\.0 is not supported"), ("None", "'None' is not a lamindb version")],
)
def test_rendered_lamindb_version_fails_before_installing(
    fake_lamindb: MagicMock, make_context, lamindb_connection, pin: str, error: str
) -> None:
    op = _single_task(task.lamindb_virtualenv(lamindb_version="{{ params.pin }}"), step)
    context = make_context()
    context["params"] = {"pin": pin}
    op.render_template_fields(context)

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute") as venv_execute,
        pytest.raises(LaminDBVersionError, match=error),
    ):
        op.execute(context)
    venv_execute.assert_not_called()


def test_mapped_steps_build_their_own_pinned_environment(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock()

    @dag
    def test_dag():
        task.lamindb_virtualenv(lamindb_version="2.{{ 10 + ti.map_index }}.0", venv_cache_path="/tmp/venvs")(
            load
        ).partial(credentials="c", day="d").expand(user=["a", "b", "c"])

    mapped = test_dag().get_task("load")
    requirements, hashes = [], []

    def fake_venv_execute(self, context):
        requirements.append(list(self.requirements))
        hashes.append(self._calculate_cache_hash()[0])

    for map_index in (0, 1, 2):
        context = make_context()
        context["ti"].map_index = map_index
        context["ti"].xcom_pull.return_value = None
        mapped.render_template_fields(context)
        op = context["task"]
        with (
            patch.object(lamindb_step, "dag_source", return_value="# dag source"),
            patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
        ):
            op.execute(context)

    assert [r[0] for r in requirements] == [
        "lamindb==2.10.0",
        "lamindb==2.11.0",
        "lamindb==2.12.0",
    ]
    assert len(set(hashes)) == 3


def test_flow_scopes_take_a_templated_lamindb_version_of_their_own() -> None:
    @dag
    def test_dag():
        with LaminDBFlow(lamindb_version="{{ params.pin }}"):
            task.lamindb_virtualenv(lamindb_version="{{ params.other }}")(step)()

    built = test_dag()
    assert built.get_task("lamindb_flow_init").lamindb_version == "{{ params.pin }}"
    assert built.get_task("lamindb_flow_finish").lamindb_version == "{{ params.pin }}"
    assert built.get_task("step").lamindb_version == "{{ params.other }}"


def test_flow_operators_render_lamindb_version() -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator(task_id="init", lamindb_version="{{ params.pin }}")
    assert "lamindb_version" in init.template_fields
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.8\.0 is not supported"):
        LaminDBFlowInitOperator(task_id="init2", lamindb_version="2.8.0")


def test_dags_with_templated_lamindb_versions_serialize() -> None:
    from airflow.serialization.serialized_objects import DagSerialization

    @dag
    def test_dag():
        task.lamindb_virtualenv(lamindb_version="{{ params.pin }}")(load).partial(
            credentials="c", day="d"
        ).expand(user=["a"])

    serialized = DagSerialization.to_dict(test_dag())
    assert "{{ params.pin }}" in str(serialized)


def test_lamindb_space_is_templated_and_reaches_the_step(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv(lamindb_space="{{ params.space }}"), step)
    context = make_context()
    context["params"] = {"space": " lab "}
    op.render_template_fields(context)

    _execute_in_fake_venv(op, context)

    fake_lamindb.Space.filter.assert_called_once()
    assert (
        fake_lamindb.track.call_args.kwargs["space"] is fake_lamindb.Space.filter.return_value.one_or_none()
    )


def test_lamindb_space_from_default_args_reaches_steps_and_flow_operators() -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    @dag(default_args={"lamindb_space": "lab"})
    def test_dag():
        LaminDBFlowInitOperator()
        task.lamindb_virtualenv(step)()

    built = test_dag()
    assert built.get_task("step").lamindb_space == "lab"
    assert built.get_task("lamindb_flow_init").lamindb_space == "lab"
    assert "lamindb_space" in built.get_task("lamindb_flow_init").template_fields


def test_flow_scopes_do_not_copy_the_space_of_their_steps() -> None:
    @dag
    def test_dag():
        with LaminDBFlow(lamindb_space="lab"):
            task.lamindb_virtualenv(lamindb_space="other")(step)()

    built = test_dag()
    assert built.get_task("lamindb_flow_init").lamindb_space == "lab"
    assert built.get_task("step").lamindb_space == "other"


def test_flow_init_starts_the_flow_run_in_its_space(make_context, lamindb_connection) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator(task_id="init", lamindb_space="lab")
    assert init._runtime_kwargs(make_context())["space"] == "lab"
    init.lamindb_space = "  "  # rendered empty: lamindb's default space
    assert init._runtime_kwargs(make_context())["space"] is None


class SiteSetup(execution_setup.LaminDBSetup):
    """Records how it was called; the token stands for a secret from an Airflow Variable."""

    def __init__(self) -> None:
        self.contexts: list = []

    def bootstrap_source(self) -> str:
        return "import os\nos.environ['SITE_BOOTSTRAPPED'] = '1'"

    def execution_env(self, context) -> dict[str, str]:
        self.contexts.append(context)
        return {"SITE_TOKEN": "s3cret-token", "LAMIN_API_KEY": "ignored", "OVERRIDDEN": "setup"}


def test_lamindb_setup_prepares_the_step_virtualenv(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock()
    setup = SiteSetup()
    op = _single_task(task.lamindb_virtualenv(lamindb_setup=setup, env_vars={"OVERRIDDEN": "task"}), step)
    assert setup.contexts == []  # nothing resolved when the DAG is parsed
    context = make_context()
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

    with patch.object(lamindb_step, "dag_source", return_value="# dag source"):
        result, seen = _execute_connected(op, context)

    assert result == 1
    assert setup.contexts == [context]
    assert seen["env_vars"]["SITE_TOKEN"] == "s3cret-token"
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"  # the provider's own variables win
    assert seen["env_vars"]["OVERRIDDEN"] == "task"  # and the task's env_vars over both
    source = seen["source"]
    assert source.index("os.environ['SITE_BOOTSTRAPPED'] = '1'") < source.index("def step():")
    assert op.env_vars == {"OVERRIDDEN": "task"}
    assert "s3cret-token" not in repr({field: getattr(op, field) for field in op.template_fields})


def test_lamindb_setup_also_prepares_connect_only_tasks(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(task.lamindb_virtualenv(track=False, connect=True, lamindb_setup=SiteSetup()), step)
    _, seen = _execute_connected(op, make_context())
    assert seen["env_vars"]["SITE_TOKEN"] == "s3cret-token"
    assert "SITE_BOOTSTRAPPED" in seen["source"]


def test_lamindb_setup_prepares_the_flow_operators(make_context, lamindb_connection) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    setup = SiteSetup()
    init = LaminDBFlowInitOperator(task_id="init", lamindb_setup=setup)
    seen = {}

    def fake_venv_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = dict(self.env_vars)
        return "flowuid"

    context = make_context()
    with patch.object(type(init).__mro__[2], "execute", fake_venv_execute):
        init.execute(context)

    assert seen["source"].startswith("# lamindb_setup.bootstrap_source()\nexec(compile(")
    assert seen["env_vars"]["SITE_TOKEN"] == "s3cret-token"
    assert init.env_vars is None


class PackagesSetup(execution_setup.LaminDBSetup):
    def requirements(self) -> list[str]:
        return ["bionty", "pandas>=2"]


def test_lamindb_setup_requirements_are_installed_in_the_step_virtualenv(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    op = _single_task(
        task.lamindb_virtualenv(lamindb_setup=PackagesSetup(), requirements=["pandas==1.5"]), step
    )
    assert op.requirements == ["pandas==1.5"]  # added when the task runs
    with patch.object(lamindb_step, "dag_source", return_value="# dag source"):
        _, seen = _execute_connected(op, make_context())
    assert seen["requirements"] == ["pandas==1.5", "bionty", "lamindb==2.11.0"]


def test_lamindb_setup_requirements_are_installed_for_the_flow_operators(
    make_context, lamindb_connection
) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator(task_id="init", lamindb_setup=PackagesSetup(), lamindb_version="2.11.1")
    seen = {}

    def fake_venv_execute(self, context):
        seen["requirements"] = list(self.requirements)
        return "flowuid"

    with patch.object(type(init).__mro__[2], "execute", fake_venv_execute):
        init.execute(make_context())

    assert seen["requirements"] == ["bionty", "pandas>=2", "lamindb==2.11.1"]


def test_lamindb_setup_requirements_must_be_strings(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    class BrokenSetup(execution_setup.LaminDBSetup):
        def requirements(self) -> list[str]:
            return ["bionty", 2]  # type: ignore[list-item]

    op = _single_task(task.lamindb_virtualenv(lamindb_setup=BrokenSetup()), step)
    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        pytest.raises(TypeError, match=r"requirements\(\) must return strings; 2 are not"),
    ):
        _execute_connected(op, make_context())


class CachedSetup(execution_setup.LaminDBSetup):
    def __init__(self, path="/opt/venv-cache") -> None:
        self.path = path

    def venv_cache_path(self):
        return self.path


@pytest.mark.parametrize(
    ("task_path", "expected"),
    [(None, "/opt/venv-cache"), ("/task/cache", "/task/cache"), ("", "")],  # "" opts out
)
def test_lamindb_setup_venv_cache_path_is_the_default_for_steps(
    fake_lamindb: MagicMock, make_context, lamindb_connection, task_path, expected
) -> None:
    op = _single_task(task.lamindb_virtualenv(lamindb_setup=CachedSetup(), venv_cache_path=task_path), step)
    seen: dict = {}

    def fake_venv_execute(self, context):
        seen["venv_cache_path"] = self.venv_cache_path
        return 1

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        op.execute(make_context())
    assert seen["venv_cache_path"] == expected


def test_lamindb_setup_venv_cache_path_reaches_the_flow_operators(make_context, lamindb_connection) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator(task_id="init", lamindb_setup=CachedSetup())
    seen = {}

    def fake_venv_execute(self, context):
        seen["venv_cache_path"] = self.venv_cache_path
        return "flowuid"

    with patch.object(type(init).__mro__[2], "execute", fake_venv_execute):
        init.execute(make_context())
    assert seen["venv_cache_path"] == "/opt/venv-cache"


@pytest.mark.parametrize(
    ("path", "error", "match"),
    [("relative/cache", ValueError, "absolute path"), (42, TypeError, "a path or None")],
)
def test_lamindb_setup_venv_cache_path_must_be_an_absolute_path(path, error, match) -> None:
    with pytest.raises(error, match=match):
        execution_setup.setup_venv_cache_path(CachedSetup(path))


def test_lamindb_setup_venv_cache_path_accepts_path_objects() -> None:
    from pathlib import Path

    assert execution_setup.setup_venv_cache_path(CachedSetup(Path("/opt/cache"))) == "/opt/cache"


def test_lamindb_setup_reaches_the_flow_tasks_of_a_scope() -> None:
    setup = SiteSetup()

    @dag
    def scoped():
        with LaminDBFlow(lamindb_setup=setup):
            task.lamindb_virtualenv(lamindb_setup=setup)(step)()

    built = scoped()
    assert built.get_task("lamindb_flow_init").lamindb_setup is setup
    assert built.get_task("lamindb_flow_finish").lamindb_setup is setup

    @dag(default_args={"lamindb_setup": setup})
    def mapped_dag():
        with LaminDBFlow():
            task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["a"])

    built = mapped_dag()
    init, finish = built.get_task("lamindb_flow_init"), built.get_task("lamindb_flow_finish")
    assert init.lamindb_setup is finish.lamindb_setup is setup


def test_steps_and_flow_tasks_with_different_setups_fail_when_the_scope_closes() -> None:
    @dag
    def scoped_dag():
        with LaminDBFlow():  # without the setup the step needs
            task.lamindb_virtualenv(lamindb_setup=SiteSetup())(step)()

    with pytest.raises(AirflowException, match="different lamindb_setup"):
        scoped_dag()

    @dag
    def mapped_dag():
        with LaminDBFlow():
            task.lamindb_virtualenv(lamindb_setup=SiteSetup())(load).partial(credentials="c", day="d").expand(
                user=["a"]
            )

    with pytest.raises(AirflowException, match="step 'load' and flow task 'lamindb_flow_init'"):
        mapped_dag()

    @dag
    def standalone_dag():  # a standalone step has no flow task to agree with
        task.lamindb_virtualenv(lamindb_setup=SiteSetup())(step)()

    standalone_dag()


def test_a_setup_mismatch_leaves_no_partial_scope_and_the_scope_can_be_entered_again() -> None:
    def mismatched_scope() -> None:
        with LaminDBFlow():
            task.lamindb_virtualenv(step, task_id="fine")()
            task.lamindb_virtualenv(step, task_id="differs", lamindb_setup=SiteSetup())()

    with DAG("d", schedule=None) as built:
        with pytest.raises(AirflowException, match="different lamindb_setup"):
            mismatched_scope()

        # nothing was scoped or added: not the earlier member, not the flow tasks
        assert set(built.task_ids) == {"fine", "differs"}
        assert built.get_task("fine").lamindb_scope is None
        assert built.get_task("differs").lamindb_scope is None
        assert built.get_task("fine").downstream_task_ids == set()

        with LaminDBFlow(lamindb_setup=SiteSetup()):  # the default scope is free again
            task.lamindb_virtualenv(step, task_id="again", lamindb_setup=SiteSetup())()

    assert built.get_task("again").lamindb_scope == ""
    assert {"lamindb_flow_init", "lamindb_flow_finish"} <= set(built.task_ids)


def test_pod_steps_run_the_bootstrap_but_not_execution_env(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    """Pods get their environment from the pod spec; a setup written for virtualenvs mustn't break them."""
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    setup = SiteSetup()
    op = _single_task(
        task.lamindb_kubernetes(image="python:3.12", track=False, connect=True, lamindb_setup=setup), step
    )
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = self.env_vars

    with patch.object(type(op).__mro__[2], "execute", fake_pod_execute):
        op.execute(make_context())

    assert setup.contexts == []  # execution_env isn't called for pods
    assert "s3cret-token" not in repr(seen)
    assert "SITE_BOOTSTRAPPED" in seen["source"]


def test_lamindb_setup_from_default_args_reaches_every_lamindb_task() -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowFinishOperator

    setup = SiteSetup()

    @dag(default_args={"lamindb_setup": setup})
    def test_dag():
        with LaminDBFlow():
            task.lamindb_virtualenv(step)()
        LaminDBFlowFinishOperator(task_id="other_finish")

    built = test_dag()
    assert built.get_task("step").lamindb_setup is setup
    assert built.get_task("lamindb_flow_init").lamindb_setup is setup
    assert built.get_task("other_finish").lamindb_setup is setup


def test_lamindb_setup_from_default_args_reaches_pod_steps() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    setup = SiteSetup()

    @dag(default_args={"lamindb_setup": setup})
    def test_dag():
        task.lamindb_kubernetes(image="python:3.12")(step)()

    assert test_dag().get_task("step").lamindb_setup is setup


def test_sensitive_looking_names_are_still_recorded(fake_lamindb: MagicMock, make_context) -> None:
    """Airflow matches sensitive names by substring; only values Airflow knows to be secret count."""

    def analyze(tokenizer, secretory_cells, max_tokens):
        return tokenizer

    fake_lamindb.flow_run = MagicMock()

    @dag
    def test_dag():
        task.lamindb_virtualenv(lamindb_conn_id=None)(analyze)("bpe", ["acinar"], 512)

    op = test_dag().get_task("analyze")
    context = make_context()
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

    def fake_venv_execute(self, context):
        namespace: dict = {}
        exec(self.get_python_source(), namespace)
        return namespace["analyze"](*self.op_args, **self.op_kwargs)

    with (
        patch.object(lamindb_step, "dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
    ):
        op.execute(context)
    assert fake_lamindb.track.call_args.kwargs["params"] == {
        "tokenizer": "bpe",
        "secretory_cells": ["acinar"],
        "max_tokens": 512,
    }


@pytest.mark.parametrize(("pin", "expected"), [(2.11, "lamindb==2.11"), ("2.11.0", "lamindb==2.11.0")])
def test_natively_rendered_lamindb_version(
    fake_lamindb: MagicMock, make_context, lamindb_connection, pin, expected
):
    fake_lamindb.flow_run = MagicMock()

    @dag(render_template_as_native_obj=True, params={"pin": pin})
    def test_dag():
        task.lamindb_virtualenv(lamindb_version="{{ params.pin }}")(step)()

    op = test_dag().get_task("step")
    context = make_context()
    context["params"] = {"pin": pin}
    op.render_template_fields(context)
    assert op.lamindb_version == pin
    context["ti"].xcom_pull.return_value = None

    _, seen = _execute_in_fake_venv(op, context)
    assert seen["requirements"][0] == expected


def test_literal_lamindb_versions_may_be_numbers() -> None:
    assert _single_task(task.lamindb_virtualenv(lamindb_version=2.11), step).lamindb_version == 2.11
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.7 is not supported"):
        _single_task(task.lamindb_virtualenv(lamindb_version=2.7), step)


@pytest.mark.parametrize(
    ("space", "expected"), [(None, None), ("", None), ("  ", None), (" lab ", "lab"), (7, "7")]
)
def test_space_names(space, expected) -> None:
    assert execution_parameters.space_name(space) == expected


def test_dags_with_a_lamindb_setup_serialize_the_same_on_every_parse() -> None:
    """Airflow 3 versions DAGs by their serialized form; an object address in it changes every parse."""
    import json

    from airflow.serialization.serialized_objects import DagSerialization

    def serialized() -> str:
        @dag(default_args={"lamindb_setup": SiteSetup()})
        def test_dag():
            task.lamindb_virtualenv(step)()

        return json.dumps(DagSerialization.to_dict(test_dag()), sort_keys=True, default=str)

    first = serialized()
    assert first == serialized()
    assert "test_decorators.SiteSetup()" in first


class ConfiguredSetup(execution_setup.LaminDBSetup):
    """Stands for an installation's [lamindb] setup_class."""

    def bootstrap_source(self) -> str:
        return "import os\nos.environ['SITE_CONFIGURED'] = '1'"

    def execution_env(self, context) -> dict[str, str]:
        return {"SITE_CONFIGURED_TOKEN": "configured"}


@pytest.fixture
def configured_setup(monkeypatch) -> execution_setup.LaminDBSetup:
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.ConfiguredSetup")
    execution_setup.configured_setup.cache_clear()
    setup = execution_setup.configured_setup()
    assert type(setup).__name__ == "ConfiguredSetup"
    return setup


def test_the_configured_setup_prepares_every_lamindb_task(
    fake_lamindb: MagicMock, make_context, lamindb_connection, configured_setup
) -> None:
    fake_lamindb.flow_run = MagicMock()

    @dag
    def scoped():
        with LaminDBFlow():
            task.lamindb_virtualenv(step)()

    op = scoped().get_task("step")
    assert op.lamindb_setup is None  # resolved when the task runs, not stored on it
    context = make_context()
    context["ti"].xcom_pull.return_value = None

    with patch.object(lamindb_step, "dag_source", return_value="# dag source"):
        _, seen = _execute_connected(op, context)

    assert "SITE_CONFIGURED" in seen["source"]
    assert seen["env_vars"]["SITE_CONFIGURED_TOKEN"] == "configured"
    init = op.get_dag().get_task("lamindb_flow_init")
    assert execution_setup.setup_bootstrap_source(init.lamindb_setup).endswith(
        "os.environ['SITE_CONFIGURED'] = '1'"
    )
    assert execution_setup.configured_setup() is configured_setup  # loaded once per process


def test_dags_and_tasks_override_the_configured_setup(configured_setup) -> None:
    site = SiteSetup()

    @dag(default_args={"lamindb_setup": site})
    def overridden():
        with LaminDBFlow():
            task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["a"])

    built = overridden()
    assert execution_setup.resolve_setup(built.get_task("lamindb_flow_init").lamindb_setup) is site

    opted_out = _single_task(task.lamindb_virtualenv(lamindb_setup=execution_setup.LaminDBSetup()), step)
    assert execution_setup.setup_bootstrap_source(opted_out.lamindb_setup) == ""
    assert execution_setup.setup_execution_env(opted_out.lamindb_setup, {}) == {}


def test_a_step_overriding_the_configured_setup_alone_fails_when_the_scope_closes(configured_setup) -> None:
    @dag
    def one_step_differs():
        with LaminDBFlow():
            task.lamindb_virtualenv(step)()
            task.lamindb_virtualenv(lamindb_setup=SiteSetup(), task_id="other")(step)()

    with pytest.raises(AirflowException, match="step 'other' and flow task 'lamindb_flow_init'"):
        one_step_differs()


@pytest.mark.parametrize(
    ("path", "error"),
    [
        ("no_such_module.Setup", "can't be imported"),
        (f"{__name__}.NoSuchClass", "can't be imported"),
        ("NoDots", "can't be imported"),
        (f"{__name__}.step", "is not a subclass of LaminDBSetup"),
    ],
)
def test_a_broken_setup_class_fails_when_the_dag_is_parsed(monkeypatch, path: str, error: str) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", path)
    execution_setup.configured_setup.cache_clear()
    with pytest.raises(AirflowException, match=error):
        _single_task(task.lamindb_virtualenv, step)
    with pytest.raises(AirflowException, match=error):
        LaminDBFlowInitOperator(task_id="init")
    # untracked tasks don't use the setup
    _single_task(task.lamindb_virtualenv(track=False), step)


def test_equivalent_setups_are_the_same_setup() -> None:
    """Task groups deep-copy their default_args, and steps may each create their own instance."""
    from airflow.sdk import TaskGroup

    assert SiteSetup() == SiteSetup()
    assert hash(SiteSetup()) == hash(SiteSetup())
    assert SiteSetup() != ConfiguredSetup()
    assert execution_setup.LaminDBSetup() != SiteSetup()

    @dag
    def two_instances():
        shared = {"lamindb_setup": SiteSetup()}
        with LaminDBFlow(lamindb_setup=SiteSetup()):
            with TaskGroup("a", default_args=shared):
                task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["x"])
            with TaskGroup("b", default_args=shared):
                task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["y"])
            task.lamindb_virtualenv(lamindb_setup=SiteSetup())(step)()

    built = two_instances()
    assert built.get_task("lamindb_flow_init").downstream_task_ids == {
        "a.load",
        "b.load",
        "step",
        "lamindb_flow_finish",
    }


def test_mapped_steps_get_their_task_groups_setup() -> None:
    from airflow.sdk import TaskGroup

    @dag
    def grouped():
        with (
            LaminDBFlow(lamindb_setup=SiteSetup()),
            TaskGroup("g", default_args={"lamindb_setup": SiteSetup()}),
        ):
            task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["a"])

    built = grouped()
    mapped = built.get_task("g.load")
    assert lamindb_step.task_setting(mapped, "lamindb_setup") == SiteSetup()
    assert mapped.unmap({"op_kwargs": {"user": "a"}}).lamindb_setup == SiteSetup()  # what Airflow runs
    init, finish = built.get_task("lamindb_flow_init"), built.get_task("lamindb_flow_finish")
    assert init.lamindb_setup == finish.lamindb_setup == SiteSetup()


def test_a_single_step_may_opt_out_of_the_setup(configured_setup) -> None:
    @dag
    def opted_out():
        with LaminDBFlow():
            task.lamindb_virtualenv(step)()
            task.lamindb_virtualenv(lamindb_setup=execution_setup.LaminDBSetup(), task_id="plain")(step)()
            task.lamindb_virtualenv(lamindb_setup=execution_setup.LaminDBSetup())(load).partial(
                credentials="c", day="d"
            ).expand(user=["a"])

    built = opted_out()
    assert (
        execution_setup.resolve_setup(built.get_task("lamindb_flow_init").lamindb_setup) is configured_setup
    )
    assert built.get_task("lamindb_flow_init").downstream_task_ids == {
        "step",
        "plain",
        "load",
        "lamindb_flow_finish",
    }


@pytest.mark.parametrize(
    "bootstrap",
    [
        "\n        import os\n        os.environ['BOOTSTRAPPED'] = '1'\n    ",  # indented, as in a class
        "from __future__ import annotations\nimport os\nos.environ['BOOTSTRAPPED'] = '1'",
    ],
)
def test_bootstrap_source_may_be_indented_or_use_future_imports(bootstrap: str, monkeypatch) -> None:
    monkeypatch.delenv("BOOTSTRAPPED", raising=False)
    source = execution_source.build_remote_step_source(
        user_source="def probe():\n    return 1\n",
        callable_name="probe",
        config={},
        bootstrap=bootstrap,
    )
    namespace: dict = {}
    exec(compile(source, "<script>", "exec"), namespace)
    assert os.environ["BOOTSTRAPPED"] == "1"
    assert "os" not in namespace  # the bootstrap runs in its own namespace


def test_execution_env_must_return_strings(fake_lamindb: MagicMock, make_context, lamindb_connection) -> None:
    class NumberSetup(execution_setup.LaminDBSetup):
        def execution_env(self, context) -> dict[str, str]:
            return {"RETRIES": 3, "NAME": "ok"}  # type: ignore[dict-item]

    op = _single_task(task.lamindb_virtualenv(track=False, connect=True, lamindb_setup=NumberSetup()), step)
    with pytest.raises(
        TypeError, match=r"NumberSetup\(\)\.execution_env\(\) must return strings; the values of 'RETRIES'"
    ):
        _execute_connected(op, make_context())


@pytest.mark.parametrize(("operator", "fails"), [("init", True), ("finish", False)])
def test_flow_finish_closes_the_run_even_if_execution_env_fails(
    make_context, lamindb_connection, operator, fails
):
    from airflow.providers.lamindb.operators.flow import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

    class BrokenSetup(execution_setup.LaminDBSetup):
        def execution_env(self, context) -> dict[str, str]:
            raise KeyError("site_token")

    operator_class = {"init": LaminDBFlowInitOperator, "finish": LaminDBFlowFinishOperator}[operator]
    flow_task = operator_class(task_id=operator, lamindb_setup=BrokenSetup())
    ran = []

    def fake_venv_execute(self, context):
        ran.append(dict(self.env_vars))
        return "flowuid"

    with patch.object(type(flow_task).__mro__[2], "execute", fake_venv_execute):
        if fails:  # without its setup, the steps couldn't run either
            with pytest.raises(KeyError, match="site_token"):
                flow_task.execute(make_context())
        else:
            flow_task.execute(make_context())
    assert len(ran) == (0 if fails else 1)


def test_a_broken_setup_class_fails_parsing_for_mapped_steps_too(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", "no_such_module.Setup")
    execution_setup.configured_setup.cache_clear()

    @dag
    def only_mapped():
        task.lamindb_virtualenv(load).partial(credentials="c", day="d").expand(user=["a"])

    with pytest.raises(AirflowException, match="can't be imported"):
        only_mapped()


def test_the_setup_class_option_is_read_once_per_process(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.ConfiguredSetup")
    execution_setup.configured_setup.cache_clear()
    first = execution_setup.configured_setup()
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.SiteSetup")
    assert execution_setup.configured_setup() is first


def test_lamindb_branch_is_templated_and_reaches_steps_and_flow_operators(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    @dag(default_args={"lamindb_branch": "{{ params.branch }}"})
    def test_dag():
        LaminDBFlowInitOperator()
        task.lamindb_virtualenv(step)()

    built = test_dag()
    init, op = built.get_task("lamindb_flow_init"), built.get_task("step")
    assert "lamindb_branch" in init.template_fields
    assert "lamindb_branch" in op.template_fields
    context = make_context()
    context["params"] = {"branch": " feature-x "}
    init.render_template_fields(context)
    assert init._runtime_kwargs(context)["branch"] == "feature-x"

    fake_lamindb.flow_run = MagicMock(space_id=1, branch_id=1)
    feature = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    feature.id = 2
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None
    _execute_in_fake_venv(op, context)
    fake_lamindb.Branch.filter.assert_called_once()


def test_flow_scopes_do_not_copy_the_branch_of_their_steps() -> None:
    @dag
    def test_dag():
        with LaminDBFlow(lamindb_branch="feature-x"):
            task.lamindb_virtualenv(lamindb_branch="{{ params.branch }}")(step)()

    built = test_dag()
    assert built.get_task("lamindb_flow_init").lamindb_branch == "feature-x"
    assert built.get_task("step").lamindb_branch == "{{ params.branch }}"


def test_lamindb_branch_reaches_pod_steps(fake_lamindb: MagicMock, make_context, lamindb_connection) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(
        task.lamindb_kubernetes(image="python:3.12", track=False, connect=True, lamindb_branch="feature-x"),
        step,
    )
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()

    with patch.object(type(op).__mro__[2], "execute", fake_pod_execute):
        op.execute(make_context())
    assert "'branch': 'feature-x'" in seen["source"]


def test_mapped_pod_steps_join_a_scope() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")

    @dag
    def test_dag():
        with LaminDBFlow(lamindb_instance="owner/name"):
            task.lamindb_kubernetes(image="python:3.12", lamindb_instance="owner/name")(load).partial(
                credentials="c", day="d"
            ).expand(user=["a", "b"])

    built = test_dag()
    init = built.get_task("lamindb_flow_init")
    assert init.downstream_task_ids == {"load", "lamindb_flow_finish"}
    assert init.lamindb_instance == "owner/name"
    assert built.get_task("load").partial_kwargs["lamindb_scope"] == ""
