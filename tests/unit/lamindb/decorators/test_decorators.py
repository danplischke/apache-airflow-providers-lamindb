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
    op = _single_task(task.lamindb_venv(track=False, connect=True, auto_flow=True), step)
    assert op.get_dag().task_ids == ["step"]  # no flow tasks

    result, seen = _execute_connected(op, make_context())

    assert result == 1
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()  # no flow run needed
    assert "run_connected" in seen["source"]
    assert seen["requirements"] == ["lamindb-core==2.11.0", "numpy", "pandas>=2.0.0", "pandera>=0.24.0"]
    assert seen["env_vars"]["LAMIN_API_KEY"] == "api-key"
    assert not seen["settings_dir_exists_after"]
    assert op.env_vars is None


def test_connect_only_step_cleans_up_when_the_function_fails(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    def fail():
        raise ValueError("boom")

    op = _single_task(task.lamindb_venv(track=False, connect=True, task_id="step"), fail)

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
    op = _single_task(task.lamindb_venv(track=False, connect=True), step)
    with pytest.raises(AirflowNotFoundException, match="'lamindb_default' connection"):
        op.execute(make_context())


def test_connect_only_step_rejects_instances_on_older_lamindb(
    make_context, lamindb_connection, monkeypatch
) -> None:
    def too_old(self):
        raise LaminDBVersionError("runs lamindb 2.7.0")

    monkeypatch.setattr(remote.RemoteLaminDB, "instance_lamindb_version", too_old)
    op = _single_task(task.lamindb_venv(track=False, connect=True), step)
    with pytest.raises(LaminDBVersionError, match=r"2\.7\.0"):
        _execute_connected(op, make_context())


def test_connect_only_mapped_steps_resolve_their_own_instance(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    @dag
    def test_dag():
        task.lamindb_venv(track=False, connect=True, lamindb_instance="owner/site{{ ti.map_index }}")(
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


def test_task_lamindb_k8s_connect_only_uses_the_pod_credentials(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    op = _single_task(task.lamindb_k8s(image="python:3.12", track=False, connect=True), step)
    seen = {}

    def fake_pod_execute(self, context):
        seen["source"] = self.get_python_source()
        seen["env_vars"] = self.env_vars

    with patch.object(type(op).__mro__[2], "execute", fake_pod_execute):
        op.execute(make_context())

    assert (
        "run_connected(_lamindb_airflow_user_fn, args, kwargs, **{'instance': 'owner/name', 'space': None})"
        in seen["source"]
    )
    assert "api-key" not in repr(seen)


def test_templated_lamindb_version_is_checked_once_rendered(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _single_task(task.lamindb_venv(lamindb_version="{{ params.pin }}"), step)  # parses
    assert "lamindb_version" in op.template_fields

    context = make_context()
    context["params"] = {"pin": " 2.10.1 "}
    op.render_template_fields(context)
    _, seen = _execute_in_fake_venv(op, context)
    assert seen["requirements"][0] == "lamindb-core==2.10.1"


@pytest.mark.parametrize(
    ("pin", "error"),
    [("2.8.0", r"lamindb 2\.8\.0 is not supported"), ("None", "'None' is not a lamindb version")],
)
def test_rendered_lamindb_version_fails_before_installing(
    fake_lamindb: MagicMock, make_context, lamindb_connection, pin: str, error: str
) -> None:
    op = _single_task(task.lamindb_venv(lamindb_version="{{ params.pin }}"), step)
    context = make_context()
    context["params"] = {"pin": pin}
    op.render_template_fields(context)

    with (
        patch.object(remote, "dag_source", return_value="# dag source"),
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
        task.lamindb_venv(lamindb_version="2.{{ 10 + ti.map_index }}.0", venv_cache_path="/tmp/venvs")(
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
            patch.object(remote, "dag_source", return_value="# dag source"),
            patch.object(type(op).__mro__[2], "execute", fake_venv_execute),
        ):
            op.execute(context)

    assert [r[0] for r in requirements] == [
        "lamindb-core==2.10.0",
        "lamindb-core==2.11.0",
        "lamindb-core==2.12.0",
    ]
    assert "pandas>=2.0.0" in requirements[0]  # lamindb 2.10 and newer allow pandas 3
    assert len(set(hashes)) == 3


def test_auto_flow_does_not_copy_a_templated_lamindb_version() -> None:
    @dag
    def test_dag():
        task.lamindb_venv(
            auto_flow=True,
            lamindb_version="{{ params.pin }}",
            requirements=["lamindb-core=={{ params.pin }}", "lamindb[bionty]==2.11.0"],
        )(step)()

    built = test_dag()
    init = built.get_task("lamindb_flow_init")
    assert init.lamindb_version is None  # the instance's version
    assert init.requirements == ["lamindb[bionty]==2.11.0"]


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
        task.lamindb_venv(lamindb_version="{{ params.pin }}")(load).partial(credentials="c", day="d").expand(
            user=["a"]
        )

    serialized = DagSerialization.to_dict(test_dag())
    assert "{{ params.pin }}" in str(serialized)


def test_lamindb_space_is_templated_and_reaches_the_step(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _single_task(task.lamindb_venv(lamindb_space="{{ params.space }}"), step)
    context = make_context()
    context["params"] = {"space": " lab "}
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

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
        task.lamindb_venv(step)()

    built = test_dag()
    assert built.get_task("step").lamindb_space == "lab"
    assert built.get_task("lamindb_flow_init").lamindb_space == "lab"
    assert "lamindb_space" in built.get_task("lamindb_flow_init").template_fields


@pytest.mark.parametrize(("space", "flow_space"), [("lab", "lab"), ("{{ params.space }}", None)])
def test_auto_flow_copies_only_a_literal_space(space: str, flow_space: str | None) -> None:
    op = _single_task(task.lamindb_venv(auto_flow=True, lamindb_space=space), step)
    assert op.get_dag().get_task("lamindb_flow_init").lamindb_space == flow_space


def test_flow_init_starts_the_flow_run_in_its_space(make_context, lamindb_connection) -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator

    init = LaminDBFlowInitOperator(task_id="init", lamindb_space="lab")
    assert init._runtime_kwargs(make_context())["space"] == "lab"
    init.lamindb_space = "  "  # rendered empty: lamindb's default space
    assert init._runtime_kwargs(make_context())["space"] is None


class SiteSetup(remote.LaminDBSetup):
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
    op = _single_task(task.lamindb_venv(lamindb_setup=setup, env_vars={"OVERRIDDEN": "task"}), step)
    assert setup.contexts == []  # nothing resolved when the DAG is parsed
    context = make_context()
    op.render_template_fields(context)
    context["ti"].xcom_pull.return_value = None

    with patch.object(remote, "dag_source", return_value="# dag source"):
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
    op = _single_task(task.lamindb_venv(track=False, connect=True, lamindb_setup=SiteSetup()), step)
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


def test_lamindb_setup_reaches_auto_wired_and_helper_wired_flow_tasks() -> None:
    from airflow.providers.lamindb.operators.flow import wire_lamindb_flow

    setup = SiteSetup()
    op = _single_task(task.lamindb_venv(auto_flow=True, lamindb_setup=setup), step)
    assert op.get_dag().get_task("lamindb_flow_init").lamindb_setup is setup

    @dag(default_args={"lamindb_setup": setup})
    def mapped_dag():
        task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["a"])

    built = mapped_dag()
    init, finish = wire_lamindb_flow(built)
    assert init.lamindb_setup is finish.lamindb_setup is setup


def test_steps_and_flow_tasks_with_different_setups_fail_when_wired() -> None:
    from airflow.providers.lamindb.operators.flow import LaminDBFlowInitOperator, wire_lamindb_flow

    @dag
    def auto_dag():
        LaminDBFlowInitOperator()
        task.lamindb_venv(auto_flow=True, lamindb_setup=SiteSetup())(step)()

    with pytest.raises(AirflowException, match="different lamindb_setup"):
        auto_dag()

    @dag
    def helper_dag():
        LaminDBFlowInitOperator()  # without the setup the step needs
        task.lamindb_venv(lamindb_setup=SiteSetup())(load).partial(credentials="c", day="d").expand(
            user=["a"]
        )

    with pytest.raises(AirflowException, match="step 'load' and flow task 'lamindb_flow_init'"):
        wire_lamindb_flow(helper_dag())


def test_pod_steps_run_the_bootstrap_but_not_execution_env(
    fake_lamindb: MagicMock, make_context, lamindb_connection
) -> None:
    """Pods get their environment from the pod spec; a setup written for virtualenvs mustn't break them."""
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    setup = SiteSetup()
    op = _single_task(
        task.lamindb_k8s(image="python:3.12", track=False, connect=True, lamindb_setup=setup), step
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
        task.lamindb_venv(auto_flow=True)(step)()
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
        task.lamindb_k8s(image="python:3.12")(step)()

    assert test_dag().get_task("step").lamindb_setup is setup


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


@pytest.mark.parametrize(
    ("pin", "expected"), [(2.11, "lamindb-core==2.11"), ("2.11.0", "lamindb-core==2.11.0")]
)
def test_natively_rendered_lamindb_version(
    fake_lamindb: MagicMock, make_context, lamindb_connection, pin, expected
):
    fake_lamindb.flow_run = MagicMock()

    @dag(render_template_as_native_obj=True, params={"pin": pin})
    def test_dag():
        task.lamindb_venv(lamindb_version="{{ params.pin }}")(step)()

    op = test_dag().get_task("step")
    context = make_context()
    context["params"] = {"pin": pin}
    op.render_template_fields(context)
    assert op.lamindb_version == pin
    context["ti"].xcom_pull.return_value = None

    _, seen = _execute_in_fake_venv(op, context)
    assert seen["requirements"][0] == expected


def test_literal_lamindb_versions_may_be_numbers() -> None:
    assert _single_task(task.lamindb_venv(lamindb_version=2.11), step).lamindb_version == 2.11
    with pytest.raises(LaminDBVersionError, match=r"lamindb 2\.7 is not supported"):
        _single_task(task.lamindb_venv(lamindb_version=2.7), step)


@pytest.mark.parametrize(
    ("space", "expected"), [(None, None), ("", None), ("  ", None), (" lab ", "lab"), (7, "7")]
)
def test_space_names(space, expected) -> None:
    assert remote.space_name(space) == expected


def test_dags_with_a_lamindb_setup_serialize_the_same_on_every_parse() -> None:
    """Airflow 3 versions DAGs by their serialized form; an object address in it changes every parse."""
    import json

    from airflow.serialization.serialized_objects import DagSerialization

    def serialized() -> str:
        @dag(default_args={"lamindb_setup": SiteSetup()})
        def test_dag():
            task.lamindb_venv(step)()

        return json.dumps(DagSerialization.to_dict(test_dag()), sort_keys=True, default=str)

    first = serialized()
    assert first == serialized()
    assert "test_decorators.SiteSetup()" in first


class ConfiguredSetup(remote.LaminDBSetup):
    """Stands for an installation's [lamindb] setup_class."""

    def bootstrap_source(self) -> str:
        return "import os\nos.environ['SITE_CONFIGURED'] = '1'"

    def execution_env(self, context) -> dict[str, str]:
        return {"SITE_CONFIGURED_TOKEN": "configured"}


@pytest.fixture
def configured_setup(monkeypatch) -> remote.LaminDBSetup:
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.ConfiguredSetup")
    remote.configured_setup.cache_clear()
    setup = remote.configured_setup()
    assert type(setup).__name__ == "ConfiguredSetup"
    return setup


def test_the_configured_setup_prepares_every_lamindb_task(
    fake_lamindb: MagicMock, make_context, lamindb_connection, configured_setup
) -> None:
    fake_lamindb.flow_run = MagicMock()
    op = _single_task(task.lamindb_venv(auto_flow=True), step)
    assert op.lamindb_setup is None  # resolved when the task runs, not stored on it
    context = make_context()
    context["ti"].xcom_pull.return_value = None

    with patch.object(remote, "dag_source", return_value="# dag source"):
        _, seen = _execute_connected(op, context)

    assert "SITE_CONFIGURED" in seen["source"]
    assert seen["env_vars"]["SITE_CONFIGURED_TOKEN"] == "configured"
    init = op.get_dag().get_task("lamindb_flow_init")
    assert remote.setup_bootstrap_source(init.lamindb_setup).endswith("os.environ['SITE_CONFIGURED'] = '1'")
    assert remote.configured_setup() is configured_setup  # loaded once per process


def test_dags_and_tasks_override_the_configured_setup(configured_setup) -> None:
    from airflow.providers.lamindb.operators.flow import wire_lamindb_flow

    site = SiteSetup()

    @dag(default_args={"lamindb_setup": site})
    def overridden():
        task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["a"])
        wire_lamindb_flow()

    built = overridden()
    assert remote.resolve_setup(built.get_task("lamindb_flow_init").lamindb_setup) is site

    opted_out = _single_task(task.lamindb_venv(lamindb_setup=remote.LaminDBSetup()), step)
    assert remote.setup_bootstrap_source(opted_out.lamindb_setup) == ""
    assert remote.setup_execution_env(opted_out.lamindb_setup, {}) == {}


def test_a_step_overriding_the_configured_setup_alone_fails_when_wired(configured_setup) -> None:
    @dag
    def one_step_differs():
        task.lamindb_venv(auto_flow=True)(step)()
        task.lamindb_venv(auto_flow=True, lamindb_setup=SiteSetup(), task_id="other")(step)()

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
    remote.configured_setup.cache_clear()
    with pytest.raises(AirflowException, match=error):
        _single_task(task.lamindb_venv, step)
    with pytest.raises(AirflowException, match=error):
        LaminDBFlowInitOperator(task_id="init")
    # untracked tasks don't use the setup
    _single_task(task.lamindb_venv(track=False), step)


def test_equivalent_setups_are_the_same_setup() -> None:
    """Task groups deep-copy their default_args, and steps may each create their own instance."""
    from airflow.sdk import TaskGroup

    from airflow.providers.lamindb.operators.flow import wire_lamindb_flow

    assert SiteSetup() == SiteSetup()
    assert hash(SiteSetup()) == hash(SiteSetup())
    assert SiteSetup() != ConfiguredSetup()
    assert remote.LaminDBSetup() != SiteSetup()

    @dag
    def two_instances():
        shared = {"lamindb_setup": SiteSetup()}
        with TaskGroup("a", default_args=shared):
            task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["x"])
        with TaskGroup("b", default_args=shared):
            task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["y"])
        task.lamindb_venv(lamindb_setup=SiteSetup())(step)()
        wire_lamindb_flow(lamindb_setup=SiteSetup())

    two_instances()


def test_mapped_steps_get_their_task_groups_setup() -> None:
    from airflow.sdk import TaskGroup

    from airflow.providers.lamindb.operators.flow import wire_lamindb_flow

    @dag
    def grouped():
        with TaskGroup("g", default_args={"lamindb_setup": SiteSetup()}):
            task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["a"])

    built = grouped()
    mapped = built.get_task("g.load")
    assert remote.task_setting(mapped, "lamindb_setup") == SiteSetup()
    assert mapped.unmap({"op_kwargs": {"user": "a"}}).lamindb_setup == SiteSetup()  # what Airflow runs
    init, finish = wire_lamindb_flow(built)
    assert init.lamindb_setup == finish.lamindb_setup == SiteSetup()


def test_a_single_step_may_opt_out_of_the_setup(configured_setup) -> None:
    from airflow.providers.lamindb.operators.flow import wire_lamindb_flow

    @dag
    def opted_out():
        task.lamindb_venv(auto_flow=True)(step)()
        task.lamindb_venv(auto_flow=True, lamindb_setup=remote.LaminDBSetup(), task_id="plain")(step)()
        task.lamindb_venv(lamindb_setup=remote.LaminDBSetup())(load).partial(credentials="c", day="d").expand(
            user=["a"]
        )
        wire_lamindb_flow()

    built = opted_out()
    assert remote.resolve_setup(built.get_task("lamindb_flow_init").lamindb_setup) is configured_setup


@pytest.mark.parametrize(
    "bootstrap",
    [
        "\n        import os\n        os.environ['BOOTSTRAPPED'] = '1'\n    ",  # indented, as in a class
        "from __future__ import annotations\nimport os\nos.environ['BOOTSTRAPPED'] = '1'",
    ],
)
def test_bootstrap_source_may_be_indented_or_use_future_imports(bootstrap: str, monkeypatch) -> None:
    monkeypatch.delenv("BOOTSTRAPPED", raising=False)
    source = remote.build_remote_step_source(
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
    class NumberSetup(remote.LaminDBSetup):
        def execution_env(self, context) -> dict[str, str]:
            return {"RETRIES": 3, "NAME": "ok"}  # type: ignore[dict-item]

    op = _single_task(task.lamindb_venv(track=False, connect=True, lamindb_setup=NumberSetup()), step)
    with pytest.raises(
        TypeError, match=r"NumberSetup\(\)\.execution_env\(\) must return strings; the values of 'RETRIES'"
    ):
        _execute_connected(op, make_context())


@pytest.mark.parametrize(("operator", "fails"), [("init", True), ("finish", False)])
def test_flow_finish_closes_the_run_even_if_execution_env_fails(
    make_context, lamindb_connection, operator, fails
):
    from airflow.providers.lamindb.operators.flow import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

    class BrokenSetup(remote.LaminDBSetup):
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
    remote.configured_setup.cache_clear()

    @dag
    def only_mapped():
        task.lamindb_venv(load).partial(credentials="c", day="d").expand(user=["a"])

    with pytest.raises(AirflowException, match="can't be imported"):
        only_mapped()


def test_the_setup_class_option_is_read_once_per_process(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.ConfiguredSetup")
    remote.configured_setup.cache_clear()
    first = remote.configured_setup()
    monkeypatch.setenv("AIRFLOW__LAMINDB__SETUP_CLASS", f"{__name__}.SiteSetup")
    assert remote.configured_setup() is first
