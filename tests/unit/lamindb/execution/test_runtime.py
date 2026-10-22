from __future__ import annotations

import ast
import inspect
from unittest.mock import MagicMock

import pytest

from airflow.providers.lamindb.execution import runtime


def test_runtime_is_self_contained() -> None:
    """The module is shipped as source to interpreters without Airflow or this package."""
    tree = ast.parse(inspect.getsource(runtime))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert imported <= {"datetime", "inspect", "logging", "pathlib", "re", "lamindb"}
    top_level = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert all(node.module == "datetime" for node in top_level if isinstance(node, ast.ImportFrom))
    assert not any(isinstance(node, ast.Import) for node in top_level)


def test_connect_uses_instance_only_when_given(fake_lamindb: MagicMock) -> None:
    assert runtime.connect() is fake_lamindb
    fake_lamindb.connect.assert_not_called()
    runtime.connect("owner/name")
    fake_lamindb.connect.assert_called_once_with("owner/name")


@pytest.mark.parametrize("version", ["2.8.0", "1.17.0"])
def test_connect_rejects_older_lamindb(fake_lamindb: MagicMock, version: str) -> None:
    fake_lamindb.__version__ = version
    with pytest.raises(
        RuntimeError, match=rf"lamindb {version} is installed, .* needs lamindb 2\.8\.1 or newer"
    ):
        runtime.connect("owner/name")
    fake_lamindb.connect.assert_not_called()


@pytest.mark.parametrize(
    ("version", "supported"),
    [
        ("2.8.1", True),
        ("2.8.1a3", True),  # pre-releases of 2.8.1 count as 2.8.1
        ("2.8.2", True),
        ("2.9", True),
        ("2.10.0", True),
        ("2.11.0", True),
        ("2.12.0.dev4+g1a2b3c", True),
        ("3.0.0", True),
        ("10.0", True),
        ("2.8.0", False),
        ("2.8", False),
        ("2.7.9", False),
        ("1.17.0", False),
        ("", False),
        ("unknown", False),
    ],
)
def test_lamindb_version_supported(version: str, supported: bool) -> None:
    assert runtime.lamindb_version_supported(version) is supported


def test_start_flow_run_tracks_dag_file_and_tags_run(fake_lamindb: MagicMock) -> None:
    run = runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {"a": 1}, instance="owner/name")

    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_called_once_with(
        path="/dags/my_dag.py", entrypoint="my_dag", params={"a": 1}, new_run=True, stream_tracking=False
    )
    assert run.reference == "my_dag/run_1"
    assert run.reference_type == runtime.FLOW_RUN_REFERENCE_TYPE
    run.save.assert_called()
    assert fake_lamindb.context.run is None
    run.transform._update_source_code_from_path.assert_not_called()


def test_start_flow_run_saves_source_of_new_transform(fake_lamindb: MagicMock) -> None:
    def track(*args, **kwargs) -> None:
        fake_lamindb.context._run = MagicMock(**{"transform.hash": None})

    fake_lamindb.track.side_effect = track
    run = runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {})
    run.transform._update_source_code_from_path.assert_called_once()
    run.transform.save.assert_called_once()


def test_start_flow_run_restarts_existing_run_on_retry(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = existing = MagicMock(finished_at="yesterday")
    assert runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {}) is existing
    fake_lamindb.track.assert_not_called()
    assert existing._status_code == -2
    assert existing.finished_at is None
    # on every branch but the archive and the trash, not only lamindb's current one
    fake_lamindb.Run.filter.assert_called_with(
        reference="my_dag/run_1", reference_type=runtime.FLOW_RUN_REFERENCE_TYPE, branch_id__gte=1
    )


def test_start_flow_run_refuses_to_clobber_global_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.context._run = MagicMock()
    with pytest.raises(RuntimeError, match="already set"):
        runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {})


@pytest.mark.parametrize(("success", "status"), [(True, 0), (False, 1)])
def test_finish_flow_run(fake_lamindb: MagicMock, success: bool, status: int) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(finished_at=None)
    assert runtime.finish_flow_run("my_dag/run_1", success) is flow_run
    assert flow_run._status_code == status
    assert flow_run.finished_at is not None
    flow_run.save.assert_called_once()


def test_finish_flow_run_without_flow_run_raises(fake_lamindb: MagicMock) -> None:
    with pytest.raises(LookupError, match="No LaminDB flow run found for 'my_dag/run_1'"):
        runtime.finish_flow_run("my_dag/run_1", True)


def test_run_step_tracks_step_under_flow_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(
        **{"transform.key": "my_dag.py"}, space_id=runtime.ALL_SPACE_ID
    )

    def extract(count, scale=2):
        assert fake_lamindb.context.run is not None
        return count * scale

    result = runtime.run_step(
        extract, (3,), {}, "my_dag/run_1", "# dag source", step_reference="http://ti", instance="owner/name"
    )

    assert result == 6
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_called_once_with(
        key="my_dag.py",
        source_code="# dag source",
        kind="script",
        entrypoint="extract",
        params={"count": 3, "scale": 2},
        initiated_by_run=flow_run,
        new_run=True,
        stream_tracking=False,
    )
    assert fake_lamindb.context.run is None


def test_run_step_records_reference_and_status(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock()
    runs = []

    def track(*args, **kwargs) -> None:
        runs.append(MagicMock())
        fake_lamindb.context._run = runs[-1]

    fake_lamindb.track.side_effect = track

    runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", step_reference="http://ti")
    runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", step_reference="x" * 256)

    assert runs[0].reference == "http://ti"
    assert runs[0].reference_type == runtime.STEP_RUN_REFERENCE_TYPE
    assert runs[0]._status_code == 0
    assert not isinstance(runs[1].reference, str)  # too long for Run.reference: left unset


def _recorded_runs(fake_lamindb: MagicMock) -> list[MagicMock]:
    """Collect the runs ``ln.track`` creates; the step clears ``ln.context`` afterwards."""
    runs: list[MagicMock] = []
    track = fake_lamindb.track.side_effect

    def recording_track(*args, **kwargs) -> None:
        track(*args, **kwargs)
        runs.append(fake_lamindb.context.run)

    fake_lamindb.track.side_effect = recording_track
    return runs


def test_run_step_marks_run_errored_and_reraises(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock()
    runs = _recorded_runs(fake_lamindb)
    fake_lamindb.context._space = "stale"

    def boom():
        raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        runtime.run_step(boom, (), {}, "my_dag/run_1", "")
    assert runs[0]._status_code == 1
    assert fake_lamindb.context.run is None
    assert fake_lamindb.context._space is None


def test_run_step_without_flow_run_raises(fake_lamindb: MagicMock) -> None:
    with pytest.raises(LookupError, match="No LaminDB flow run"):
        runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "")
    fake_lamindb.track.assert_not_called()


def test_standalone_step_tracks_the_dag_file_without_a_flow_run(fake_lamindb: MagicMock) -> None:
    def extract(count, scale=2):
        assert fake_lamindb.context.run is not None
        return count * scale

    result = runtime.run_step(
        extract, (3,), {}, None, "# dag source", step_reference="http://ti", fileloc="/dags/my_dag.py"
    )

    assert result == 6
    fake_lamindb.Run.filter.assert_not_called()  # no flow run is looked up
    fake_lamindb.track.assert_called_once_with(
        key="my_dag.py",
        source_code="# dag source",
        kind="script",
        entrypoint="extract",
        params={"count": 3, "scale": 2},
        initiated_by_run=None,
        new_run=True,
        stream_tracking=False,
    )
    assert fake_lamindb.context.run is None


def test_standalone_step_needs_the_dag_file_name(fake_lamindb: MagicMock) -> None:
    with pytest.raises(ValueError, match="file name of its DAG"):
        runtime.run_step(lambda: None, (), {}, None, "# dag source")
    fake_lamindb.track.assert_not_called()


def test_standalone_step_has_no_space_or_branch_to_inherit(fake_lamindb: MagicMock) -> None:
    runtime.run_step(lambda: None, (), {}, None, "# dag", fileloc="/dags/my_dag.py")
    assert "space" not in fake_lamindb.track.call_args.kwargs
    assert fake_lamindb.context._branch is None


def test_standalone_step_takes_its_own_space_and_branch(fake_lamindb: MagicMock) -> None:
    lab = fake_lamindb.Space.filter.return_value.one_or_none.return_value
    feature = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    feature.id, feature.name = 2, "feature-x"
    runs = _recorded_runs(fake_lamindb)
    seen = {}

    def fn():
        seen["branch"] = fake_lamindb.context._branch

    runtime.run_step(fn, (), {}, None, "# dag", fileloc="/dags/my_dag.py", space="lab", branch="feature-x")

    assert fake_lamindb.track.call_args.kwargs["space"] is lab
    assert runs[0].branch is feature
    assert seen["branch"] is feature
    assert fake_lamindb.context._branch is None


def test_standalone_step_marks_run_errored_and_cleans_up(fake_lamindb: MagicMock) -> None:
    runs = _recorded_runs(fake_lamindb)
    fake_lamindb.context._transform = "stale"
    fake_lamindb.context._uid = "stale"

    def boom():
        raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        runtime.run_step(boom, (), {}, None, "# dag", fileloc="/dags/my_dag.py")
    assert runs[0]._status_code == 1
    assert fake_lamindb.context.run is None
    assert fake_lamindb.context._transform is None
    assert fake_lamindb.context._uid is None  # the next attempt must not be pinned to this transform


def test_every_attempt_of_a_standalone_step_is_its_own_run(fake_lamindb: MagicMock) -> None:
    for _ in range(2):
        runtime.run_step(lambda: None, (), {}, None, "# dag", fileloc="/dags/my_dag.py")
    assert [c.kwargs["new_run"] for c in fake_lamindb.track.call_args_list] == [True, True]


def test_standalone_step_uses_the_key_of_the_transform_with_the_same_source(fake_lamindb: MagicMock) -> None:
    """Another key for a known source makes lamindb ask whether the file was renamed."""
    fake_lamindb.Transform.filter.return_value.first.return_value = MagicMock(key="team/my_dag.py")

    runtime.run_step(lambda: None, (), {}, None, "# dag", fileloc="/elsewhere/my_dag.py")

    assert fake_lamindb.track.call_args.kwargs["key"] == "team/my_dag.py"
    assert fake_lamindb.track.call_args.args == ()
    fake_lamindb.Transform.filter.assert_called_once_with(
        hash=fake_lamindb.setup.core.hashing.hash_string("# dag")
    )


def test_standalone_step_of_an_edited_dag_is_a_new_version(fake_lamindb: MagicMock) -> None:
    previous = MagicMock(uid="AbCdEfGhIjKl0000")
    order_by = fake_lamindb.Transform.filter.return_value.order_by.return_value
    order_by.first.return_value = previous

    runtime.run_step(lambda: None, (), {}, None, "# edited", fileloc="/dags/my_dag.py")

    assert fake_lamindb.track.call_args.args == ("AbCdEfGhIjKl",)  # the stem makes lamindb bump the version
    assert fake_lamindb.track.call_args.kwargs["key"] == "my_dag.py"
    fake_lamindb.Transform.filter.assert_called_with(key="my_dag.py", is_latest=True)


def test_standalone_step_key_is_relative_to_the_dev_dir(fake_lamindb: MagicMock, tmp_path) -> None:
    dags = tmp_path / "project" / "dags"
    dags.mkdir(parents=True)
    fake_lamindb.setup.core._settings_store.dev_dir_containing.return_value = (tmp_path / "project").resolve()

    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "dags/my_dag.py"
    fake_lamindb.setup.core._settings_store.dev_dir_containing.return_value = None
    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "my_dag.py"
    # a file outside the dev dir falls back to its name
    fake_lamindb.setup.core._settings_store.dev_dir_containing.return_value = (tmp_path / "other").resolve()
    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "my_dag.py"


def test_standalone_step_key_uses_the_one_dev_dir_of_older_lamindb(fake_lamindb: MagicMock, tmp_path) -> None:
    # lamindb 2.8 keys ln.track(path=...) relative to ln.setup.settings.dev_dir, without dev_dir_containing
    dags = tmp_path / "project" / "dags"
    dags.mkdir(parents=True)
    del fake_lamindb.setup.core._settings_store.dev_dir_containing
    fake_lamindb.setup.settings.dev_dir = (tmp_path / "project").resolve()

    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "dags/my_dag.py"
    fake_lamindb.setup.settings.dev_dir = (tmp_path / "other").resolve()
    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "my_dag.py"
    fake_lamindb.setup.settings.dev_dir = None
    assert runtime.transform_key(fake_lamindb, dags / "my_dag.py") == "my_dag.py"


def test_run_step_leaves_out_excluded_params_but_passes_them(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock(**{"transform.key": "my_dag.py"})
    received = {}

    def load(user, password, *, retries=3, **options):
        received.update(user=user, password=password, retries=retries, **options)
        return "ok"

    result = runtime.run_step(
        load,
        ("alice", "s3cret"),
        {"token": "t0ken", "region": "eu"},
        "my_dag/run_1",
        "",
        exclude_params=["password", "token"],
    )

    assert result == "ok"
    assert received == {"user": "alice", "password": "s3cret", "retries": 3, "token": "t0ken", "region": "eu"}
    params = fake_lamindb.track.call_args.kwargs["params"]
    assert params == {"user": "alice", "retries": 3, "options": {"region": "eu"}}


@pytest.fixture
def lamindb_inference(monkeypatch: pytest.MonkeyPatch, fake_lamindb: MagicMock):
    """``lamindb.models`` with a stand-in for lamindb's parameter type inference."""
    import sys
    import types

    class SQLRecord:
        pass

    def infer_convert_dtype_key_value(key, value, mute=False):
        assert mute
        if isinstance(value, (int, str)):
            return type(value).__name__, value, ""
        if isinstance(value, list) and all(isinstance(item, SQLRecord) for item in value):
            return "list[cat[ULabel]]", value, ""
        return "?", value, ""

    models = types.ModuleType("lamindb.models")
    models.SQLRecord = SQLRecord  # type: ignore[attr-defined]
    feature_manager = types.ModuleType("lamindb.models._feature_manager")
    feature_manager.infer_convert_dtype_key_value = infer_convert_dtype_key_value  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "lamindb.models", models)
    monkeypatch.setitem(sys.modules, "lamindb.models._feature_manager", feature_manager)
    return SQLRecord


def test_step_params_drops_values_lamindb_cannot_record_without_logging_them(
    lamindb_inference, caplog: pytest.LogCaptureFixture
) -> None:
    class Client:
        def __repr__(self) -> str:
            return "Client(password='s3cret')"

    record = lamindb_inference()

    def step(count, name, client, label, labels, missing=None):
        pass

    params = runtime.step_params(step, (1, "x", Client(), record, [record]), {})

    assert params == {"count": 1, "name": "x", "label": record, "labels": [record], "missing": None}
    assert "Not recording parameter 'client' of type Client" in caplog.text
    assert "s3cret" not in caplog.text


def test_step_params_keeps_everything_when_lamindb_cannot_be_asked(fake_lamindb: MagicMock) -> None:
    def step(client):
        pass

    client = object()
    assert runtime.step_params(step, (client,), {}) == {"client": client}


def test_run_connected_connects_without_recording(fake_lamindb: MagicMock) -> None:
    assert runtime.run_connected(lambda a, b=2: a + b, (1,), {"b": 3}, instance="owner/name") == 4
    fake_lamindb.connect.assert_called_once_with("owner/name")
    fake_lamindb.track.assert_not_called()
    fake_lamindb.Run.filter.assert_not_called()


def test_start_flow_run_in_space(fake_lamindb: MagicMock) -> None:
    lab = fake_lamindb.Space.filter.return_value.one_or_none.return_value

    runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {}, space="lab")

    assert fake_lamindb.track.call_args.kwargs["space"] is lab
    assert fake_lamindb.context._space is None


def test_unknown_space_fails_before_anything_is_recorded(fake_lamindb: MagicMock) -> None:
    fake_lamindb.Space.filter.return_value.one_or_none.return_value = None
    with pytest.raises(LookupError, match="LaminDB space 'lab' does not exist, or you have no access to it"):
        runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {}, space="lab")
    fake_lamindb.flow_run = MagicMock()
    with pytest.raises(LookupError, match="'lab'"):
        runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", space="lab")
    with pytest.raises(LookupError, match="'lab'"):
        runtime.run_connected(lambda: None, (), {}, space="lab")
    fake_lamindb.track.assert_not_called()


def test_retried_flow_init_keeps_the_space_of_its_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock(space_id=2)
    fake_lamindb.Space.filter.return_value.one_or_none.return_value = MagicMock(id=2)
    assert runtime.start_flow_run("r", "/p.py", "e", {}, space="lab") is fake_lamindb.flow_run

    fake_lamindb.Space.filter.return_value.one_or_none.return_value = MagicMock(id=3)
    with pytest.raises(RuntimeError, match="in another space than 'other'"):
        runtime.start_flow_run("r", "/p.py", "e", {}, space="other")


@pytest.mark.parametrize(
    ("flow_space_id", "step_space", "expected"),
    [
        (runtime.ALL_SPACE_ID, None, None),  # unchanged behaviour: lamindb's default
        (2, None, "flow"),  # inherited from the flow run
        (2, "lab", "lab"),
        (runtime.ALL_SPACE_ID, "lab", "lab"),
    ],
)
def test_step_space(fake_lamindb: MagicMock, flow_space_id, step_space, expected) -> None:
    flow_space = MagicMock(name="flow space")
    fake_lamindb.flow_run = MagicMock(space_id=flow_space_id, space=flow_space)
    lab = fake_lamindb.Space.filter.return_value.one_or_none.return_value

    runtime.run_step(lambda: None, (), {}, "my_dag/run_1", "", space=step_space)

    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs.get("space") is {None: None, "flow": flow_space, "lab": lab}[expected]
    assert fake_lamindb.context._space is None


def test_run_connected_sets_the_default_space_while_the_function_runs(fake_lamindb: MagicMock) -> None:
    lab = fake_lamindb.Space.filter.return_value.one_or_none.return_value
    seen = []

    def save_something():
        seen.append(fake_lamindb.context._space)
        raise ValueError("x")

    with pytest.raises(ValueError, match="x"):
        runtime.run_connected(save_something, (), {}, space="lab")
    assert seen == [lab]
    assert fake_lamindb.context._space is None


def test_start_flow_run_on_a_branch(fake_lamindb: MagicMock) -> None:
    feature = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    feature.id = 2

    run = runtime.start_flow_run("my_dag/run_1", "/dags/my_dag.py", "my_dag", {}, branch="feature-x")

    assert "branch" not in fake_lamindb.track.call_args.kwargs  # the transform stays findable from main
    assert run.branch is feature
    assert fake_lamindb.context._branch is None


@pytest.mark.parametrize(("branch_id", "name"), [(0, "archive"), (-1, "trash")])
def test_runs_are_not_recorded_on_the_archive_or_the_trash(fake_lamindb: MagicMock, branch_id, name) -> None:
    record = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    record.id, record.name = branch_id, name
    with pytest.raises(ValueError, match=f"can't be recorded on the LaminDB '{name}' branch"):
        runtime.start_flow_run("r", "/p.py", "e", {}, branch=name)
    fake_lamindb.track.assert_not_called()


def test_unknown_branch_fails_before_anything_is_recorded(fake_lamindb: MagicMock) -> None:
    fake_lamindb.Branch.filter.return_value.one_or_none.return_value = None
    with pytest.raises(LookupError, match="LaminDB branch 'feature-x' does not exist"):
        runtime.start_flow_run("r", "/p.py", "e", {}, branch="feature-x")
    fake_lamindb.track.assert_not_called()


def test_retried_flow_init_keeps_the_branch_of_its_run(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = MagicMock(branch_id=2)
    fake_lamindb.Branch.filter.return_value.one_or_none.return_value = MagicMock(id=3)
    with pytest.raises(RuntimeError, match="on another branch than 'other'"):
        runtime.start_flow_run("r", "/p.py", "e", {}, branch="other")


@pytest.mark.parametrize(
    ("flow_branch_id", "step_branch", "expected"),
    [
        (runtime.MAIN_BRANCH_ID, None, None),  # unchanged behaviour
        (2, None, "flow"),  # inherited from the flow run
        (2, "feature-y", "feature-y"),
        (runtime.MAIN_BRANCH_ID, "feature-y", "feature-y"),
    ],
)
def test_step_branch(fake_lamindb: MagicMock, flow_branch_id, step_branch, expected) -> None:
    flow_branch = MagicMock(name="flow branch", id=flow_branch_id)
    fake_lamindb.flow_run = MagicMock(
        branch_id=flow_branch_id, branch=flow_branch, space_id=runtime.ALL_SPACE_ID
    )
    feature_y = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    feature_y.id = 5
    runs = _recorded_runs(fake_lamindb)
    seen = []

    runtime.run_step(
        lambda: seen.append(fake_lamindb.context._branch), (), {}, "my_dag/run_1", "", branch=step_branch
    )

    branch = {None: None, "flow": flow_branch, "feature-y": feature_y}[expected]
    assert "branch" not in fake_lamindb.track.call_args.kwargs
    assert seen == [branch]  # records fn saves go on the branch
    if branch is not None:
        assert runs[0].branch is branch
    assert fake_lamindb.context._branch is None


def test_run_connected_sets_the_default_branch_while_the_function_runs(fake_lamindb: MagicMock) -> None:
    feature = fake_lamindb.Branch.filter.return_value.one_or_none.return_value
    feature.id = 2
    seen = []
    runtime.run_connected(lambda: seen.append(fake_lamindb.context._branch), (), {}, branch="feature-x")
    assert seen == [feature]
    assert fake_lamindb.context._branch is None
