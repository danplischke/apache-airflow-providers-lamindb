"""LaminDB run bookkeeping shared by the worker and remote interpreters.

This module is shipped *as source* to virtualenvs and pods, which may have neither
Airflow nor this package installed. Keep it self-contained: standard library and
``lamindb`` only, imported inside functions, so the worker can import it without
lamindb.

Mapping:

- one ``LaminDBFlow`` scope in a DAG run -> one LaminDB flow run (a ``Run`` of the DAG file's ``Transform``)
- one Airflow task run -> one LaminDB step run, a child of the scope's flow run
  (``initiated_by_run``) or, outside a scope, standalone

The flow run is identified by ``reference="<dag_id>/<run_id>"`` (plus ``/<scope_id>`` for scopes
with an id) and ``reference_type="airflow_dag_run"``, so any task can look it up without XCom.
"""

from datetime import datetime, timezone

FLOW_RUN_REFERENCE_TYPE = "airflow_dag_run"
STEP_RUN_REFERENCE_TYPE = "airflow_task_instance"
MAX_REFERENCE_LENGTH = 255

# the oldest lamindb the provider supports, for the installed package here and for instances and
# version pins on the worker; anything older fails deliberately
MIN_LAMINDB_VERSION = "2.8.1"

ALL_SPACE_ID = 1
"""Id of the ``all`` space every instance has: the default, without access restrictions."""

MAIN_BRANCH_ID = 1
"""Id of the ``main`` branch every instance has; ``archive`` is 0 and ``trash`` -1."""

STATUS_STARTED = -1
STATUS_RESTARTED = -2
STATUS_COMPLETED = 0
STATUS_ERRORED = 1


def _release(version):
    import re

    match = re.match(r"\s*(\d+)\.(\d+)(?:\.(\d+))?", str(version))
    return (int(match[1]), int(match[2]), int(match[3] or 0)) if match else None


def lamindb_version_at_least(version, minimum):
    """Whether lamindb ``version``, such as ``"2.11.0"``, is ``minimum`` or newer.

    Compares major, minor and patch (a missing patch is 0); pre-releases count as their release.
    """
    release = _release(version)
    return release is not None and release >= _release(minimum)


def lamindb_version_supported(version):
    """Whether lamindb ``version`` is ``MIN_LAMINDB_VERSION`` or newer."""
    return lamindb_version_at_least(version, MIN_LAMINDB_VERSION)


def connect(instance=None):
    """Import lamindb, connected to ``instance`` if given.

    Credentials and the default instance follow lamindb's own configuration
    (``LAMIN_API_KEY``, ``LAMIN_CURRENT_INSTANCE``, ``~/.lamin``). Fails if the installed lamindb
    is older than ``MIN_LAMINDB_VERSION``.
    """
    import lamindb as ln

    version = getattr(ln, "__version__", "unknown")
    if not lamindb_version_supported(version):
        raise RuntimeError(
            f"lamindb {version} is installed, but apache-airflow-providers-lamindb needs lamindb "
            f"{MIN_LAMINDB_VERSION} or newer. Install a newer lamindb in the virtualenv or pod image."
        )
    if instance:
        ln.connect(instance)
    return ln


def get_flow_run(reference):
    """Return the flow run tagged with ``reference``, or None if it was never started."""
    import lamindb as ln

    # on any branch but the archive and the trash: lamindb's default scope is only the current branch
    return (
        ln.Run.filter(
            reference=reference, reference_type=FLOW_RUN_REFERENCE_TYPE, branch_id__gte=MAIN_BRANCH_ID
        )
        .order_by("-created_at")
        .first()
    )


def require_flow_run(reference):
    flow_run = get_flow_run(reference)
    if flow_run is None:
        raise LookupError(
            f"No LaminDB flow run found for {reference!r}. The LaminDBFlow scope of this task needs "
            "its flow init task to run first."
        )
    return flow_run


def finish_run(run, success):
    """Close a run the same way lamindb's own decorators do."""
    run.finished_at = datetime.now(timezone.utc)
    run._status_code = STATUS_COMPLETED if success else STATUS_ERRORED
    run.save()


def resolve_space(space):
    """Return the ``Space`` named (or with the uid) ``space``; ``None`` for no space."""
    import lamindb as ln

    if space is None:
        return None
    record = ln.Space.filter(ln.Q(name=space) | ln.Q(uid=space)).one_or_none()
    if record is None:
        raise LookupError(f"LaminDB space {space!r} does not exist, or you have no access to it.")
    return record


def resolve_branch(branch):
    """Return the ``Branch`` named (or with the uid) ``branch``; ``None`` for no branch."""
    import lamindb as ln

    if branch is None:
        return None
    record = ln.Branch.filter(ln.Q(name=branch) | ln.Q(uid=branch)).one_or_none()
    if record is None:
        raise LookupError(f"LaminDB branch {branch!r} does not exist, or you have no access to it.")
    if record.id < MAIN_BRANCH_ID:
        raise ValueError(f"Runs can't be recorded on the LaminDB {record.name!r} branch.")
    return record


def _move_to_branch(run, branch_record):
    """Put ``run`` on ``branch_record``; its transform stays where lamindb's default lookup finds it.

    ``ln.track(branch=...)`` would also create a new transform on the branch, where steps and later
    flow runs on other branches wouldn't find it by its source.
    """
    if branch_record is not None and run.branch_id != branch_record.id:
        run.branch = branch_record
        run.save()


def _reset_context(ln):
    """Leave the process clean for in-process runners like ``dag.test()``."""
    ln.context._run = None
    # ln.track() remembers the transform's uid, which would pin the next call to the same transform
    ln.context._uid = None
    ln.context._transform = None
    ln.context._space = None
    ln.context._branch = None


def start_flow_run(reference, path, entrypoint, params, instance=None, space=None, branch=None):
    """Start (or restart, on retry) the flow run for a DAG run; return it.

    Uses ``ln.track`` on the DAG file so the flow run belongs to a versioned
    ``Transform`` of the DAG source. Idempotent per DAG run: a retry of the init
    task restarts the existing run instead of creating a second one. With ``space``,
    the transform (if new) and the run are created in that space; with ``branch``, the run
    is on that branch.
    """
    from pathlib import Path

    ln = connect(instance)
    space_record = resolve_space(space)
    branch_record = resolve_branch(branch)
    existing = get_flow_run(reference)
    if existing is not None:
        if space_record is not None and existing.space_id != space_record.id:
            raise RuntimeError(
                f"The flow run of {reference!r} is in another space than {space!r}; "
                "it keeps its space when the init task is retried."
            )
        if branch_record is not None and existing.branch_id != branch_record.id:
            raise RuntimeError(
                f"The flow run of {reference!r} is on another branch than {branch!r}; "
                "it keeps its branch when the init task is retried."
            )
        existing.started_at = datetime.now(timezone.utc)
        existing.finished_at = None
        existing._status_code = STATUS_RESTARTED
        existing.save()
        return existing

    if ln.context.run is not None:
        raise RuntimeError(
            "LaminDB global run context is already set in this process; "
            "cannot start a flow run. Call ln.finish() first."
        )
    space_kwargs = {"space": space_record} if space_record is not None else {}
    try:
        ln.track(
            path=path,
            entrypoint=entrypoint,
            params=params,
            new_run=True,
            stream_tracking=False,
            **space_kwargs,
        )
        run = ln.context.run
    finally:
        _reset_context(ln)
    if run is None:
        raise RuntimeError("ln.track() did not create a run (read-only connection?)")
    transform = run.transform
    if transform.hash is None:
        # lamindb persists source code at ln.finish(); do it now so steps resolve
        # this transform by hash and DAG edits bump its version.
        transform._update_source_code_from_path(Path(path))
        transform.save()
    run.reference = reference
    run.reference_type = FLOW_RUN_REFERENCE_TYPE
    if branch_record is not None:
        run.branch = branch_record
    run.save()
    return run


def finish_flow_run(reference, success, instance=None):
    """Close the flow run of a DAG run; return it."""
    connect(instance)
    flow_run = require_flow_run(reference)
    finish_run(flow_run, success)
    return flow_run


def run_connected(fn, args, kwargs, instance=None, space=None, branch=None):
    """Call ``fn`` connected to ``instance``, without recording anything.

    With ``space`` or ``branch``, records ``fn`` saves are created in that space or on that branch
    by default.
    """
    ln = connect(instance)
    ln.context._space = resolve_space(space)
    ln.context._branch = resolve_branch(branch)
    try:
        return fn(*args, **kwargs)
    finally:
        _reset_context(ln)


def _recordable(key, value):
    """Whether lamindb can record ``value`` as a run parameter.

    Mirrors lamindb's own check, which skips other values with a warning that includes the value.
    """
    try:
        from lamindb.models import SQLRecord
        from lamindb.models._feature_manager import infer_convert_dtype_key_value
    except ImportError:
        return True  # leave the decision to lamindb
    if value is None or isinstance(value, SQLRecord) or (isinstance(value, list) and not value):
        return True
    try:
        dtype, _, _ = infer_convert_dtype_key_value(key, value, mute=True)
    except Exception:
        return False
    if dtype in {"cat ? str", "list[cat ? str]"}:
        return True
    if dtype.startswith("list[cat"):
        return all(isinstance(item, SQLRecord) for item in value)
    return dtype != "?" and not dtype.startswith("cat")


def step_params(fn, args, kwargs, exclude_params=()):
    """The parameters recorded for a call of ``fn``: its bound arguments, including defaults.

    Leaves out the names in ``exclude_params``, which may also be keys of ``**kwargs``, and values
    lamindb cannot record. Only names and types are logged, never values.
    """
    import inspect
    import logging

    signature = inspect.signature(fn)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    excluded = set(exclude_params)
    params = {name: value for name, value in bound.arguments.items() if name not in excluded}
    for parameter in signature.parameters.values():
        if parameter.kind is parameter.VAR_KEYWORD and parameter.name in params:
            extra = params[parameter.name]
            params[parameter.name] = {key: value for key, value in extra.items() if key not in excluded}
    for name, value in list(params.items()):
        if not _recordable(name, value):
            del params[name]
            logging.getLogger("lamindb_airflow").warning(
                "Not recording parameter %r of type %s: lamindb cannot store it", name, type(value).__name__
            )
    return params


def transform_key(ln, fileloc):
    """The key of the DAG file's transform, as ``ln.track(path=...)`` infers it for the flow run.

    Relative to the development directory the file is in, else the file name. Spelled out because
    ``ln.track(source_code=...)`` needs a key, and ``path=`` can't be combined with it.
    """
    from pathlib import Path

    path = Path(fileloc).expanduser().resolve(strict=False)
    try:
        # the helper lamindb >= 2.11 infers the key of ln.track(path=...) with
        dev_dir_containing = ln.setup.core._settings_store.dev_dir_containing
    except (AttributeError, ImportError):
        # older lamindb: relative to the one configured development directory
        root = getattr(ln.setup.settings, "dev_dir", None)
    else:
        try:
            root = dev_dir_containing(path, instance_slug=ln.setup.settings.instance.slug)
        except (AttributeError, ValueError):
            root = None
    if root is None:
        return path.name
    try:
        return path.relative_to(root).as_posix()
    except ValueError:  # the file isn't below it
        return path.name


def standalone_transform(ln, fileloc, source_code):
    """The ``(key, uid)`` to track a standalone step of the DAG file with ``source_code`` under.

    Without a flow run to take the transform from, the step finds the DAG file's transform the way
    the flow init does, so that both resolve the same one:

    - a transform with the same source is used with its own key: tracking it under another key makes
      lamindb ask whether the file was renamed, which nothing can answer in a virtualenv or pod;
    - else the latest transform with this key becomes the previous version, so an edited DAG file
      is a new version instead of an unrelated transform (``uid`` is its stem);
    - else a new transform is created (``uid`` is ``None``).
    """
    known = ln.Transform.filter(hash=ln.setup.core.hashing.hash_string(source_code)).first()
    if known is not None:
        return known.key, None
    key = transform_key(ln, fileloc)
    previous = ln.Transform.filter(key=key, is_latest=True).order_by("-created_at").first()
    return key, previous.uid[:-4] if previous is not None else None


def run_step(
    fn,
    args,
    kwargs,
    flow_run_reference,
    source_code,
    step_reference=None,
    instance=None,
    exclude_params=(),
    space=None,
    branch=None,
    fileloc=None,
):
    """Run ``fn`` as a tracked step, a child of the flow run ``flow_run_reference`` if given.

    ``source_code`` is the DAG file's source. With a flow run, it is tracked under the flow run's
    transform key, which makes lamindb resolve the same ``Transform`` by hash. Without one
    (``None``), the step is standalone and finds the transform of the DAG file at ``fileloc`` itself
    (see ``standalone_transform``). ``fn`` gets ``args`` and ``kwargs`` unchanged; see
    ``step_params`` for what is recorded. The step run, and records ``fn`` saves, are in ``space``,
    else in the flow run's, and on ``branch``, else on the flow run's.
    """
    ln = connect(instance)
    flow_run = require_flow_run(flow_run_reference) if flow_run_reference is not None else None
    space_record = resolve_space(space)
    if space_record is None and flow_run is not None and flow_run.space_id not in (None, ALL_SPACE_ID):
        space_record = flow_run.space
    branch_record = resolve_branch(branch)
    if branch_record is None and flow_run is not None and flow_run.branch_id not in (None, MAIN_BRANCH_ID):
        branch_record = flow_run.branch
    track_kwargs = {
        "source_code": source_code,
        "kind": "script",
        "entrypoint": fn.__name__,
        "params": step_params(fn, args, kwargs, exclude_params),
        "initiated_by_run": flow_run,
        "new_run": True,
        "stream_tracking": False,
    }
    if space_record is not None:
        track_kwargs["space"] = space_record
    try:
        _track_step(ln, flow_run, fileloc, track_kwargs)
        run = ln.context.run
        if step_reference and len(step_reference) <= MAX_REFERENCE_LENGTH:
            run.reference = step_reference
            run.reference_type = STEP_RUN_REFERENCE_TYPE
        _move_to_branch(run, branch_record)
        # records fn saves default to the context's branch
        ln.context._branch = branch_record
        try:
            result = fn(*args, **kwargs)
        except BaseException:
            finish_run(run, False)
            raise
        finish_run(run, True)
        return result
    finally:
        _reset_context(ln)


def _track_step(ln, flow_run, fileloc, track_kwargs):
    """Call ``ln.track`` for a step: under the flow run's transform, or the DAG file's own."""
    if flow_run is not None:
        ln.track(key=flow_run.transform.key, **track_kwargs)
        return
    if fileloc is None:
        raise ValueError("A standalone step needs the file name of its DAG to find its transform.")
    key, uid = standalone_transform(ln, fileloc, track_kwargs["source_code"])
    ln.track(*([uid] if uid else []), key=key, **track_kwargs)
