# LaminDB Lineage

The lineage operators record Airflow DAG runs in LaminDB, so that data created by a DAG can be traced
back to the DAG run and the task that created it:

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

lamindb runs in a virtualenv (`@task.lamindb_venv` and the flow operators) or a Kubernetes pod
(`@task.lamindb_k8s`), never on the worker, so its version doesn't have to fit Airflow's
dependencies. The tasks connect with the [lamindb connection](connections/lamindb.md) (see
[LaminDB settings](#lamindb-settings)).

## @task.lamindb_venv

`@task.lamindb_venv` works like `@task.virtualenv` and accepts all of its arguments. The function
runs in a virtualenv, recorded as a step of the DAG run's flow run: lamindb tracks its parameters and
outcome, and artifacts the function saves are linked to the step run. Plain Airflow tasks can sit in
between and exchange XComs with the steps.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_decorator_lamindb_venv"
```

lamindb is added to `requirements` when the task runs (see `lamindb_version` under
[LaminDB settings](#lamindb-settings)), unless you list it yourself.

## @task.lamindb_k8s

`@task.lamindb_k8s` works like `@task.kubernetes` and accepts all of its arguments; it needs
`pip install "apache-airflow-providers-lamindb[cncf.kubernetes]"`. The image must have lamindb 2.8.1 or newer
installed, and the pod needs the API key, for example from a Kubernetes secret. The connection only provides the instance,
because environment variables of a pod are readable in its spec.

```python
from airflow.providers.cncf.kubernetes.secret import Secret


@task.lamindb_k8s(
    image="my-registry/pipeline:1.0",  # with lamindb installed
    secrets=[Secret("env", "LAMIN_API_KEY", "lamin", "api-key")],
)
def train(dataset_key: str) -> str: ...
```

The flow operators of a DAG with only pod steps run in a virtualenv on the worker.

## LaminDBFlowInitOperator and LaminDBFlowFinishOperator

[`LaminDBFlowInitOperator`][airflow.providers.lamindb.operators.flow.LaminDBFlowInitOperator] starts
the flow run of a DAG run: it calls `ln.track()` on the DAG file in a virtualenv and tags the run with
the DAG run. Retrying it restarts the same run instead of creating a second one. It returns the flow
run uid.

[`LaminDBFlowFinishOperator`][airflow.providers.lamindb.operators.flow.LaminDBFlowFinishOperator]
closes the flow run as `completed`, or `errored` if any other task of the DAG run failed.

Init is an Airflow *setup* task and finish a *teardown* task. Finish therefore runs after the steps even
when they fail, and Airflow ignores it when deciding the DAG run state, so a failed step still fails the
DAG run. Pass `is_setup=False` or `is_teardown=False` (with your own `trigger_rule`) to opt out.

Both accept every `PythonVirtualenvOperator` argument except `python_callable`, `op_args` and
`op_kwargs`. Wire them with `with init >> finish:`, Airflow's setup/teardown context: when the block
ends, Airflow wires init upstream of the tasks in it that have no upstream, and finish downstream of the
ones that have no downstream. Every task in the block, LaminDB step or not, runs inside the flow run.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_operator_lamindb_flow"
```

Airflow does not track mapped tasks (`.expand()`) in the context; add them with
`with init >> finish as scope: ... scope.add_task(mapped)` if they are a root or leaf.

## Auto-wiring

Alternatively, pass `auto_flow=True` to the steps (default `False`). Each one then adds the DAG's
`lamindb_flow_init` and `lamindb_flow_finish` tasks on first use and wires
`init >> step >> finish`, which adds an edge per step. The flow tasks copy the settings of the first
LaminDB step: its connection and instance, and for `@task.lamindb_venv` also its Python version,
index, lamindb pin and environment settings. Flow operators declared before the steps are reused
instead.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_auto_flow"
```

Caveats:

- Mapped steps (`.expand()`) are not auto-wired.
- Flow operators declared *after* an auto-wiring step clash with the auto-added task id; declare them
  first.
- Finish waits for auto-wired steps only.

Every step looks the flow run up by its DAG run, so no XCom plumbing is needed and any DAG topology
works. A step whose flow init has not run fails with a clear error.

## LaminDB settings

The lineage operators and decorators accept these arguments, also through a DAG's `default_args`:

`lamindb_conn_id` (default `lamindb_default`)
:   The [connection](connections/lamindb.md) with the Lamin API key and the instance. Virtualenvs get
    the API key as `LAMIN_API_KEY` and a temporary, empty `LAMIN_SETTINGS_DIR`, so lamindb never reads
    the worker's `~/.lamin`. Both are set only while the task runs and are never rendered; explicit
    `env_vars` of a task take precedence. Pods only get the instance. A task fails if its connection
    does not exist; there is no fallback to lamindb's own configuration.

    `None` uses lamindb's own configuration (`LAMIN_API_KEY`, `~/.lamin`) instead, for example for
    instances that aren't hosted on LaminHub, or for `@task.lamindb_k8s` steps whose pods bring their
    own credentials. Then also pass `lamindb_instance` or set `LAMIN_CURRENT_INSTANCE`: the virtualenv
    runs outside your project directory. Auto-wired flow operators get the step's `lamindb_conn_id`,
    so with `None` they use the worker's lamindb configuration in their virtualenv.

`lamindb_instance`
:   The instance slug `owner/name`; overrides the connection's.

`lamindb_version` (virtualenvs only)
:   The lamindb version to install, 2.8.1 or newer. Defaults to the instance's version on LaminHub,
    so that the virtualenv speaks the instance's schema, else the latest. The virtualenvs install
    `lamindb-core` and the few packages it needs here (`numpy`, `pandas`, `pandera`), about half the
    size of the full `lamindb`; for lamindb before 2.10 with pandas below 3, as lamindb requires. List
    `lamindb` in `requirements` if your steps need all of it, for example `bionty` or `anndata`.

Lineage needs lamindb 2.8.1 or newer, and anything older fails deliberately. An older
`lamindb_version` fails when the DAG is parsed. An instance on an older lamindb fails the task
before its virtualenv is built. A step or flow task whose virtualenv or pod has an older lamindb,
for example from your own `requirements` or the pod's image, fails when it starts.

A step can use another connection than the rest of the DAG, for example to run under another API key:

```python
@task.lamindb_venv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

All LaminDB tasks of a DAG run must use the same instance as its flow run; a step that connects to
another instance fails before it starts.

## Recorded parameters

A step records the arguments of its function, including defaults, as the step run's `params`. These
are left out:

- Arguments whose template reads an Airflow Variable or Connection, such as
  `"{{ var.value.lims_token }}"` or `{"auth": "Bearer {{ conn.lims.password }}"}`. Other templates,
  such as `"{{ ds }}"` or `"{{ params.sample }}"`, are recorded. `lamindb_exclude_templated="all"`
  leaves out every templated argument, and `"none"` records them.
- Arguments that contain a secret Airflow masks in its logs, such as a Connection's password or a
  Variable with a sensitive name, wherever the value came from.
- The parameters listed in `lamindb_exclude_params`, which may also name keys of `**kwargs`. Unknown
  names fail when the DAG is parsed.
- lamindb itself records parameters with sensitive names, such as `password`, `api_key` or
  `access_token`, as `***REDACTED***`. It matches whole words, so `tokenizer` is recorded as is.
- Values lamindb cannot store, such as client objects. The task log names them and their type, but
  not their value.

The function still gets every argument unchanged. The check applies to the whole argument, so a
secret anywhere inside a dict leaves out the dict. It looks at templates only: a secret that reaches
the step another way, for example as the XCom of an upstream task that read it, is recorded unless you
list its parameter in `lamindb_exclude_params`.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_recorded_params"
```

Leaving an argument out of the lineage doesn't hide it elsewhere: Airflow shows rendered arguments in
the task's *Rendered Template* view, masking only those it knows to be secret, and the function may
log or return it.

## Untracked tasks

A LaminDB task runs in one of three modes:

| | Step run | Flow run needed | Connected to the instance |
|---|---|---|---|
| default (`track=True`) | yes | yes | yes |
| `track=False, connect=True` | no | no | yes |
| `track=False` | no | no | no: the plain `@task.virtualenv` or `@task.kubernetes` |

With `connect=True`, a task that only reads or writes records gets the same setup as a step, without
lineage: a virtualenv gets lamindb (see `lamindb_version`) and the connection's credentials, and both
connect to the instance before the function runs. A pod connects with its own credentials, as for
steps. `auto_flow` is ignored, and the task may connect to another instance than the DAG's flow run,
for example one per mapped task through a templated `lamindb_instance`.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_connect_only"
```

Plain `track=False` runs the function unchanged, and lamindb is not added to `requirements`.

## How the steps are recorded

The decorators reuse Airflow's mechanism of shipping the function's source text and append a small
wrapper that connects to the instance, binds the step to the flow run and records the outcome. In the
virtualenv or pod, the step calls `ln.track()` with the DAG file's source and `initiated_by_run=`
the flow run, so it resolves the same transform as the flow run. Nothing is pickled by reference, and
the remote environment only needs lamindb. Step runs reference the Airflow task instance log URL
(`reference_type="airflow_task_instance"`).

## Limitations

- lamindb prompts on stdin when it finds a transform with the same source hash under a different key
  (for example after renaming a DAG file). Inside an Airflow task there is no stdin, so that task fails;
  run the DAG file once locally with `ln.track()` to resolve the rename.
- The pod variant embeds the DAG file source in the script passed via an environment variable; keep DAG
  files reasonably small.
