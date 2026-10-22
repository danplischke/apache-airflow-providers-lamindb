# LaminDB Lineage

The lineage decorators record Airflow tasks in LaminDB, so that data created by a DAG can be traced
back to the task run that created it, and optionally to the DAG run around it:

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| task run, standalone (the default) | `Run` of that transform, `entrypoint=` the function name |
| `LaminDBFlow` scope in a DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run in a scope | `Run` with `initiated_by_run=` the flow run, `entrypoint=` the function name |

A tracked task is **standalone** unless you put it in a [`LaminDBFlow`](#lamindbflow-optional-flow-scope)
block. Standalone tasks need no flow run; they record the source, parameters, environment and
outcome of each task run, with a reference to the Airflow task instance. A `LaminDBFlow` block adds a
flow run around its tasks, for pipeline-level lineage.

lamindb runs in a virtualenv (`@task.lamindb_virtualenv` and the flow operators) or a Kubernetes pod
(`@task.lamindb_kubernetes`), never on the worker, so its version doesn't have to fit Airflow's
dependencies. The tasks connect with the [lamindb connection](connections/lamindb.md) (see
[LaminDB settings](#lamindb-settings)).

## @task.lamindb_virtualenv

`@task.lamindb_virtualenv` works like `@task.virtualenv` and accepts all of its arguments. The function
runs in a virtualenv and is tracked by default: lamindb records its parameters and outcome, and
artifacts the function saves are linked to the task run. Plain Airflow tasks can sit in between and
exchange XComs with the tracked ones. Each attempt of a task, including retries and every map index,
is its own run.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_decorator_lamindb_virtualenv"
```

lamindb is added to `requirements` when the task runs (see `lamindb_version` under
[LaminDB settings](#lamindb-settings)), unless you list it yourself.

## @task.lamindb_kubernetes

`@task.lamindb_kubernetes` works like `@task.kubernetes` and accepts all of its arguments; it needs
`pip install "apache-airflow-providers-lamindb[k8s]"`. The image must have lamindb 2.8.1 or newer
installed, and the pod needs the API key, for example from a Kubernetes secret. The connection only provides the instance,
because environment variables of a pod are readable in its spec.

```python
from airflow.providers.cncf.kubernetes.secret import Secret


@task.lamindb_kubernetes(
    image="my-registry/pipeline:1.0",  # with lamindb installed
    secrets=[Secret("env", "LAMIN_API_KEY", "lamin", "api-key")],
)
def train(dataset_key: str) -> str: ...
```

Pod steps are tracked standalone by default too. The lifecycle operators of a `LaminDBFlow` always
run in a virtualenv on the worker, also when all the tasks in the scope are pod steps.

## LaminDBFlow: optional flow scope

[`LaminDBFlow`][airflow.providers.lamindb.flow.LaminDBFlow] is a context manager that groups the
tracked tasks created inside it under one flow run per DAG run. Import it from the package:

```python
from airflow.providers.lamindb import LaminDBFlow
```

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_flow_scope"
```

On exit, the block adds a `LaminDBFlowInitOperator` and a `LaminDBFlowFinishOperator` to the DAG, wires
init upstream of the scope's tasks that have no upstream within the scope, and finish downstream of
those that have no downstream within it. Init is paired with finish as Airflow's setup and teardown
(see [the operators](#lamindbflowinitoperator-and-lamindbflowfinishoperator)). An empty block adds
nothing and warns with `LaminDBFlowWarning`.

**Membership.** Every task *invocation* created inside the block belongs to the scope: normal calls,
`.override()`, `.partial()`, `.expand()` and `.expand_kwargs()`, also inside task groups. There is no
`add_task`. A mapped task is one invocation, so its map indexes join automatically and no extra
lifecycle tasks are created per index. Two details:

- A task created *outside* the block and only wired inside it (for example `curate(upstream_result)`
  where `upstream_result` comes from a standalone task) is wired into the DAG as usual, but it is not a
  member: it isn't tracked under the scope's flow run.
- Plain Airflow tasks and `track=False` tasks created inside the block are members too: they run
  between init and finish, inside the flow run's lifecycle. Only tracked LaminDB tasks get the flow
  run as parent.

**Configuration.** `LaminDBFlow(**kwargs)` configures both lifecycle operators explicitly: the
connection and instance, the lamindb setup, and for the virtualenvs the Python and lamindb version,
index and environment settings (any argument of the
[flow operators](#lamindbflowinitoperator-and-lamindbflowfinishoperator)). Nothing is borrowed from the
tasks in the block, and the tasks' own connection, instance and environment settings stay their own;
the only thing a scoped task takes from its parent is the default branch and space (see
[LaminDB settings](#lamindb-settings)). Scoped tracking needs its flow run: a task whose parent flow
run doesn't exist fails with an error instead of running standalone.

**Rules.**

- Scopes can't be nested; entering a `LaminDBFlow` inside another one raises an error.
- One DAG has one default scope, with the task ids `lamindb_flow_init` and `lamindb_flow_finish` and the
  flow run reference `<dag_id>/<run_id>`. More scopes in the same DAG need a stable, explicit
  `scope_id`: `LaminDBFlow("enrich", ...)`. Such a scope gets task ids of its own and appends its id to
  the flow run reference, so concurrent scopes and DAG runs never share a flow run. Invalid ids, and
  ids that make the reference too long, raise a clear error; the identity is never truncated.
- If the block raises, the scope is discarded and the exception propagates: nothing is wired and no
  lifecycle operators are added. A new `LaminDBFlow` can be used afterwards.
- Finish marks the flow run `errored` when a task *of its scope* failed, including a failed map index or
  a failed plain task. It ignores failures of tasks outside the scope, and sibling scopes don't affect
  each other.

```python
with DAG("pipeline"):
    raw = extract()  # standalone
    with LaminDBFlow():  # default scope
        curate(raw)  # in the scope's flow run; raw itself is not
    with LaminDBFlow("enrich", lamindb_version="2.11"):  # sibling scope: explicit id
        enrich(extract.override(task_id="extract_again")())
```

## LaminDBFlowInitOperator and LaminDBFlowFinishOperator

These are the operators that `LaminDBFlow` adds, and they stay exported for advanced use.
[`LaminDBFlowInitOperator`][airflow.providers.lamindb.operators.flow.LaminDBFlowInitOperator] starts
a flow run: it calls `ln.track()` on the DAG file in a virtualenv and tags the run with the DAG run.
Retrying it restarts the same run instead of creating a second one. It returns the flow run uid.

[`LaminDBFlowFinishOperator`][airflow.providers.lamindb.operators.flow.LaminDBFlowFinishOperator]
closes the flow run as `completed`, or `errored` if a task failed. In a `LaminDBFlow` that means a task
of the scope; see the rules above.

Init is an Airflow *setup* task and finish a *teardown* task. Finish therefore runs after the scope's
tasks even when they fail, and Airflow ignores it when deciding the DAG run state, so a failed task
still fails the DAG run. Pass `is_setup=False` or `is_teardown=False` (with your own `trigger_rule`) to
opt out.

Both accept every `PythonVirtualenvOperator` argument except `python_callable`, `op_args` and
`op_kwargs`.

Used directly, with `with init >> finish:`, they are only Airflow's setup/teardown context: they do
**not** make tracked tasks their children. A tracked task is standalone unless it was created inside a
`LaminDBFlow`, however init and finish are wired around it. A finish operator you declare yourself keeps
looking at the DAG run's other tasks; the scope's finish only looks at its own members.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_operator_lamindb_flow"
```

## Mapped tasks

Mapped tasks (`.expand()`, `.expand_kwargs()`) are tracked standalone like any other, and join a
`LaminDBFlow` when they are created inside it; `.partial()` and `.override()` work as usual.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_mapped_steps"
```

- Each map index is its own run. In a scope, finish waits for all of them, and marks the flow run
  errored if any failed; a retried map index records a new run.
- A map over nothing runs no tasks; the flow run completes.

A scoped task looks the flow run up by its DAG run (and scope), so no XCom plumbing is needed. A
scoped task whose flow init has not run fails with a clear error.

## Migrating to LaminDBFlow

Tracked tasks used to need a flow run, found through the DAG's flow operators. They no longer attach
to flow operators implicitly, and `auto_flow` is removed.

| Before | Now |
|---|---|
| `@task.lamindb_virtualenv(auto_flow=True)` | Drop it: the task is tracked standalone. For a flow run around tasks, create them inside `with LaminDBFlow(...):`. |
| `with LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():` around the steps | `with LaminDBFlow(...):` around the steps. |
| `scope.add_task(mapped)` | Remove it: mapped invocations created inside the block join. |
| Flow settings (`lamindb_conn_id`, `lamindb_version`, ...) copied from the first step | Pass them to `LaminDBFlow(...)`. |
| `default_args={"auto_flow": True}` | Remove it, and create the tasks inside `with LaminDBFlow(...):`. |

What fails, and what only warns:

- `auto_flow` fails with a `TypeError` that points at `LaminDBFlow`, also when it comes from the
  `default_args` of a DAG or task group. A mapped task with `auto_flow` in its `default_args` fails when
  Airflow unmaps it at run time, since that's when its arguments are known.
- `lamindb_scope` in `default_args` fails with a `TypeError`: it would make every tracked task a member
  of a flow run.
- A `LaminDBFlowInitOperator` or `LaminDBFlowFinishOperator` created outside a `LaminDBFlow` warns with
  `LaminDBFlowWarning` when the DAG is parsed. Such DAGs keep running, but their steps are recorded as
  standalone runs, without a parent flow run, until you switch to `LaminDBFlow`.

**Behaviour changes when you switch.** Every task created in the block becomes a member, not only the
tracked ones: plain Airflow tasks and `track=False` tasks in it now wait for the flow init, and a failure
of one of them marks the flow run `errored`. Create tasks that should stay independent of the flow run
outside the block. Tracked tasks in the block need their flow run: one whose flow init didn't run fails
instead of running standalone.

## LaminDB settings

The lineage operators and decorators accept these arguments, also through a DAG's `default_args`:

`lamindb_conn_id` (default `lamindb_default`)
:   The [connection](connections/lamindb.md) with the Lamin API key and the instance. Virtualenvs get
    the API key as `LAMIN_API_KEY` and a temporary, empty `LAMIN_SETTINGS_DIR`, so lamindb never reads
    the worker's `~/.lamin`. Both are set only while the task runs and are never rendered; explicit
    `env_vars` of a task take precedence. Pods only get the instance. A task fails if its connection
    does not exist; there is no fallback to lamindb's own configuration.

    `None` uses lamindb's own configuration (`LAMIN_API_KEY`, `~/.lamin`) instead, for example for
    instances that aren't hosted on LaminHub, or for `@task.lamindb_kubernetes` steps whose pods bring their
    own credentials. Then also pass `lamindb_instance` or set `LAMIN_CURRENT_INSTANCE`: the virtualenv
    runs outside your project directory. A `LaminDBFlow` takes its own `lamindb_conn_id`, which is not
    borrowed from the tasks in it; with `None` its lifecycle operators use the worker's lamindb
    configuration in their virtualenv.

`lamindb_instance`
:   The instance slug `owner/name`; overrides the connection's.

`lamindb_space`
:   The name or uid of a [space](https://docs.lamin.ai/permissions) to record in, instead of
    lamindb's default. Templated; an empty rendered value means the default. On the flow init
    operator, it's the space of the flow run, and of the DAG file's transform when that is first
    recorded; a transform keeps its space when later flow runs are in another one. Tasks in a
    `LaminDBFlow` default to their flow run's space, and so do the records their function saves, unless
    the task sets its own `lamindb_space`. Standalone tasks use lamindb's default space unless they set
    one. With `connect=True`, it's the space of the records the function saves. An unknown
    space, or one you have no access to, fails the task before anything is recorded; there is no
    fallback to the default space. A retried flow init keeps its run's space and fails if it's given
    another one.

    LaminHub enforces access to spaces; the provider has no permission checks of its own. A step in
    another space than its flow run needs access to both, and artifacts saved in a space need a
    storage location in it (see lamindb's `NoStorageLocationForSpace`).

`lamindb_branch`
:   The name or uid of a [branch](https://docs.lamin.ai/lamindb.branch) to record on, instead of the
    instance's current one, for example a draft branch that is reviewed and merged later. Templated; an
    empty rendered value means the default. On the flow init operator, it's the branch of the flow run.
    Tasks in a `LaminDBFlow` default to their flow run's branch, and so do the records their function
    saves, unless the task sets its own `lamindb_branch`. Standalone tasks use the instance's current
    branch unless they set one. With `connect=True`, it's the branch of the records the
    function saves.

    The DAG file's transform stays on the branch lamindb finds it on (by default `main`), so steps and
    later flow runs on other branches still find it by its source. Steps find their flow run on any
    branch. An unknown branch fails the task before anything is recorded, and so do `archive` and
    `trash`. A retried flow init keeps its run's branch and fails if it's given another one.

`lamindb_version` (virtualenvs only)
:   The lamindb version to install, 2.8.1 or newer. Defaults to the instance's version on LaminHub,
    so that the virtualenv speaks the instance's schema, else the latest. The virtualenvs install
    `lamindb`. If you list `lamindb` or `lamindb-core` in `requirements` yourself, nothing is added;
    a bare `lamindb-core` also needs `numpy`, `pandas` and `pandera` for lineage tracking.

    Templated: a mapped task can pin its own version, for example from an upstream task's XCom, and
    gets its own virtualenv (and cache entry) for it. A template is checked once rendered, before
    anything is installed. A `LaminDBFlow` doesn't borrow the version of the tasks in it: pass its own
    `lamindb_version`, or its lifecycle operators install the instance's version (the latest without a
    connection). In DAGs that render templates as native objects,
    keep versions strings: a number such as `2.10` renders as `2.1`.

    ```python
    @task.lamindb_virtualenv(lamindb_version="{{ ti.xcom_pull(task_ids='pick_versions')[ti.map_index] }}")
    def process(sample: str) -> None: ...


    process.expand(sample=["S1", "S2"])
    ```

Lineage needs lamindb 2.8.1 or newer, and anything older fails deliberately. An older
`lamindb_version` fails when the DAG is parsed. An instance on an older lamindb fails the task
before its virtualenv is built. A step or flow task whose virtualenv or pod has an older lamindb,
for example from your own `requirements` or the pod's image, fails when it starts.

A task can use another connection than the rest of the DAG, for example to run under another API key:

```python
@task.lamindb_virtualenv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

Task connections are owned by the task. In a `LaminDBFlow`, the tasks must use the same instance as
the scope's flow run; a scoped task that connects to another instance fails before it starts.
Standalone tasks have no flow run to match.

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

| | Task run recorded | Parent flow run | Connected to the instance |
|---|---|---|---|
| default (`track=True`) | yes | only inside a `LaminDBFlow` | yes |
| `track=False, connect=True` | no | no | yes |
| `track=False` | no | no | no: the plain `@task.virtualenv` or `@task.kubernetes` |

With `connect=True`, a task that only reads or writes records gets the same setup as a step, without
lineage: a virtualenv gets lamindb (see `lamindb_version`) and the connection's credentials, and both
connect to the instance before the function runs. A pod connects with its own credentials, as for
steps. The task may connect to another instance than a flow run in the DAG,
for example one per mapped task through a templated `lamindb_instance`.

```python title="tests/system/lamindb/example_lamindb_lineage.py"
--8<-- "tests/system/lamindb/example_lamindb_lineage.py:howto_lamindb_connect_only"
```

Plain `track=False` runs the function unchanged, and lamindb is not added to `requirements`.

## Deployment setup

Some deployments need to prepare the virtualenvs and pods beyond what the provider does, for example
install a certificate bundle before lamindb connects, or pass more credentials. Subclass
[`LaminDBSetup`][airflow.providers.lamindb.execution.setup.LaminDBSetup] in a module your Airflow
installation can import:

```python title="tests/system/lamindb/site_setup.py"
--8<-- "tests/system/lamindb/site_setup.py:howto_lamindb_setup_class"
```

and name it in the provider's Airflow option, the same for DAG processors and workers:

```ini
[lamindb]
setup_class = mycompany.airflow.SiteSetup
```

or `AIRFLOW__LAMINDB__SETUP_CLASS=mycompany.airflow.SiteSetup`. Every LaminDB task then runs it, and
DAGs don't mention it. The class is loaded once per process; an import path that doesn't work fails
the DAGs when they are parsed.

- `requirements()` returns default requirements installed in every LaminDB virtualenv, besides
  lamindb, e.g. `["bionty", "pandas>=2"]`. They are added after the task's own `requirements` when it
  runs; a package the task lists itself is not added again, so a task can pin another version. They
  may not list lamindb: pin it with `lamindb_version`, or in the task's `requirements`. Pods don't
  install them: their image brings its packages.
- `bootstrap_source()` returns Python source that runs in the virtualenv or pod before lamindb is
  imported. It runs as its own module, so it may be indented or start with `from __future__`, and its
  names don't leak into the task's script. In a virtualenv it runs before Airflow unpickles the
  function's arguments; in a pod, after.
- `execution_env(context)` returns environment variables for the virtualenv. It's called on the
  worker when the task runs, after the templates are rendered and the connection resolved, and the
  values are set only while the task runs, never in its template fields. The provider's own
  variables (`LAMIN_SETTINGS_DIR`, `LAMIN_API_KEY`, `LAMIN_CACHE_DIR`) take precedence, and the task's
  own `env_vars` over both; each task's `env_vars` are restored afterwards, also when it fails. Keys
  and values must be strings. Pod steps don't call it: pods get their environment from the pod spec,
  e.g. its `secrets`. If it fails in the flow finish task, that task logs it and still closes the flow
  run.
- Tracked tasks, connect-only tasks and flow operators all run it, including the lifecycle operators
  of a `LaminDBFlow`.

### Overriding it

A `lamindb_setup` argument overrides the option; as with any argument, a task's own beats the DAG's
`default_args`:

```python
class SequencingSetup(SiteSetup):  # extends the installation's setup
    def execution_env(self, context):
        return {**super().execution_env(context), "SEQ_TOKEN": Variable.get("seq_token")}


with DAG("sequencing", default_args={"lamindb_setup": SequencingSetup()}):
    ...


@task.lamindb_virtualenv(lamindb_setup=LaminDBSetup())  # opts out: the base class does nothing
def plain_step() -> None: ...
```

The lifecycle operators of a `LaminDBFlow` are configured by the scope, not by the tasks in it, so
pass `lamindb_setup` to `LaminDBFlow(...)` when its init and finish need a setup other than the
installation's.

Setups are the same when their class and `repr` are, so each task may create its own instance.
`LaminDBSetup`'s `repr` names the class; a subclass that takes settings should include them in its
`repr`, which Airflow also uses to store `default_args` in the serialized DAG.

Keep deployment specifics, such as package indexes, cache paths and secret names, in your subclass.

## How the steps are recorded

The decorators reuse Airflow's mechanism of shipping the function's source text and append a small
wrapper that connects to the instance and records the outcome. In the virtualenv or pod, the task
calls `ln.track()` with the DAG file's source and the function's entrypoint. The transform identity
is the DAG's source file, not the function, with or without a `LaminDBFlow`; in a scope the task
also passes `initiated_by_run=` the scope's flow run. Nothing is pickled by reference, and the remote
environment only needs lamindb. Runs reference the Airflow task instance (by its log URL,
`reference_type="airflow_task_instance"`, where one is available).

## Limitations

- lamindb prompts on stdin when it finds a transform with the same source hash under a different key
  (for example after renaming a DAG file). Inside an Airflow task there is no stdin, so that task fails;
  run the DAG file once locally with `ln.track()` to resolve the rename.
- The pod variant embeds the DAG file source in the script passed via an environment variable; keep DAG
  files reasonably small.
