# apache-airflow-providers-lamindb

An [Apache Airflow](https://airflow.apache.org) 3 provider for [LaminDB](https://lamin.ai) with two parts:

- **[Lineage](#lineage-tracked-tasks-and-optional-flows)**: records tasks as LaminDB runs, and
  optionally DAG runs as flow runs around them, with `@task.lamindb_virtualenv` / `@task.lamindb_kubernetes`
  decorators that run lamindb in a virtualenv or a Kubernetes pod.
- **[Event-driven scheduling](#event-driven-scheduling)**: triggers DAGs on changes in LaminDB instances
  hosted on LaminHub, with triggers, sensors, filters and a hook for the
  [LaminHub REST API](https://docs.lamin.ai/rest).

Both parts use the same Airflow connection, and the worker never needs the `lamindb` package.

## Installation

```bash
pip install apache-airflow-providers-lamindb
pip install "apache-airflow-providers-lamindb[k8s]"  # adds @task.lamindb_kubernetes
```

Requires `apache-airflow>=3.3.0` and `apache-airflow-providers-standard`. Airflow 3.3.0 added the asset
state store that the event triggers use to resume after restarts.

Supports lamindb 2.8.1 and newer only, for LaminDB instances and for the lamindb that lineage tasks
run. Older versions fail deliberately with an explicit error.

Lineage tasks install `lamindb` in their own virtualenv or use the pod's image, so its version
does not have to fit Airflow's dependencies. Tested end to end with Airflow 3.3.2 and lamindb 2.8.1
and 2.11.0.

## Connection

Create the connection `lamindb_default` (type `lamindb`) with a Lamin API key and the instance slug
of a LaminDB instance hosted on LaminHub:

```bash
export AIRFLOW_CONN_LAMINDB_DEFAULT='{
    "conn_type": "lamindb",
    "password": "<lamin-api-key>",
    "extra": {"instance": "my-org/my-instance"}
}'
```

Every operator, decorator, trigger and sensor accepts `lamindb_conn_id` to use another connection,
for example a different API key for one task. See
[Connection types](https://danplischke.github.io/apache-airflow-providers-lamindb/connections/lamindb/).

## Lineage: tracked tasks and optional flows

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| task run | `Run` of that transform, `entrypoint=` the function name |
| `LaminDBFlow` scope in a DAG run (optional) | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"`; its tasks' runs have `initiated_by_run=` that flow run |

Tracked tasks are standalone by default: each task run is recorded with its source, parameters,
environment and outcome, with no flow run needed.

```python
from airflow.sdk import DAG, task

with DAG("my_pipeline") as dag:

    @task.lamindb_virtualenv
    def extract(count: int = 10) -> dict:
        return {"count": count}

    @task
    def double(data: dict) -> dict:
        # a plain Airflow task: not recorded in LaminDB
        return {"count": data["count"] * 2}

    @task.lamindb_virtualenv(requirements=["pandas"])
    def report(data: dict) -> None:
        import pandas as pd

        print(pd.Series([data["count"]]).describe())

    report(double(extract(count=3)))
```

To also record the DAG run as a flow run around some tasks, create them inside a `LaminDBFlow`:

```python
from airflow.providers.lamindb import LaminDBFlow

with DAG("my_pipeline_with_flow") as dag:
    with LaminDBFlow():
        report(double(extract(count=3)))
```

- `@task.lamindb_virtualenv(...)` works like `@task.virtualenv`; the function runs as a tracked task in a
  virtualenv that installs `lamindb` in the instance's version.
- `@task.lamindb_kubernetes(image=..., ...)` works like `@task.kubernetes`; the image needs
  lamindb and an API key for the instance, for example from a Kubernetes secret.
- `LaminDBFlow(...)` is an optional context manager. Every task invocation created inside it,
  mapped ones included, belongs to the scope; on exit it adds the flow's init and finish tasks (a
  virtualenv setup/teardown pair) around them. Tracked tasks in the scope get the flow run as their
  parent; plain tasks run inside the lifecycle but are not recorded. A task created outside the block
  and only wired inside it is not a member. Scopes can't be nested; a DAG has one default scope, and
  further scopes need an explicit `scope_id`: `LaminDBFlow("enrich")`. The scope's own arguments
  (`lamindb_conn_id`, `lamindb_version`, ...) configure its init and finish tasks; they are not
  borrowed from the tasks in it.
- `LaminDBFlowInitOperator` and `LaminDBFlowFinishOperator` remain exported as low-level building
  blocks, but wiring them with `with init >> finish:` does not make tracked tasks their children.

**Migrating:** `auto_flow` is removed, and `with init >> finish:` no longer parents tracked tasks.
Wrap the tasks in `with LaminDBFlow(...):` instead; plain Airflow tasks created in the block join the
flow too and wait for its init. See the
[migration guide](https://danplischke.github.io/apache-airflow-providers-lamindb/lineage/#migrating-to-lamindbflow).

All of them take `lamindb_conn_id` and `lamindb_instance`, the virtualenv ones also
`lamindb_version`, also through a DAG's `default_args`. Virtualenvs get the connection's API key and an empty, temporary
lamindb settings directory, so lamindb never reads the worker's `~/.lamin`. A step can use
another connection, for example to run under another API key:

```python
@task.lamindb_virtualenv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

See [Lineage](https://danplischke.github.io/apache-airflow-providers-lamindb/lineage/) for the flow scope, all settings, `track=False`, how
the steps are recorded and the limitations.

## Event-driven scheduling

- **`AssetWatcher` triggers** that run DAGs when:
  - artifacts are created (after their upload completed), updated or deleted
  - records of any registry (`core.run`, `core.collection`, `bionty.celltype`, ...) change,
    including merges of contribution branches and moves to the trash
  - branches (Change Requests) change their status, e.g. `review` → `merged`
  - comments or readmes are added to branches
- **Deferrable sensors** that wait for a branch status or for artifacts and records.
- **Filters** built in Python, such as `F(ArtifactField.KEY).startswith("raw/")`, with enums for
  registries, fields and operators.
- **LaminDBHook**, a sync and async client for the [LaminHub REST API](https://docs.lamin.ai/rest).

The triggers poll the LaminHub database write log ("Changes → Database writes") and keep their
cursor in Airflow's asset state store, so they resume after triggerer restarts. Events are delivered at
least once.

With the [connection](#connection) in place, run a DAG whenever a new FASTQ file is registered under `raw/` on the `main` branch:

```python
from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

new_fastqs = Asset(
    "lamindb_new_fastqs",
    watchers=[
        AssetWatcher(
            name="lamindb_new_fastqs_watcher",
            trigger=LaminDBArtifactEventTrigger(key_prefix="raw/", suffix=".fastq.gz"),
        )
    ],
)


@dag(schedule=[new_fastqs])
def process_fastqs():
    @task
    def process(triggering_asset_events=None):
        for event in triggering_asset_events[new_fastqs]:
            artifact = event.extra["payload"]["record"]
            print("new artifact", artifact["key"], artifact["uid"])

    process()


process_fastqs()
```

Run a DAG when a Change Request is ready for review:

```python
from airflow.providers.lamindb.triggers.branches import LaminDBBranchStatusEventTrigger

review_requested = Asset(
    "lamindb_review_requested",
    watchers=[
        AssetWatcher(
            name="lamindb_review_requested_watcher",
            trigger=LaminDBBranchStatusEventTrigger(to_status="review"),
        )
    ],
)
```

Wait for a branch to be merged, from within a DAG:

```python
from airflow.providers.lamindb.sensors.branches import LaminDBBranchStatusSensor

LaminDBBranchStatusSensor(task_id="wait_for_merge", branch="my-branch", deferrable=True)
```

Wait for a completed run of a script, filtering with enums instead of LaminHub REST filter dicts:

```python
from airflow.providers.lamindb.sensors.records import LaminDBRecordSensor
from airflow.providers.lamindb.client.filters import (
    F,
    LaminDBRegistry,
    RunField,
    RunStatus,
    TransformField,
)

LaminDBRecordSensor(
    task_id="wait_for_run",
    registry=LaminDBRegistry.RUN,
    filter=(F(RunField.TRANSFORM, TransformField.KEY) == "preprocess.py")
    & (F(RunField.STATUS_CODE) == RunStatus.COMPLETED),
    deferrable=True,
)
```

## Documentation

The documentation is at <https://danplischke.github.io/apache-airflow-providers-lamindb/>:

- [Connection types](https://danplischke.github.io/apache-airflow-providers-lamindb/connections/lamindb/)
- [Lineage](https://danplischke.github.io/apache-airflow-providers-lamindb/lineage/)
- [Triggers (event-driven scheduling)](https://danplischke.github.io/apache-airflow-providers-lamindb/triggers/)
- [Sensors and hook](https://danplischke.github.io/apache-airflow-providers-lamindb/sensors/)
- [Filters](https://danplischke.github.io/apache-airflow-providers-lamindb/filters/)
- [API reference](https://danplischke.github.io/apache-airflow-providers-lamindb/api/)
- [Example DAGs](https://github.com/danplischke/apache-airflow-providers-lamindb/tree/main/tests/system/lamindb)
- [Changelog](https://danplischke.github.io/apache-airflow-providers-lamindb/changelog/)

## Development

See [CONTRIBUTING.md](https://github.com/danplischke/apache-airflow-providers-lamindb/blob/main/CONTRIBUTING.md). In short, with [uv](https://docs.astral.sh/uv/) and
[just](https://just.systems/):

```bash
just dev     # install the dev and docs dependencies and the pre-commit hooks
just check   # lint, type check, unit tests, docs, package build and smoke test, as the CI does
just standalone  # Airflow at http://localhost:8080 with sample DAGs and a local lamindb instance
```

## License

MIT
