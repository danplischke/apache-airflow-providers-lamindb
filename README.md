# lamindb-airflow

An [Apache Airflow](https://airflow.apache.org) 3 provider for [LaminDB](https://lamin.ai) with two parts:

- **[Lineage](#lineage-dag-runs-as-flows-tasks-as-steps)**: records DAG runs as LaminDB flow runs and
  tasks as steps, with flow operators and `@task.lamindb_venv` / `@task.lamindb_k8s` decorators that
  run lamindb in a virtualenv or a Kubernetes pod.
- **[Event-driven scheduling](#event-driven-scheduling)**: triggers DAGs on changes in LaminDB instances
  hosted on LaminHub, with triggers, sensors, filters and a hook for the
  [LaminHub REST API](https://docs.lamin.ai/rest).

Both parts use the same Airflow connection, and the worker never needs the `lamindb` package.

## Installation

```bash
pip install lamindb-airflow
pip install "lamindb-airflow[cncf.kubernetes]"  # adds @task.lamindb_k8s
```

Requires `apache-airflow>=3.3.0` and `apache-airflow-providers-standard`. Airflow 3.3.0 added the asset
state store that the event triggers use to resume after restarts.

Lineage tasks install `lamindb-core` in their own virtualenv or use the pod's image, so its version
does not have to fit Airflow's dependencies. Tested end to end with Airflow 3.3.2 + lamindb 2.10.0.

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

## Lineage: DAG runs as flows, tasks as steps

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

```python
from airflow.providers.lamindb import LaminDBFlowFinishOperator, LaminDBFlowInitOperator
from airflow.sdk import DAG, task

with DAG("my_pipeline") as dag:
    with LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():

        @task.lamindb_venv
        def extract(count: int = 10) -> dict:
            return {"count": count}

        @task
        def double(data: dict) -> dict:
            # a plain Airflow task: not recorded in LaminDB
            return {"count": data["count"] * 2}

        @task.lamindb_venv(requirements=["pandas"])
        def report(data: dict) -> None:
            import pandas as pd

            print(pd.Series([data["count"]]).describe())

        report(double(extract(count=3)))
```

- `@task.lamindb_venv(...)` works like `@task.virtualenv`; the function runs as a step in a
  virtualenv that installs `lamindb-core` in the instance's version.
- `@task.lamindb_k8s(image=..., ...)` works like `@task.kubernetes`; the image needs
  lamindb and an API key for the instance, for example from a Kubernetes secret.
- `LaminDBFlowInitOperator` and `LaminDBFlowFinishOperator` open and close the flow run in
  a virtualenv, as Airflow setup and teardown tasks. So `with init >> finish:` is Airflow's
  setup/teardown context: when the block ends, Airflow wires init upstream of the tasks in it
  that have no upstream, and finish downstream of the ones that have no downstream. Every task
  in the block, LaminDB step or not, runs inside the flow run. Airflow does not track mapped
  tasks (`.expand()`) in the context; add them with
  `with init >> finish as scope: ... scope.add_task(mapped)` if they are a root or leaf.
  Alternatively, pass `auto_flow=True` to the steps: each one adds the DAG's flow tasks on
  first use and wires `init >> step >> finish`.

All of them take `lamindb_conn_id` and `lamindb_instance`, the virtualenv ones also
`lamindb_version`, also through a DAG's `default_args`. Virtualenvs get the connection's API key and an empty, temporary
lamindb settings directory, so lamindb never reads the worker's `~/.lamin`. A step can use
another connection, for example to run under another API key:

```python
@task.lamindb_venv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

See [Lineage](https://danplischke.github.io/apache-airflow-providers-lamindb/lineage/) for auto-wiring, all settings, `track=False`, how
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
from airflow.providers.lamindb.utils.filters import (
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
- [Example DAGs](tests/system/lamindb)
- [Changelog](https://danplischke.github.io/apache-airflow-providers-lamindb/changelog/)

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). In short, with [uv](https://docs.astral.sh/uv/) and
[just](https://just.systems/):

```bash
just dev     # install the dev and docs dependencies and the pre-commit hooks
just check   # lint, type check, unit tests, docs, package build and smoke test, as the CI does
```

## License

MIT
