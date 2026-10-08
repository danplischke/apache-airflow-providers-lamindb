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
for example a different API key for one task. See [docs/connections/lamindb.rst](docs/connections/lamindb.rst).

## Lineage: DAG runs as flows, tasks as steps

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

```python
from airflow.sdk import DAG, task

with DAG("my_pipeline") as dag:

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
  a virtualenv, as Airflow setup and teardown tasks. Each step adds and wires them on first
  use; declare them before the steps to configure them:

  ```python
  from airflow.providers.lamindb import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

  with DAG("my_pipeline") as dag:
      init = LaminDBFlowInitOperator(retries=3)
      finish = LaminDBFlowFinishOperator()
      ...
  ```

All of them take `lamindb_conn_id` and `lamindb_instance`, the virtualenv ones also
`lamindb_version`, also through a DAG's `default_args`. Virtualenvs get the connection's API key and an empty, temporary
lamindb settings directory, so lamindb never reads the worker's `~/.lamin`. A step can use
another connection, for example to run under another API key:

```python
@task.lamindb_venv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

See [docs/lineage.rst](docs/lineage.rst) for auto-wiring, all settings, `track=False`, how
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

- [Connection](docs/connections/lamindb.rst)
- [Lineage](docs/lineage.rst)
- [Triggers (event-driven scheduling)](docs/triggers.rst)
- [Sensors and hook](docs/sensors.rst)
- [Filters](docs/filters.rst)
- [Example DAGs](tests/system/lamindb)
- [Changelog](docs/changelog.rst)

## Development

The layout follows the provider packages in the `apache/airflow` repository (`provider.yaml`,
`get_provider_info.py`, `tests/unit`, `tests/system`, `docs`). Keep `provider.yaml` and
`get_provider_info.py` in sync; a unit test checks that they match.

```bash
uv sync
uv run pytest tests/unit                 # lamindb and the LaminHub API mocked
uv run ruff check . && uv run ruff format --check .
uv run --isolated --python 3.10 mypy    # type-check on the oldest supported Python
```

Integration and system tests run against a real lamindb instance:

```bash
# use a throwaway instance: keep ~/.lamin away from your real settings
export HOME=/tmp/lamin-home
lamin init --storage /tmp/lamin-home/store --name airflowtest
LAMINDB_INTEGRATION_TEST=1 pytest tests/integration   # shipped sources against a real instance

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/lamindb
airflow db migrate
LAMINDB_E2E_TEST=1 pytest tests/system             # dag.test(): real task runner + virtualenv
```

## License

MIT
