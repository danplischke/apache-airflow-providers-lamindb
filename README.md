# lamindb-airflow

Apache Airflow 3 provider that records DAG runs as [LaminDB](https://lamin.ai) flow runs
and tasks as steps.

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

## Installation

```bash
pip install lamindb-airflow                     # virtualenv steps only: the worker needs no lamindb
pip install "lamindb-airflow[lamindb]"          # adds in-process steps (@task.lamindb, LaminDBStepOperator)
pip install "lamindb-airflow[cncf.kubernetes]"  # adds @task.lamindb_k8s
```

Requires `apache-airflow>=3.0` and `apache-airflow-providers-standard`. Wherever lamindb
runs (worker, virtualenv or pod) it must be able to connect to the instance, for example
via `LAMIN_API_KEY` and `LAMIN_CURRENT_INSTANCE`.

Version compatibility is constrained by Airflow's and lamindb's shared `universal-pathlib`
pin, not by this package:

| Airflow | newest lamindb that installs alongside |
|---|---|
| 3.1.x | 2.4.2 |
| 3.2.0 and later | 2.10.0 (current latest) |

Tested end to end with Airflow 3.3.2 + lamindb 2.10.0.

## Usage

```python
import lamindb as ln
from airflow.sdk import DAG, task

from lamindb_airflow import LaminDBStepOperator


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()  # optional: an already-decorated function is used as is
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


with DAG("my_pipeline") as dag:
    t1 = LaminDBStepOperator(task_id="extract", python_callable=extract, op_kwargs={"count": 3})
    t2 = LaminDBStepOperator(task_id="transform", python_callable=transform, op_args=[t1.output])

    @task.lamindb
    def load(data: dict) -> int:
        return data["count"]

    @task.lamindb_venv(requirements=["pandas"])
    def report(count: int) -> None:
        import pandas as pd

        print(pd.Series([count]).describe())

    t1 >> t2 >> report(load(t2.output))
```

Each step adds the DAG's `lamindb_flow_init` and `lamindb_flow_finish` tasks on first use
and wires `init >> step >> finish` (`auto_flow=True`). To configure them, declare them
yourself before the steps; they are reused:

```python
from lamindb_airflow import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

with DAG("my_pipeline") as dag:
    init = LaminDBFlowInitOperator(retries=3)
    finish = LaminDBFlowFinishOperator()
    ...
```

How it works:

- The flow init task calls `ln.track()` on the DAG file and tags the run with the DAG
  run id. Retrying it restarts the same run instead of creating a second one.
- Every step looks the flow run up by that tag. No XCom plumbing, any DAG topology. A
  step whose flow init has not run fails with a clear error.
- The step callable is wrapped with `ln.step()` at execution time, so lamindb records
  source, parameters and outcome and links the step run to the flow run.
- The flow finish task closes the flow run as `completed`, or `errored` if any task in
  the DAG run failed.
- Init is an Airflow *setup* task and finish a *teardown* task. Finish therefore runs
  after the steps even when they fail, and Airflow ignores it when deciding the DAG run
  state, so a failed step still fails the DAG run. Pass `is_setup=False` /
  `is_teardown=False` to opt out.

Auto-wiring caveats:

- The first tracked LaminDB step decides the kind of flow tasks: in-process for
  `LaminDBStepOperator` / `@task.lamindb`, virtualenv for `@task.lamindb_venv` (copying
  its Python version, index, lamindb pin and env settings) and `@task.lamindb_k8s`.
- Mapped steps (`.expand()`) are not auto-wired. Declare the flow operators and wire
  `init >> mapped_step >> finish` yourself.
- Flow operators declared *after* an auto-wiring step clash with the auto-added task id;
  declare them first, or pass `auto_flow=False` to the steps.
- Finish waits for LaminDB steps only; wire other tasks upstream of it if the flow run
  should cover them.

### Untracked tasks

Pass `track=False` to use the instance from a task without recording anything:

```python
@task.lamindb(track=False)
def count_artifacts() -> int:
    return ln.Artifact.filter().count()


@task.lamindb_venv(track=False, requirements=["pandas"])
def row_count(key: str) -> int:
    import lamindb as ln

    return len(ln.Artifact.get(key=key).load())
```

- No flow run is needed or created. Untracked tasks are never auto-wired, so a DAG with
  only untracked tasks gets no flow init/finish tasks.
- In-process, the function is called directly; lamindb must be installed on the worker.
  A callable decorated with `@ln.step()` / `@ln.flow()` runs undecorated: `@ln.step()`
  would fail without a flow run and `@ln.flow()` would record one.
- The virtualenv and pod variants still add the lamindb requirement and connect to
  `lamindb_instance` before calling the function.
- Artifacts saved from an untracked task have no run; lamindb warns about that.

## Operators and decorators

| | needs lamindb on the worker |
|---|---|
| `LaminDBFlowInitOperator(task_id="lamindb_flow_init")` | yes |
| `LaminDBFlowFinishOperator(task_id="lamindb_flow_finish")` | yes |
| `LaminDBVenvFlowInitOperator(...)`, `LaminDBVenvFlowFinishOperator(...)` – same, in a virtualenv; accept `PythonVirtualenvOperator` arguments | no |
| `LaminDBStepOperator(task_id, python_callable, op_args=None, op_kwargs=None)` | yes |
| `@task.lamindb` – TaskFlow variant of `LaminDBStepOperator` | yes |
| `@task.lamindb_venv(...)` – like `@task.virtualenv`; the function runs as a step in the virtualenv | no |
| `@task.lamindb_k8s(image=..., ...)` – like `@task.kubernetes`; the image needs lamindb and credentials | no |

All step operators accept `auto_flow` and `track` (see [Untracked tasks](#untracked-tasks)).
The virtualenv and pod variants also accept
`lamindb_instance`, the instance slug to connect to (default: the worker's instance if
lamindb is set up there, else `LAMIN_CURRENT_INSTANCE`). The virtualenv variants accept
`lamindb_version`: `lamindb==<version>` is added to `requirements` unless you list
lamindb yourself (default: the worker's version if installed, else the latest). The
remote side must speak the instance's schema version.

The remote variants reuse Airflow's mechanism of shipping the function's source text and
append a small wrapper that connects to the instance, binds the step to the flow run and
records the outcome. Nothing is pickled by reference, and the remote environment only
needs `lamindb`. Their step runs reference the Airflow task instance log URL
(`reference_type="airflow_task_instance"`).

## Notes

- lamindb prompts on stdin when it finds a transform with the same source hash under a
  different key (for example after renaming a DAG file). Inside an Airflow task there is
  no stdin, so that task fails; run the DAG file once locally with `ln.track()` to
  resolve the rename.
- The pod variant embeds the DAG file source in the script passed via an environment
  variable; keep DAG files reasonably small.

## Tests

```bash
pytest tests/unit                        # lamindb mocked

# use a throwaway instance: keep ~/.lamin away from your real settings
export HOME=/tmp/lamin-home
lamin init --storage /tmp/lamin-home/store --name airflowtest
LAMINDB_INTEGRATION_TEST=1 pytest tests/integration   # operators against a real instance

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system
airflow db migrate
LAMINDB_E2E_TEST=1 pytest tests/system             # dag.test(): real task runner + virtualenv
```

## License

MIT
