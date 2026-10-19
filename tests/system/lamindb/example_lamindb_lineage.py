"""Example DAGs for apache-airflow-providers-lamindb.

test_example_lamindb_lineage.py runs them through dag.test().

LaminDB only runs inside the virtualenvs, so the worker needs no lamindb. The examples pass
``system_site_packages=True`` so the virtualenvs reuse the lamindb of the test environment.
"""

from __future__ import annotations

from airflow.sdk import DAG, task

from airflow.providers.lamindb.operators.flow import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

# Init/finish wrap the steps; Airflow wires them to the steps' roots and leaves.
with DAG("lamindb_example_ok") as dag_ok:
    # --8<-- [start:howto_decorator_lamindb_venv]
    @task.lamindb_venv(system_site_packages=True)
    def extract(count: int = 10) -> dict:
        return {"count": count}

    @task.lamindb_venv(system_site_packages=True)
    def transform(data: dict) -> dict:
        return {"count": data["count"] * 2}

    @task
    def load(data: dict) -> int:
        # a plain Airflow task: not recorded in LaminDB
        return data["count"] + 1

    @task.lamindb_venv(system_site_packages=True)
    def report(value: int) -> dict:
        import lamindb as ln

        assert ln.context.run is not None, "step run context missing in venv"
        return {"value": value * 10, "run_uid": ln.context.run.uid}

    # --8<-- [end:howto_decorator_lamindb_venv]

    # --8<-- [start:howto_operator_lamindb_flow]
    init = LaminDBFlowInitOperator(system_site_packages=True)
    finish = LaminDBFlowFinishOperator(system_site_packages=True)
    with init >> finish:
        report(load(transform(extract(count=3))))
    # --8<-- [end:howto_operator_lamindb_flow]


with (
    DAG("lamindb_example_fail") as dag_fail,
    LaminDBFlowInitOperator(system_site_packages=True)
    >> LaminDBFlowFinishOperator(system_site_packages=True),
):

    @task.lamindb_venv(system_site_packages=True)
    def boom() -> None:
        raise ValueError("step failed on purpose")

    boom()


# auto_flow adds flow tasks that copy the first step's virtualenv settings.
with DAG("lamindb_example_auto_flow") as dag_auto_flow:
    # --8<-- [start:howto_lamindb_auto_flow]
    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def venv_extract(count: int = 10) -> dict:
        return {"count": count}

    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def venv_double(data: dict) -> dict:
        return {"count": data["count"] * 2}

    venv_double(venv_extract(count=4))
    # --8<-- [end:howto_lamindb_auto_flow]


# Arguments that read Airflow Variables or Connections through templates are not recorded.
with (
    DAG("lamindb_example_params", params={"sample": "S1"}) as dag_params,
    LaminDBFlowInitOperator(system_site_packages=True)
    >> LaminDBFlowFinishOperator(system_site_packages=True) as flow,
):
    # --8<-- [start:howto_lamindb_recorded_params]
    @task.lamindb_venv(system_site_packages=True, lamindb_exclude_params=["session"])
    def fetch(sample: str, credentials: str, session: str = "default") -> dict:
        return {"sample": sample, "credentials_length": len(credentials)}

    # records only sample: credentials reads a Variable, and session is excluded
    fetch("{{ params.sample }}", "{{ var.value.lims_credentials }}")
    # --8<-- [end:howto_lamindb_recorded_params]

    flow.add_task(
        fetch.override(task_id="fetch_mapped")
        .partial(credentials="{{ var.value.lims_credentials }}")
        .expand(sample=["S2", "S3"])
    )


# connect=True with track=False: connected to the instance, but nothing is recorded and no flow run
# is needed.
with DAG("lamindb_example_connect") as dag_connect:
    # --8<-- [start:howto_lamindb_connect_only]
    @task.lamindb_venv(system_site_packages=True, track=False, connect=True)
    def count_artifacts() -> int:
        import lamindb as ln

        return ln.Artifact.filter().count()

    count_artifacts()
    # --8<-- [end:howto_lamindb_connect_only]


# auto_flow wires mapped steps too.
with DAG("lamindb_example_mapped", params={"samples": ["S1", "S2"], "fail_on": ""}) as dag_mapped:
    # --8<-- [start:howto_lamindb_mapped_steps]
    @task
    def list_samples(**context) -> list[str]:
        return context["params"]["samples"]

    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def measure(sample: str, fail_on: str = "") -> int:
        if sample == fail_on:
            raise ValueError(f"measuring {sample} failed on purpose")
        return len(sample)

    measure.partial(fail_on="{{ params.fail_on }}").expand(sample=list_samples())
    # --8<-- [end:howto_lamindb_mapped_steps]


# With the [lamindb] setup_class option set to system.lamindb.site_setup.SiteSetup, every LaminDB task
# of the installation runs that setup, auto-wired flow tasks included; the DAG doesn't mention it.
with DAG("lamindb_example_setup") as dag_setup:

    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def use_site_setup() -> None:
        import os

        assert os.environ["SITE_LAMINDB_PRELOADED"] == "False"
        assert os.environ["SITE_TOKEN"]

    use_site_setup()
