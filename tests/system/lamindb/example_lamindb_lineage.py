"""Example DAGs for apache-airflow-providers-lamindb.

test_example_lamindb_lineage.py runs them through dag.test().

LaminDB only runs inside the virtualenvs, so the worker needs no lamindb. The examples pass
``system_site_packages=True`` so the virtualenvs reuse the lamindb of the test environment.
"""

from __future__ import annotations

from airflow.sdk import DAG, task

from airflow.providers.lamindb import LaminDBFlow
from airflow.providers.lamindb.operators.flow import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

# A LaminDBFlow block gives its tasks a flow run for the DAG run; tasks outside it are standalone.
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

    # --8<-- [start:howto_lamindb_flow_scope]
    with LaminDBFlow(system_site_packages=True):
        report(load(transform(extract(count=3))))
    # --8<-- [end:howto_lamindb_flow_scope]


# Without a LaminDBFlow, every tracked task is standalone: a run of the DAG file's transform, no
# parent flow run.
with DAG("lamindb_example_standalone") as dag_standalone:

    @task.lamindb_venv(system_site_packages=True)
    def solo(count: int = 5) -> int:
        return count + 1

    @task.lamindb_venv(system_site_packages=True)
    def solo_next(value: int) -> int:
        return value * 2

    solo_next(solo(count=4))


# The flow operators on their own are Airflow's setup and teardown only: they do not parent the tasks.
with DAG("lamindb_example_operators") as dag_operators:

    @task.lamindb_venv(system_site_packages=True)
    def inner() -> int:
        return 1

    # --8<-- [start:howto_operator_lamindb_flow]
    init = LaminDBFlowInitOperator(system_site_packages=True)
    finish = LaminDBFlowFinishOperator(system_site_packages=True)
    with init >> finish:
        inner()  # still standalone: use `with LaminDBFlow():` for a parent flow run
    # --8<-- [end:howto_operator_lamindb_flow]


with (
    DAG("lamindb_example_fail") as dag_fail,
    LaminDBFlow(system_site_packages=True),
):

    @task.lamindb_venv(system_site_packages=True)
    def boom() -> None:
        raise ValueError("step failed on purpose")

    boom()


# Sibling scopes need a stable scope_id. A failure in one flow run doesn't touch the other, nor a
# standalone task.
with DAG(
    "lamindb_example_scopes", params={"fail_enrich": False}, render_template_as_native_obj=True
) as dag_scopes:

    @task.lamindb_venv(system_site_packages=True)
    def stage_one() -> int:
        return 1

    @task.lamindb_venv(system_site_packages=True)
    def stage_two(fail: bool = False) -> int:
        if fail:
            raise ValueError("enrich failed on purpose")
        return 2

    outside = stage_one.override(task_id="outside")()
    with LaminDBFlow(system_site_packages=True):
        stage_one.override(task_id="first")()
    with LaminDBFlow("enrich", system_site_packages=True):
        stage_two.override(task_id="second")("{{ params.fail_enrich }}")


# Arguments that read Airflow Variables or Connections through templates are not recorded.
with (
    DAG("lamindb_example_params", params={"sample": "S1"}) as dag_params,
    LaminDBFlow(system_site_packages=True),
):
    # --8<-- [start:howto_lamindb_recorded_params]
    @task.lamindb_venv(system_site_packages=True, lamindb_exclude_params=["session"])
    def fetch(sample: str, credentials: str, session: str = "default") -> dict:
        return {"sample": sample, "credentials_length": len(credentials)}

    # records only sample: credentials reads a Variable, and session is excluded
    fetch("{{ params.sample }}", "{{ var.value.lims_credentials }}")
    # --8<-- [end:howto_lamindb_recorded_params]

    fetch.override(task_id="fetch_mapped").partial(credentials="{{ var.value.lims_credentials }}").expand(
        sample=["S2", "S3"]
    )


# connect=True with track=False: connected to the instance, but nothing is recorded.
with DAG("lamindb_example_connect") as dag_connect:
    # --8<-- [start:howto_lamindb_connect_only]
    @task.lamindb_venv(system_site_packages=True, track=False, connect=True)
    def count_artifacts() -> int:
        import lamindb as ln

        return ln.Artifact.filter().count()

    count_artifacts()
    # --8<-- [end:howto_lamindb_connect_only]


# Mapped steps are tracked per map index, and join a LaminDBFlow created around them.
with DAG("lamindb_example_mapped", params={"samples": ["S1", "S2"], "fail_on": ""}) as dag_mapped:
    # --8<-- [start:howto_lamindb_mapped_steps]
    @task
    def list_samples(**context) -> list[str]:
        return context["params"]["samples"]

    @task.lamindb_venv(system_site_packages=True)
    def measure(sample: str, fail_on: str = "") -> int:
        if sample == fail_on:
            raise ValueError(f"measuring {sample} failed on purpose")
        return len(sample)

    with LaminDBFlow(system_site_packages=True):
        measure.partial(fail_on="{{ params.fail_on }}").expand(sample=list_samples())
    # --8<-- [end:howto_lamindb_mapped_steps]


# With the [lamindb] setup_class option set to system.lamindb.site_setup.SiteSetup, every LaminDB task
# of the installation runs that setup, a flow scope's tasks included; the DAG doesn't mention it.
with DAG("lamindb_example_setup") as dag_setup:

    @task.lamindb_venv(system_site_packages=True)
    def use_site_setup() -> None:
        import os

        assert os.environ["SITE_LAMINDB_PRELOADED"] == "False"
        assert os.environ["SITE_TOKEN"]

    with LaminDBFlow(system_site_packages=True):
        use_site_setup()
