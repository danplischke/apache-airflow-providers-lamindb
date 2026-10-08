"""Example DAGs for lamindb-airflow, run by test_example_lamindb_lineage.py through dag.test().

LaminDB only runs inside the virtualenvs, so the worker needs no lamindb. The examples pass
``system_site_packages=True`` so the virtualenvs reuse the lamindb of the test environment.
"""

from __future__ import annotations

from airflow.sdk import DAG, task

from airflow.providers.lamindb.operators.flow import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

# Flow init/finish declared explicitly to configure them; the steps reuse them.
with DAG("lamindb_example_ok") as dag_ok:
    # [START howto_operator_lamindb_flow]
    LaminDBFlowInitOperator(system_site_packages=True)
    LaminDBFlowFinishOperator(system_site_packages=True)
    # [END howto_operator_lamindb_flow]

    # [START howto_decorator_lamindb_venv]
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

    report(load(transform(extract(count=3))))
    # [END howto_decorator_lamindb_venv]


# Flow init/finish are added and wired by the step (auto_flow=True).
with DAG("lamindb_example_fail") as dag_fail:

    @task.lamindb_venv(system_site_packages=True)
    def boom() -> None:
        raise ValueError("step failed on purpose")

    boom()


# The auto-added flow tasks copy the first step's virtualenv settings.
with DAG("lamindb_example_auto_flow") as dag_auto_flow:
    # [START howto_lamindb_auto_flow]
    @task.lamindb_venv(system_site_packages=True)
    def venv_extract(count: int = 10) -> dict:
        return {"count": count}

    @task.lamindb_venv(system_site_packages=True)
    def venv_double(data: dict) -> dict:
        return {"count": data["count"] * 2}

    venv_double(venv_extract(count=4))
    # [END howto_lamindb_auto_flow]
