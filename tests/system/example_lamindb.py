"""Example DAGs for lamindb-airflow, run by test_example_dags.py through dag.test()."""

import lamindb as ln
from airflow.sdk import DAG, task

from airflow.providers.lamindb.operators.flow import LaminDBFlowFinishOperator, LaminDBFlowInitOperator
from airflow.providers.lamindb.operators.step import LaminDBStepOperator


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


def boom() -> None:
    raise ValueError("step failed on purpose")


with DAG("lamindb_example_ok") as dag_ok:
    init = LaminDBFlowInitOperator()
    finish = LaminDBFlowFinishOperator()
    t_extract = LaminDBStepOperator(task_id="extract", python_callable=extract, op_kwargs={"count": 3})
    t_transform = LaminDBStepOperator(task_id="transform", python_callable=transform, op_args=[t_extract.output])

    @task.lamindb
    def load(data: dict) -> int:
        return data["count"] + 1

    @task.lamindb_venv(system_site_packages=True)
    def venv_step(value: int) -> dict:
        import lamindb as ln

        assert ln.context.run is not None, "step run context missing in venv"
        return {"venv_value": value * 10, "run_uid": ln.context.run.uid}

    init >> t_extract >> t_transform >> venv_step(load(t_transform.output)) >> finish


# Init/finish wrap the steps; Airflow wires them to the steps' roots and leaves.
with DAG("lamindb_example_fail") as dag_fail, LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():
    LaminDBStepOperator(task_id="boom", python_callable=boom)


# Everything LaminDB-related runs in virtualenvs: the worker needs no lamindb.
# auto_flow adds flow tasks that copy the first step's virtualenv settings.
with DAG("lamindb_example_venv_only") as dag_venv_only:

    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def venv_extract(count: int = 10) -> dict:
        return {"count": count}

    @task.lamindb_venv(system_site_packages=True, auto_flow=True)
    def venv_double(data: dict) -> dict:
        return {"count": data["count"] * 2}

    venv_double(venv_extract(count=4))
