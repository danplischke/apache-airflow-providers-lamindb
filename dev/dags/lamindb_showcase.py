"""Lineage sample DAGs for `just standalone`.

They record into the throwaway lamindb instance in .cache/lamin-home, or into a hosted instance if
AIRFLOW_CONN_LAMINDB_DEFAULT is exported before `just standalone`. The virtualenvs reuse the lamindb
of .venv.
"""

from __future__ import annotations

import os

from airflow.sdk import DAG, Param, task

from airflow.providers.lamindb.operators.flow import (
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
)

LOCAL_INSTANCE = "anonymous/airflowtest"

if "AIRFLOW_CONN_LAMINDB_DEFAULT" in os.environ:
    LAMINDB_ARGS: dict = {"system_site_packages": True}
else:
    # the local instance is unknown to LaminHub, so the virtualenvs use lamindb's own settings
    LAMINDB_ARGS = {"system_site_packages": True, "lamindb_conn_id": None, "lamindb_instance": LOCAL_INSTANCE}

with DAG(
    "lamindb_showcase",
    schedule=None,
    default_args=LAMINDB_ARGS,
    params={"count": Param(3, type="integer", minimum=1, maximum=10, description="number of values")},
    render_template_as_native_obj=True,
    tags=["lamindb"],
):

    @task.lamindb_venv
    def measure(count: int) -> list[float]:
        import random

        return [round(random.uniform(1, 2), 3) for _ in range(count)]

    @task.lamindb_venv
    def square(value: float) -> float:
        return round(value**2, 3)

    @task
    def scale(values: list[float], factor: float) -> list[float]:
        # a plain Airflow task, not recorded in LaminDB; it also turns the lazy sequence of the mapped
        # results into a list, which a virtualenv task cannot receive
        return [round(v * factor, 3) for v in values]

    @task.lamindb_venv
    def save_table(values: list[float]) -> str:
        import csv
        import tempfile
        from pathlib import Path

        import lamindb as ln

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "squares.csv"
            with path.open("w", newline="") as f:
                csv.writer(f).writerows([v] for v in values)
            artifact = ln.Artifact(path, key="airflow-showcase/squares.csv").save()
        return artifact.uid

    @task.lamindb_venv
    def summarize(uid: str) -> dict:
        import csv

        import lamindb as ln

        # loading the artifact records it as an input of this step
        with ln.Artifact.get(uid).cache().open() as f:
            values = [float(row[0]) for row in csv.reader(f)]
        return {"n": len(values), "mean": round(sum(values) / len(values), 3)}

    with LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():
        squares = square.expand(value=measure("{{ params.count }}"))
        summarize(save_table(scale(squares, factor=10)))


with DAG(
    "lamindb_showcase_auto_flow",
    schedule=None,
    default_args=LAMINDB_ARGS,
    params={"fail": Param(False, type="boolean", description="fail validate, so the flow run errors")},
    render_template_as_native_obj=True,
    tags=["lamindb"],
):

    @task.lamindb_venv(auto_flow=True)
    def download() -> dict:
        return {"rows": 100}

    @task.lamindb_venv(auto_flow=True)
    def validate(data: dict, fail: bool) -> dict:
        if fail:
            raise ValueError("validation failed on purpose")
        return data

    validate(download(), "{{ params.fail }}")
