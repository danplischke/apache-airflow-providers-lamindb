"""Example DAG waiting for LaminDB branches, artifacts and runs with deferrable sensors."""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.sensors.branches import LaminDBBranchStatusSensor
from airflow.providers.lamindb.sensors.records import LaminDBArtifactSensor, LaminDBRecordSensor
from airflow.providers.lamindb.utils.filters import (
    F,
    LaminDBRegistry,
    RunField,
    RunStatus,
    TransformField,
)


@dag(
    schedule=None,
    tags=["example", "lamindb"],
    params={"branch": Param("my-branch", type="string"), "key": Param("raw/sample1.fastq.gz", type="string")},
)
def example_lamindb_sensors():
    # [START howto_sensor_lamindb_branch_status]
    wait_for_merge = LaminDBBranchStatusSensor(
        task_id="wait_for_merge",
        branch="{{ params.branch }}",
        target_status="merged",
        failure_status="closed",
        deferrable=True,
        poke_interval=60,
    )
    # [END howto_sensor_lamindb_branch_status]

    # [START howto_sensor_lamindb_artifact]
    wait_for_artifact = LaminDBArtifactSensor(
        task_id="wait_for_artifact",
        key="{{ params.key }}",
        deferrable=True,
        poke_interval=60,
    )
    # [END howto_sensor_lamindb_artifact]

    # [START howto_sensor_lamindb_record]
    wait_for_run = LaminDBRecordSensor(
        task_id="wait_for_run",
        registry=LaminDBRegistry.RUN,
        # completed runs of the transform (script, notebook, ...) with the key "preprocess.py"
        filter=(F(RunField.TRANSFORM, TransformField.KEY) == "preprocess.py")
        & (F(RunField.STATUS_CODE) == RunStatus.COMPLETED),
        deferrable=True,
        poke_interval=60,
    )
    # [END howto_sensor_lamindb_record]

    # [START howto_hook_lamindb]
    @task
    def summarize(branch, artifacts, runs):
        hook = LaminDBHook()
        recent_runs = hook.query_records(
            LaminDBRegistry.RUN, F(RunField.FINISHED_AT).is_not_null(), order_by=["-started_at"], limit=5
        )
        print(f"Branch {branch['name']} is {branch['status']}; artifact {artifacts[0]['uid']} is ready")
        print(f"Run {runs[0]['uid']} completed; recent runs: {[run['uid'] for run in recent_runs]}")

    # [END howto_hook_lamindb]

    summarize(wait_for_merge.output, wait_for_artifact.output, wait_for_run.output)


example_lamindb_sensors()
