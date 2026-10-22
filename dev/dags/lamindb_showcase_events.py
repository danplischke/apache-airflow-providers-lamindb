"""Event-driven sample DAG for `just standalone` with a hosted LaminDB instance.

Only defined if AIRFLOW_CONN_LAMINDB_DEFAULT is exported, as the trigger polls LaminHub. Every table
that lamindb_showcase saves under airflow-showcase/ runs lamindb_showcase_on_table.
"""

from __future__ import annotations

import os

from airflow.sdk import Asset, AssetWatcher, dag, task

from airflow.providers.lamindb.client.enums import LaminDBRegistry
from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger

if "AIRFLOW_CONN_LAMINDB_DEFAULT" in os.environ:
    tables = Asset(
        "lamindb_showcase_tables",
        watchers=[
            AssetWatcher(
                name="lamindb_showcase_tables_watcher",
                trigger=LaminDBArtifactEventTrigger(key_prefix="airflow-showcase/", poll_interval=15),
            )
        ],
    )

    @dag(schedule=[tables], tags=["lamindb"])
    def lamindb_showcase_on_table():
        @task
        def describe(triggering_asset_events=None):
            hook = LaminDBHook()
            for event in triggering_asset_events[tables]:
                artifact = event.extra["payload"]["record"]
                runs = hook.get_records_by_ids(LaminDBRegistry.RUN, [artifact["run_id"]])
                run = runs.get(artifact["run_id"], {})
                print(f"{artifact['key']} ({artifact['uid']}) was saved by {run.get('entrypoint')}")

        describe()

    lamindb_showcase_on_table()
