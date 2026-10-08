"""Example DAGs scheduled on LaminDB changes with event-driven scheduling (asset watchers)."""

from __future__ import annotations

from airflow.sdk import Asset, AssetWatcher, dag, task

from airflow.providers.lamindb.triggers.branches import (
    LaminDBBranchBlockEventTrigger,
    LaminDBBranchStatusEventTrigger,
)
from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger, LaminDBRecordEventTrigger

# [START howto_trigger_lamindb_artifact_events]
new_fastqs = Asset(
    "lamindb_new_fastqs",
    watchers=[
        AssetWatcher(
            name="lamindb_new_fastqs_watcher",
            trigger=LaminDBArtifactEventTrigger(
                lamindb_conn_id="lamindb_default",
                key_prefix="raw/",
                suffix=".fastq.gz",
                poll_interval=30,
            ),
        )
    ],
)


@dag(schedule=[new_fastqs], tags=["example", "lamindb"])
def example_lamindb_artifact_events():
    @task
    def process_new_fastqs(triggering_asset_events=None):
        # several LaminDB events can be batched into one DAG run
        for event in triggering_asset_events[new_fastqs]:
            payload = event.extra["payload"]
            artifact = payload["record"]
            print(f"New artifact {artifact['key']} ({artifact['uid']}) in {payload['instance']}")

    process_new_fastqs()


# [END howto_trigger_lamindb_artifact_events]

example_lamindb_artifact_events()

# [START howto_trigger_lamindb_record_events]
cell_type_changes = Asset(
    "lamindb_cell_type_changes",
    watchers=[
        AssetWatcher(
            name="lamindb_cell_type_changes_watcher",
            trigger=LaminDBRecordEventTrigger(
                "bionty.celltype",
                events=["created", "updated", "deleted"],
                changed_fields=["name", "ontology_id"],
            ),
        )
    ],
)


@dag(schedule=[cell_type_changes], tags=["example", "lamindb"])
def example_lamindb_record_events():
    @task
    def sync_cell_types(triggering_asset_events=None):
        for event in triggering_asset_events[cell_type_changes]:
            payload = event.extra["payload"]
            print(payload["event"], payload["record_id"], payload["changed_fields"], payload["previous"])

    sync_cell_types()


# [END howto_trigger_lamindb_record_events]

example_lamindb_record_events()

# [START howto_trigger_lamindb_branch_events]
review_requested = Asset(
    "lamindb_review_requested",
    watchers=[
        AssetWatcher(
            name="lamindb_review_requested_watcher",
            trigger=LaminDBBranchStatusEventTrigger(to_status="review"),
        )
    ],
)
branch_comments = Asset(
    "lamindb_branch_comments",
    watchers=[
        AssetWatcher(
            name="lamindb_branch_comments_watcher",
            trigger=LaminDBBranchBlockEventTrigger(kinds="comment", branch_name="release-*"),
        )
    ],
)


@dag(schedule=[review_requested], tags=["example", "lamindb"])
def example_lamindb_branch_review():
    @task
    def validate_change_request(triggering_asset_events=None):
        for event in triggering_asset_events[review_requested]:
            payload = event.extra["payload"]
            branch = payload["branch"]
            print(f"Validate branch {branch['name']}: {payload['from_status']} -> {payload['to_status']}")

    validate_change_request()


@dag(schedule=[branch_comments], tags=["example", "lamindb"])
def example_lamindb_branch_comments():
    @task
    def notify(triggering_asset_events=None):
        for event in triggering_asset_events[branch_comments]:
            payload = event.extra["payload"]
            author = (payload["changed_by"] or {}).get("handle")
            print(f"{author} commented on {payload['branch']['name']}: {payload['block']['content']}")

    notify()


# [END howto_trigger_lamindb_branch_events]

example_lamindb_branch_review()
example_lamindb_branch_comments()
