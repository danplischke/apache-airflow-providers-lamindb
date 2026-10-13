"""Example DAGs scheduled on LaminDB changes with event-driven scheduling (asset watchers)."""

from __future__ import annotations

from airflow.sdk import Asset, AssetWatcher, Param, dag, get_current_context, task

from airflow.providers.lamindb.triggers.branches import (
    LaminDBBranchBlockEventTrigger,
    LaminDBBranchStatusEventTrigger,
)
from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger, LaminDBRecordEventTrigger
from airflow.providers.lamindb.triggers.replay import request_replay

# --8<-- [start:howto_trigger_lamindb_artifact_events]
new_fastqs_trigger = LaminDBArtifactEventTrigger(
    lamindb_conn_id="lamindb_default",
    key_prefix="raw/",
    suffix=".fastq.gz",
    poll_interval=30,
)
new_fastqs = Asset(
    "lamindb_new_fastqs",
    watchers=[AssetWatcher(name="lamindb_new_fastqs_watcher", trigger=new_fastqs_trigger)],
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


# --8<-- [end:howto_trigger_lamindb_artifact_events]

example_lamindb_artifact_events()


# --8<-- [start:howto_process_lamindb_events_one_by_one]
@dag(schedule=[new_fastqs], tags=["example", "lamindb"])
def example_lamindb_fastqs_one_by_one():
    @task
    def fastq_events(triggering_asset_events=None):
        return [event.extra["payload"] for event in triggering_asset_events[new_fastqs]]

    # one task instance per event, labeled with its database write
    @task(retries=3, map_index_template="write {{ dbwrite_id }}")
    def process_fastq(payload):
        get_current_context()["dbwrite_id"] = payload["dbwrite"]["id"]
        if payload.get("replay"):
            print(f"Replayed for request {payload['replay']['id']}")
        artifact = payload["record"]
        print(f"Processing {artifact['key']} ({artifact['uid']})")

    process_fastq.expand(payload=fastq_events())


# --8<-- [end:howto_process_lamindb_events_one_by_one]

example_lamindb_fastqs_one_by_one()


# --8<-- [start:howto_replay_lamindb_events]
@dag(
    schedule=None,
    params={
        "from_dbwrite_id": Param(1, type="integer", minimum=1),
        "to_dbwrite_id": Param(None, type=["null", "integer"], minimum=1),
    },
    tags=["example", "lamindb"],
)
def example_lamindb_replay_fastqs():
    # the watched asset as inlet gives the task its asset state store
    @task(inlets=[new_fastqs])
    def request_fastq_replay(params=None, run_id=None, asset_state_store=None):
        request_replay(
            asset_state_store,
            new_fastqs_trigger,
            from_dbwrite_id=params["from_dbwrite_id"],
            to_dbwrite_id=params["to_dbwrite_id"],
            request_id=run_id,
        )

    request_fastq_replay()


# --8<-- [end:howto_replay_lamindb_events]

example_lamindb_replay_fastqs()

# --8<-- [start:howto_trigger_lamindb_record_events]
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


# --8<-- [end:howto_trigger_lamindb_record_events]

example_lamindb_record_events()

# --8<-- [start:howto_trigger_lamindb_branch_events]
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


# --8<-- [end:howto_trigger_lamindb_branch_events]

example_lamindb_branch_review()
example_lamindb_branch_comments()
