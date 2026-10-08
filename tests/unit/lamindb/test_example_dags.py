from __future__ import annotations

from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

EXAMPLES = Path(__file__).parents[2] / "system" / "lamindb"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(EXAMPLES))


def test_example_dags_import(dagbag):
    assert dagbag.import_errors == {}
    assert sorted(dagbag.dag_ids) == [
        "example_lamindb_artifact_events",
        "example_lamindb_branch_comments",
        "example_lamindb_branch_review",
        "example_lamindb_record_events",
        "example_lamindb_sensors",
        "lamindb_example_auto_flow",
        "lamindb_example_fail",
        "lamindb_example_ok",
    ]


def test_example_dags_serialize(dagbag):
    for dag in dagbag.dags.values():
        serialized = DagSerialization.to_dict(dag)
        assert serialized["dag"]["dag_id"] == dag.dag_id


def test_watchers_use_lamindb_triggers(dagbag):
    serialized = str(DagSerialization.to_dict(dagbag.dags["example_lamindb_artifact_events"]))
    assert "airflow.providers.lamindb.triggers.records.LaminDBArtifactEventTrigger" in serialized
    assert "lamindb_new_fastqs_watcher" in serialized
