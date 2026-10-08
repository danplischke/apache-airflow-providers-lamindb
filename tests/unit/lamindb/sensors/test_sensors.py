from __future__ import annotations

from typing import Any
from unittest import mock

import pytest
from airflow.sdk.exceptions import AirflowException, AirflowFailException, TaskDeferred

from airflow.providers.lamindb.hooks.lamindb import LaminDBHook
from airflow.providers.lamindb.sensors.branches import LaminDBBranchStatusSensor
from airflow.providers.lamindb.sensors.records import LaminDBArtifactSensor, LaminDBRecordSensor
from airflow.providers.lamindb.triggers.branches import LaminDBBranchStatusSensorTrigger
from airflow.providers.lamindb.triggers.records import LaminDBRecordSensorTrigger
from airflow.providers.lamindb.utils.filters import (
    ArtifactField,
    ArtifactKind,
    F,
    LaminDBRegistry,
    RunField,
    RunStatus,
    TransformField,
)


def branch(status_code: int) -> dict[str, Any]:
    return {"id": 10, "uid": "b10", "name": "release-1", "_status_code": status_code, "created_by_id": 7}


@pytest.fixture
def get_branch():
    with mock.patch.object(LaminDBHook, "get_branch") as patched:
        yield patched


@pytest.fixture
def query_records():
    with mock.patch.object(LaminDBHook, "query_records") as patched:
        yield patched


class TestBranchStatusSensor:
    def make(self, **kwargs: Any) -> LaminDBBranchStatusSensor:
        return LaminDBBranchStatusSensor(task_id="wait", branch="release-1", poke_interval=0, **kwargs)

    def test_poke(self, get_branch):
        sensor = self.make(instance="owner/name")
        get_branch.return_value = None
        assert not sensor.poke({})
        get_branch.return_value = branch(2)
        assert not sensor.poke({})
        get_branch.return_value = branch(-1)
        result = sensor.poke({})
        assert result.is_done
        assert result.xcom_value["status"] == "merged"
        get_branch.assert_called_with("release-1")
        get_branch.return_value = branch(-2)
        with pytest.raises(AirflowFailException, match="'closed'"):
            sensor.poke({})

    def test_execute(self, get_branch):
        get_branch.side_effect = [branch(1), branch(2)]
        result = self.make(target_status=["review"]).execute({})
        assert result["status"] == "review"

    def test_execute_deferrable(self, get_branch):
        get_branch.return_value = branch(2)
        sensor = self.make(deferrable=True, instance="owner/name", timeout=600)
        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute({})
        trigger = deferred.value.trigger
        assert isinstance(trigger, LaminDBBranchStatusSensorTrigger)
        assert (trigger.branch, trigger.target_status, trigger.failure_status, trigger.instance) == (
            "release-1",
            ["merged"],
            ["closed"],
            "owner/name",
        )
        assert deferred.value.method_name == "execute_complete"
        assert deferred.value.timeout.total_seconds() == 600

    def test_execute_deferrable_done_immediately(self, get_branch):
        get_branch.return_value = branch(-1)
        assert self.make(deferrable=True).execute({})["status"] == "merged"

    def test_execute_complete(self):
        sensor = self.make()
        assert sensor.execute_complete({}, {"status": "success", "branch": {"id": 10}}) == {"id": 10}
        with pytest.raises(AirflowFailException, match="closed"):
            sensor.execute_complete({}, {"status": "failure", "message": "closed"})
        with pytest.raises(AirflowException, match="boom"):
            sensor.execute_complete({}, {"status": "error", "message": "boom"})

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"target_status": "approved"}, "Invalid branch status"),
            ({"target_status": []}, "must not be empty"),
            ({"target_status": "closed"}, "both target and failure"),
        ],
    )
    def test_validation(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            self.make(**kwargs)


class TestRecordSensor:
    def test_poke(self, query_records):
        sensor = LaminDBRecordSensor(
            task_id="wait", registry="core.run", filter={"name": {"eq": "x"}}, branch="feature", min_count=2
        )
        query_records.return_value = [{"id": 2}]
        assert not sensor.poke({})
        query_records.return_value = [{"id": 3}, {"id": 2}]
        assert sensor.poke({}).xcom_value == [{"id": 3}, {"id": 2}]
        query_records.assert_called_with(
            "core.run",
            {"and": [{"name": {"eq": "x"}}, {"branch.name": {"eq": "feature"}}]},
            order_by=["-id"],
            limit=100,
        )

    def test_waits_for_upload_of_artifacts(self, query_records):
        query_records.return_value = [{"id": 2, "_aux": {"so": 1}}, {"id": 1, "_aux": None}]
        sensor = LaminDBRecordSensor(task_id="wait", registry="core.artifact", branch=None)
        assert sensor.poke({}).xcom_value == [{"id": 1, "_aux": None}]
        query_records.assert_called_with("core.artifact", None, order_by=["-id"], limit=100)
        sensor = LaminDBRecordSensor(task_id="wait", registry="core.artifact", wait_for_upload=False)
        assert len(sensor.poke({}).xcom_value) == 2

    def test_execute_deferrable(self, query_records):
        query_records.return_value = []
        sensor = LaminDBRecordSensor(
            task_id="wait",
            registry="core.run",
            filter={"name": {"eq": "x"}},
            deferrable=True,
            poke_interval=5,
        )
        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute({})
        trigger = deferred.value.trigger
        assert isinstance(trigger, LaminDBRecordSensorTrigger)
        assert trigger.filter == {"and": [{"name": {"eq": "x"}}, {"branch_id": {"eq": 1}}]}
        assert trigger.branch is None
        assert trigger.poll_interval == 5
        assert not trigger.wait_for_upload

    def test_execute_complete(self):
        sensor = LaminDBRecordSensor(task_id="wait", registry="core.run")
        assert sensor.execute_complete({}, {"status": "success", "records": [{"id": 1}]}) == [{"id": 1}]
        with pytest.raises(AirflowException, match="nope"):
            sensor.execute_complete({}, {"status": "error", "message": "nope"})

    def test_validation(self):
        with pytest.raises(ValueError, match="min_count"):
            LaminDBRecordSensor(task_id="wait", registry="core.run", min_count=5, limit=2)
        with pytest.raises(ValueError, match="Unknown filter operator 'equals' on 'name'"):
            LaminDBRecordSensor(task_id="wait", registry="core.run", filter={"name": {"equals": "x"}})

    def test_filter_builder_and_enums(self, query_records):
        query_records.return_value = [{"id": 1}]
        sensor = LaminDBRecordSensor(
            task_id="wait",
            registry=LaminDBRegistry.RUN,
            filter=(F(RunField.TRANSFORM, TransformField.KEY) == "preprocess.py")
            & (F(RunField.STATUS_CODE) == RunStatus.COMPLETED),
            branch=None,
        )
        assert type(sensor.registry) is str
        assert sensor.filter == {
            "and": [{"transform.key": {"eq": "preprocess.py"}}, {"_status_code": {"eq": 0}}]
        }
        assert sensor.poke({}).is_done
        query_records.assert_called_with("core.run", sensor.filter, order_by=["-id"], limit=100)

    def test_templated_filter(self):
        sensor = LaminDBRecordSensor(
            task_id="wait", registry="core.run", filter=F(RunField.NAME) == "{{ params.name }}"
        )
        sensor.render_template_fields({"params": {"name": "nightly"}})
        assert sensor.filter == {"name": {"eq": "nightly"}}


class TestArtifactSensor:
    def test_query(self, query_records):
        query_records.return_value = [{"id": 1, "_aux": None}]
        sensor = LaminDBArtifactSensor(task_id="wait", key="raw/a.csv", include_internal=True)
        assert sensor.poke({}).xcom_value == [{"id": 1, "_aux": None}]
        query_records.assert_called_with(
            "core.artifact",
            {"and": [{"key": {"eq": "raw/a.csv"}}, {"branch_id": {"eq": 1}}]},
            order_by=["-id"],
            limit=100,
        )

    def test_kind_enum_and_filter_builder(self, query_records):
        query_records.return_value = []
        sensor = LaminDBArtifactSensor(
            task_id="wait", kind=ArtifactKind.DATASET, filter=F(ArtifactField.SIZE) > 0, branch=None
        )
        assert sensor.kind == ["dataset"]
        assert type(sensor.kind[0]) is str
        assert not sensor.poke({}).is_done
        query_records.assert_called_with(
            "core.artifact",
            {"and": [{"size": {"gt": 0}}, {"kind": {"in": ["dataset"]}}]},
            order_by=["-id"],
            limit=100,
        )

    def test_execute_deferrable(self, query_records):
        query_records.return_value = [{"id": 1, "_aux": {"so": 1}}]
        sensor = LaminDBArtifactSensor(
            task_id="wait", key_prefix="raw/", suffix=".csv", branch=None, deferrable=True
        )
        with pytest.raises(TaskDeferred) as deferred:
            sensor.execute({})
        trigger = deferred.value.trigger
        assert trigger.registry == "core.artifact"
        assert trigger.wait_for_upload
        assert trigger.filter["and"][:2] == [{"key": {"startswith": "raw/"}}, {"suffix": {"in": [".csv"]}}]
