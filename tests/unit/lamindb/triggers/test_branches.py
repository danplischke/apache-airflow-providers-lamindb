from __future__ import annotations

from typing import Any

import pytest

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers.branches import (
    LaminDBBranchBlockEventTrigger,
    LaminDBBranchStatusEventTrigger,
    LaminDBBranchStatusSensorTrigger,
    branch_summary,
    evaluate_branch_status,
    matches_branch_name,
)
from unit.lamindb.fakes import INSTANCE, FakeHook, dbwrite, evaluate

BRANCH = "lamindb_branch"
BLOCK = "lamindb_branchblock"


def branch(id: int, name: str, status_code: int) -> dict[str, Any]:
    return {
        "id": id,
        "uid": f"b{id}",
        "name": name,
        "description": None,
        "_status_code": status_code,
        "created_by_id": 7,
        "created_at": "2026-09-01T00:00:00+00:00",
        "space_id": 1,
    }


def status_write(id: int, branch_id: int, previous_code: int) -> dict[str, Any]:
    return dbwrite(id, "UPDATE", BRANCH, branch_id, data={"_status_code": previous_code}, branch_id=None)


async def events_for(trigger, hook: FakeHook, *, up_to: int | None = None) -> list[dict[str, Any]]:
    await trigger._setup(hook)
    selected = [
        w
        for w in hook.writes
        if evaluate(trigger._dbwrite_filter(), w) and (up_to is None or w["id"] <= up_to)
    ]
    return [event.payload for event in await trigger._events_for(hook, INSTANCE, selected)]


class TestBranchStatusEventTrigger:
    @pytest.fixture
    def hook(self) -> FakeHook:
        writes = [
            status_write(1, 10, 0),  # standalone -> draft
            dbwrite(2, "UPDATE", BRANCH, 10, data={"name": "old-name"}, branch_id=None),
            status_write(3, 10, 1),  # draft -> review
            status_write(4, 11, 2),  # review -> closed (current status)
            status_write(5, 10, 2),  # review -> merged (after the polled batch)
        ]
        records = {"core.branch": {10: branch(10, "release-1", -1), 11: branch(11, "fix-typo", -2)}}
        return FakeHook(writes=writes, records=records)

    async def test_transitions(self, hook):
        events = await events_for(LaminDBBranchStatusEventTrigger(), hook, up_to=4)
        assert [(e["branch"]["name"], e["from_status"], e["to_status"]) for e in events] == [
            ("release-1", "standalone", "draft"),
            ("release-1", "draft", "review"),
            ("fix-typo", "review", "closed"),
        ]
        assert events[0] == {
            "instance": "owner/name",
            "instance_id": INSTANCE.id,
            "event": "status_changed",
            "branch": branch_summary(branch(10, "release-1", -1)),
            "from_status": "standalone",
            "to_status": "draft",
            "changed_by": {"id": 7, "handle": "alice", "name": "Alice"},
            "dbwrite": events[0]["dbwrite"],
        }
        assert events[0]["branch"]["status"] == "merged"

    async def test_last_write_uses_current_status(self, hook):
        events = await events_for(LaminDBBranchStatusEventTrigger(), hook)
        assert [(e["dbwrite"]["id"], e["to_status"]) for e in events][-1] == (5, "merged")

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"to_status": "merged"}, [5]),
            ({"to_status": ["review", "closed"]}, [3, 4]),
            ({"from_status": "review"}, [4, 5]),
            ({"from_status": "review", "to_status": "merged"}, [5]),
            ({"branch_name": "release-*"}, [1, 3, 5]),
            ({"branch_name": ["fix-*", "hotfix-*"]}, [4]),
        ],
    )
    async def test_filters(self, hook, kwargs, expected):
        events = await events_for(LaminDBBranchStatusEventTrigger(**kwargs), hook)
        assert [e["dbwrite"]["id"] for e in events] == expected

    async def test_next_status_change_far_after_the_batch(self):
        """Status writes of other branches between a branch's changes do not hide its next change."""
        churn = [status_write(i, 11, i % 2) for i in range(3, 1503)]  # branch 11: standalone <-> draft
        writes = [
            status_write(1, 10, 1),  # draft -> review
            status_write(2, 11, 0),
            *churn,
            status_write(1503, 10, 2),  # review -> merged (current status)
        ]
        records = {"core.branch": {10: branch(10, "a", -1), 11: branch(11, "b", 0)}}
        hook = FakeHook(writes=writes, records=records)

        events = await events_for(LaminDBBranchStatusEventTrigger(branch_name="a"), hook, up_to=2)

        assert [(e["from_status"], e["to_status"]) for e in events] == [("draft", "review")]
        # each query only asks for branches still unresolved, so it takes one query per branch at most
        assert len(hook.dbwrite_queries) == 2

    async def test_skips_deleted_branches_and_noops(self):
        hook = FakeHook(
            writes=[status_write(1, 10, 1), status_write(2, 11, 2)],
            records={"core.branch": {11: branch(11, "b", 2)}},
        )
        assert await events_for(LaminDBBranchStatusEventTrigger(), hook) == []

    def test_validation_and_serialize(self):
        with pytest.raises(ValueError, match="Invalid branch status"):
            LaminDBBranchStatusEventTrigger(to_status="approved")
        trigger = LaminDBBranchStatusEventTrigger(to_status="merged", branch_name="release-*")
        classpath, kwargs = trigger.serialize()
        assert kwargs["to_status"] == ["merged"]
        assert kwargs["branch_name"] == ["release-*"]
        assert LaminDBBranchStatusEventTrigger(**kwargs).serialize() == (classpath, kwargs)


class TestBranchBlockEventTrigger:
    @pytest.fixture
    def hook(self) -> FakeHook:
        writes = [
            dbwrite(1, "INSERT", BLOCK, 100, branch_id=10),
            dbwrite(2, "INSERT", BLOCK, 101, branch_id=11),
            dbwrite(3, "INSERT", BLOCK, 102, branch_id=10),
            dbwrite(4, "UPDATE", BLOCK, 100, data={"content": "x"}, branch_id=10),
        ]
        records = {
            "core.branchblock": {
                100: {"id": 100, "kind": "comment", "content": "LGTM", "branch_id": 10},
                101: {"id": 101, "kind": "comment", "content": "typo", "branch_id": 11},
                102: {"id": 102, "kind": "readme", "content": "# Release", "branch_id": 10},
            },
            "core.branch": {10: branch(10, "release-1", 2), 11: branch(11, "fix-typo", 1)},
        }
        return FakeHook(writes=writes, records=records)

    async def test_blocks(self, hook):
        events = await events_for(LaminDBBranchBlockEventTrigger(), hook)
        assert [(e["kind"], e["block"]["content"], e["branch"]["name"]) for e in events] == [
            ("comment", "LGTM", "release-1"),
            ("comment", "typo", "fix-typo"),
            ("readme", "# Release", "release-1"),
        ]
        assert events[0]["event"] == "block_created"
        assert events[0]["changed_by"]["handle"] == "alice"

    async def test_filters(self, hook):
        events = await events_for(
            LaminDBBranchBlockEventTrigger(kinds="comment", branch_name="release-*"), hook
        )
        assert [e["block"]["id"] for e in events] == [100]

    def test_validation_and_serialize(self):
        with pytest.raises(ValueError, match="Invalid block kinds"):
            LaminDBBranchBlockEventTrigger(kinds=["review"])
        classpath, kwargs = LaminDBBranchBlockEventTrigger(kinds="readme").serialize()
        assert kwargs["kinds"] == ["readme"]
        assert LaminDBBranchBlockEventTrigger(**kwargs).serialize() == (classpath, kwargs)


class TestBranchStatusSensorTrigger:
    @pytest.fixture
    def trigger(self, monkeypatch):
        def make(hook: FakeHook, **kwargs: Any) -> LaminDBBranchStatusSensorTrigger:
            trigger = LaminDBBranchStatusSensorTrigger(
                **{"branch": "release-1", "poll_interval": 0, **kwargs}
            )
            monkeypatch.setattr(trigger, "_get_hook", lambda: hook)
            return trigger

        return make

    async def test_waits_for_target_status(self, trigger, monkeypatch):
        hook = FakeHook(records={"core.branch": {10: branch(10, "release-1", 2)}})

        async def merge(delay):
            hook.records["core.branch"][10] = branch(10, "release-1", -1)

        monkeypatch.setattr("airflow.providers.lamindb.triggers.branches.asyncio.sleep", merge)
        events = [e.payload async for e in trigger(hook).run()]
        assert events == [
            {
                "status": "success",
                "branch": branch_summary(branch(10, "release-1", -1)),
                "message": "Branch 'release-1' has status 'merged'",
            }
        ]
        assert hook.closed

    async def test_failure_status(self, trigger):
        hook = FakeHook(records={"core.branch": {10: branch(10, "release-1", -2)}})
        (event,) = [e.payload async for e in trigger(hook).run()]
        assert event["status"] == "failure"

    async def test_error(self, trigger):
        hook = FakeHook()
        hook.fail_next = [LaminDBApiError("boom")]
        events = [e.payload async for e in trigger(hook).run()]
        assert events == [{"status": "error", "message": "boom"}]

    def test_serialize(self):
        trigger = LaminDBBranchStatusSensorTrigger(5, target_status=["review"], failure_status=[])
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.lamindb.triggers.branches.LaminDBBranchStatusSensorTrigger"
        assert LaminDBBranchStatusSensorTrigger(**kwargs).serialize() == (classpath, kwargs)


def test_helpers():
    assert branch_summary(None) is None
    assert matches_branch_name(None, None)
    assert not matches_branch_name(None, ["x"])
    assert matches_branch_name({"name": "release-1"}, ["release-*"])
    assert not matches_branch_name({"name": "Release-1"}, ["release-*"])
    assert evaluate_branch_status(None, ["merged"], ["closed"]) is None
    assert evaluate_branch_status({"_status_code": -1}, ["merged"], ["closed"]) == "success"
    assert evaluate_branch_status({"_status_code": -2}, ["merged"], ["closed"]) == "failure"
    assert evaluate_branch_status({"_status_code": 2}, ["merged"], ["closed"]) is None
