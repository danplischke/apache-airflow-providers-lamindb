from __future__ import annotations

from typing import Any

import pytest

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers.records import (
    LaminDBArtifactEventTrigger,
    LaminDBRecordEventTrigger,
    LaminDBRecordSensorTrigger,
    branch_scope_filter,
    ready_records,
)
from airflow.providers.lamindb.utils.filters import (
    ArtifactField,
    ArtifactKind,
    F,
    LaminDBRegistry,
    RunField,
    ULabelField,
    UserField,
)
from unit.lamindb.fakes import INSTANCE, FakeHook, dbwrite, evaluate

MAIN, TRASH, FEATURE = 1, -1, 42
ULABEL = "lamindb_ulabel"
ARTIFACT = "lamindb_artifact"
BRANCHES = {
    1: {"id": 1, "name": "main"},
    FEATURE: {"id": FEATURE, "name": "feature"},
}


def ulabel(id: int, branch_id: int = MAIN, name: str = "label") -> dict[str, Any]:
    return {"id": id, "uid": f"u{id}", "name": name, "branch_id": branch_id}


def artifact(
    id: int, *, key: str = "raw/a.csv", kind: str | None = "dataset", aux: Any = None, branch_id: int = MAIN
) -> dict[str, Any]:
    return {
        "id": id,
        "uid": f"a{id}",
        "key": key,
        "suffix": "." + key.rsplit(".", 1)[-1],
        "kind": kind,
        "_aux": aux,
        "branch_id": branch_id,
    }


async def events_for(trigger, hook: FakeHook) -> list[dict[str, Any]]:
    await trigger._setup(hook)
    selected = [w for w in hook.writes if evaluate(trigger._dbwrite_filter(), w)]
    events = await trigger._events_for(hook, INSTANCE, selected)
    return [event.payload for event in events]


def summary(events: list[dict[str, Any]]) -> list[tuple[str, int, int]]:
    return [(e["event"], e["record_id"], e["dbwrite"]["id"]) for e in events]


class TestRecordEventTrigger:
    async def test_created_on_main(self):
        writes = [
            dbwrite(1, "INSERT", ULABEL, 1),
            dbwrite(2, "INSERT", ULABEL, 2, branch_id=FEATURE),
            dbwrite(3, "UPDATE", ULABEL, 3, data={"branch_id": FEATURE}),  # merged into main
            dbwrite(4, "UPDATE", ULABEL, 1, data={"name": "old"}),
            dbwrite(5, "INSERT", "lamindb_other", 5),
        ]
        records = {"core.ulabel": {1: ulabel(1), 2: ulabel(2, FEATURE), 3: ulabel(3)}}
        hook = FakeHook(writes=writes, records=records)
        events = await events_for(LaminDBRecordEventTrigger("core.ulabel"), hook)

        assert summary(events) == [("created", 1, 1), ("created", 3, 3)]
        assert events[0] == {
            "instance": "owner/name",
            "instance_id": INSTANCE.id,
            "event": "created",
            "registry": "core.ulabel",
            "table_name": ULABEL,
            "record_id": 1,
            "record": ulabel(1),
            "previous": None,
            "changed_fields": [],
            "changed_by": {"id": 7, "handle": "alice", "name": "Alice"},
            "dbwrite": events[0]["dbwrite"],
        }
        assert events[1]["previous"] == {"branch_id": FEATURE}

    async def test_all_events(self):
        writes = [
            dbwrite(1, "INSERT", ULABEL, 1),
            dbwrite(2, "UPDATE", ULABEL, 1, data={"name": "old"}),
            dbwrite(3, "UPDATE", ULABEL, 2, data={"branch_id": MAIN}, branch_id=TRASH),  # .delete()
            dbwrite(4, "DELETE", ULABEL, 3, data={"id": 3, "name": "x", "branch_id": MAIN}),
            dbwrite(5, "DELETE", ULABEL, 2, data={"id": 2, "name": "y", "branch_id": TRASH}, branch_id=TRASH),
            dbwrite(6, "UPDATE", ULABEL, 4, data={"name": "old"}, branch_id=FEATURE),
        ]
        records = {"core.ulabel": {1: ulabel(1), 2: ulabel(2, TRASH), 4: ulabel(4, FEATURE)}}
        hook = FakeHook(writes=writes, records=records)
        trigger = LaminDBRecordEventTrigger("core.ulabel", events=["created", "updated", "deleted"])
        events = await events_for(trigger, hook)

        assert summary(events) == [("created", 1, 1), ("updated", 1, 2), ("deleted", 2, 3), ("deleted", 3, 4)]
        assert events[1]["changed_fields"] == ["name"]
        assert events[1]["previous"] == {"name": "old"}
        assert events[2]["record"] == ulabel(2, TRASH)
        assert events[3]["record"] is None
        assert events[3]["previous"] == {"id": 3, "name": "x", "branch_id": MAIN}

    async def test_all_branches(self):
        writes = [
            dbwrite(1, "INSERT", ULABEL, 1, branch_id=FEATURE),
            dbwrite(2, "UPDATE", ULABEL, 1, data={"branch_id": FEATURE}),
            dbwrite(3, "DELETE", ULABEL, 1, data={"id": 1, "branch_id": MAIN}),
        ]
        hook = FakeHook(writes=writes, records={"core.ulabel": {1: ulabel(1)}})
        trigger = LaminDBRecordEventTrigger(
            "core.ulabel", events=["created", "updated", "deleted"], branch=None
        )
        assert summary(await events_for(trigger, hook)) == [
            ("created", 1, 1),
            ("updated", 1, 2),
            ("deleted", 1, 3),
        ]

    @pytest.mark.parametrize(
        "filter", [{"name": {"eq": "keep"}}, F(ULabelField.NAME) == "keep"], ids=["dict", "builder"]
    )
    async def test_filter(self, filter):
        writes = [
            dbwrite(1, "INSERT", ULABEL, 1),
            dbwrite(2, "INSERT", ULABEL, 2),
            dbwrite(3, "DELETE", ULABEL, 3, data={"id": 3, "name": "keep", "branch_id": MAIN}),
            dbwrite(4, "DELETE", ULABEL, 4, data={"id": 4, "name": "drop", "branch_id": MAIN}),
        ]
        records = {"core.ulabel": {1: ulabel(1, name="keep"), 2: ulabel(2, name="drop")}}
        hook = FakeHook(writes=writes, records=records)
        trigger = LaminDBRecordEventTrigger("core.ulabel", events=["created", "deleted"], filter=filter)
        assert summary(await events_for(trigger, hook)) == [("created", 1, 1), ("deleted", 3, 3)]

    async def test_deleted_record_without_local_fields(self):
        writes = [dbwrite(1, "DELETE", ULABEL, 1, data={"id": 1, "branch_id": MAIN})]
        hook = FakeHook(writes=writes)
        trigger = LaminDBRecordEventTrigger("core.ulabel", events="deleted", filter={"name": {"eq": "x"}})
        assert await events_for(trigger, hook) == []

    async def test_changed_fields(self):
        writes = [
            dbwrite(1, "UPDATE", ULABEL, 1, data={"description": "old"}),
            dbwrite(2, "UPDATE", ULABEL, 1, data={"name": "old", "description": "x"}),
        ]
        hook = FakeHook(writes=writes, records={"core.ulabel": {1: ulabel(1)}})
        trigger = LaminDBRecordEventTrigger("core.ulabel", events="updated", changed_fields=["name"])
        assert summary(await events_for(trigger, hook)) == [("updated", 1, 2)]

    async def test_skips_records_gone_since(self):
        hook = FakeHook(writes=[dbwrite(1, "INSERT", ULABEL, 1)])
        assert await events_for(LaminDBRecordEventTrigger("core.ulabel"), hook) == []

    async def test_custom_branch(self):
        writes = [dbwrite(1, "INSERT", ULABEL, 1, branch_id=FEATURE), dbwrite(2, "INSERT", ULABEL, 2)]
        records = {"core.ulabel": {1: ulabel(1, FEATURE), 2: ulabel(2)}, "core.branch": BRANCHES}
        hook = FakeHook(writes=writes, records=records)
        trigger = LaminDBRecordEventTrigger("core.ulabel", branch="feature")
        assert summary(await events_for(trigger, hook)) == [("created", 1, 1)]
        assert trigger._branch_id == FEATURE

    async def test_missing_branch(self):
        trigger = LaminDBRecordEventTrigger("core.ulabel", branch="nope")
        with pytest.raises(ValueError, match="Branch 'nope' does not exist"):
            await trigger._setup(FakeHook())

    async def test_registry_without_branches(self):
        writes = [dbwrite(1, "INSERT", "lamindb_user", 7, branch_id=None)]
        hook = FakeHook(writes=writes, records={"core.user": {7: {"id": 7, "handle": "alice"}}})
        trigger = LaminDBRecordEventTrigger("core.user")
        assert summary(await events_for(trigger, hook)) == [("created", 7, 1)]
        assert trigger._branch_id is None

    async def test_dbwrite_filter(self):
        trigger = LaminDBRecordEventTrigger("core.ulabel")
        await trigger._setup(FakeHook())
        accepted = [
            dbwrite(1, "INSERT", ULABEL, 1),
            dbwrite(2, "UPDATE", ULABEL, 1, data={"branch_id": FEATURE}),
        ]
        rejected = [
            dbwrite(3, "INSERT", ULABEL, 1, branch_id=FEATURE),
            dbwrite(4, "UPDATE", ULABEL, 1, data={"name": "x"}),
            dbwrite(5, "DELETE", ULABEL, 1, data={"branch_id": MAIN}),
            dbwrite(6, "INSERT", "lamindb_artifact", 1),
        ]
        dbwrite_filter = trigger._dbwrite_filter()
        assert [evaluate(dbwrite_filter, w) for w in accepted] == [True, True]
        assert [evaluate(dbwrite_filter, w) for w in rejected] == [False] * 4

        trigger = LaminDBRecordEventTrigger("core.ulabel", events="deleted")
        await trigger._setup(FakeHook())
        dbwrite_filter = trigger._dbwrite_filter()
        assert evaluate(dbwrite_filter, dbwrite(1, "DELETE", ULABEL, 1, data={"branch_id": MAIN}))
        assert evaluate(
            dbwrite_filter, dbwrite(2, "UPDATE", ULABEL, 1, data={"branch_id": MAIN}, branch_id=-1)
        )
        assert not evaluate(dbwrite_filter, dbwrite(3, "INSERT", ULABEL, 1))

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"events": ["removed"]}, "Invalid events"),
            ({"events": []}, "Invalid events"),
            ({"events": "deleted", "filter": {"created_by.handle": {"eq": "x"}}}, "direct fields"),
            (
                {"events": "deleted", "filter": F(ULabelField.CREATED_BY, UserField.HANDLE) == "x"},
                "direct fields",
            ),
            ({"filter": {"name": {"equals": "x"}}}, "Unknown filter operator 'equals'"),
        ],
    )
    def test_validation(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            LaminDBRecordEventTrigger("core.ulabel", **kwargs)

    def test_filter_builder_and_enums_serialize_like_plain_values(self):
        trigger = LaminDBRecordEventTrigger(
            LaminDBRegistry.ULABEL,
            events="updated",
            filter=F(ULabelField.NAME).startswith("a") & (F(ULabelField.CREATED_BY_ID) == 7),
            changed_fields=[ULabelField.NAME, "description"],
        )
        plain = LaminDBRecordEventTrigger(
            "core.ulabel",
            events="updated",
            filter={"and": [{"name": {"startswith": "a"}}, {"created_by_id": {"eq": 7}}]},
            changed_fields=["name", "description"],
        )
        assert trigger.serialize() == plain.serialize()
        assert trigger.state_key == plain.state_key
        _, kwargs = trigger.serialize()
        assert type(kwargs["registry"]) is str
        assert [type(field) for field in kwargs["changed_fields"]] == [str, str]

    def test_serialize(self):
        trigger = LaminDBRecordEventTrigger(
            "bionty.gene",
            events=["deleted", "created"],
            branch=None,
            filter={"symbol": {"eq": "TP53"}},
            changed_fields=["symbol"],
            instance="a/b",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.lamindb.triggers.records.LaminDBRecordEventTrigger"
        assert kwargs["events"] == ["created", "deleted"]
        assert kwargs["registry"] == "bionty.gene"
        assert LaminDBRecordEventTrigger(**kwargs).serialize() == (classpath, kwargs)


class TestArtifactEventTrigger:
    async def test_created_after_upload(self):
        writes = [
            dbwrite(1, "INSERT", ARTIFACT, 1),  # upload still in progress
            dbwrite(2, "INSERT", ARTIFACT, 2),  # uploaded already when polled, completion follows
            dbwrite(3, "UPDATE", ARTIFACT, 2, data={"_aux": '{"so": 1}'}),
            dbwrite(4, "INSERT", ARTIFACT, 3),  # nothing to upload
        ]
        records = {"core.artifact": {1: artifact(1, aux={"so": 1}), 2: artifact(2), 3: artifact(3)}}
        hook = FakeHook(writes=writes, records=records)
        events = await events_for(LaminDBArtifactEventTrigger(), hook)
        assert summary(events) == [("created", 2, 3), ("created", 3, 4)]

        # the upload of artifact 1 completes
        hook.writes.append(dbwrite(5, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}'}))
        records["core.artifact"][1] = artifact(1)
        trigger = LaminDBArtifactEventTrigger()
        await trigger._setup(hook)
        events = await trigger._events_for(hook, INSTANCE, [hook.writes[-1]])
        assert summary([e.payload for e in events]) == [("created", 1, 5)]

    async def test_reupload_is_an_update(self):
        writes = [
            dbwrite(1, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}'}),
            dbwrite(2, "UPDATE", ARTIFACT, 1, data={"hash": "old", "_aux": "null"}),  # replace() starts
            dbwrite(3, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}'}),  # replace() completes
        ]
        hook = FakeHook(writes=writes, records={"core.artifact": {1: artifact(1)}})
        trigger = LaminDBArtifactEventTrigger(events=["created", "updated"])
        assert summary(await events_for(trigger, hook)) == [
            ("created", 1, 1),
            ("updated", 1, 2),
            ("updated", 1, 3),
        ]

    async def test_long_aux_history_does_not_hide_other_completions(self):
        # artifact 1 was replaced many times: each replace() clears _aux, its completion sets it again
        history = [
            dbwrite(i, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}' if i % 2 else "null"})
            for i in range(1, 26)
        ]
        writes = [
            *history,
            dbwrite(26, "INSERT", ARTIFACT, 2),  # uploaded already when polled, completion follows
            dbwrite(27, "UPDATE", ARTIFACT, 2, data={"_aux": '{"so": 1}'}),
        ]
        hook = FakeHook(writes=writes, records={"core.artifact": {1: artifact(1), 2: artifact(2)}})

        events = await events_for(LaminDBArtifactEventTrigger(events=["created"]), hook)

        assert [e for e in summary(events) if e[1] == 2] == [("created", 2, 27)]

    async def test_upload_completions_of_many_artifacts(self):
        count = 250  # more than one id chunk
        writes = [dbwrite(i, "INSERT", ARTIFACT, i) for i in range(1, count + 1)]
        writes += [
            dbwrite(count + i, "UPDATE", ARTIFACT, i, data={"_aux": '{"so": 1}'}) for i in range(1, count + 1)
        ]
        records = {"core.artifact": {i: artifact(i) for i in range(1, count + 1)}}
        hook = FakeHook(writes=writes, records=records)

        events = await events_for(LaminDBArtifactEventTrigger(events=["created"]), hook)

        assert summary(events) == [("created", i, count + i) for i in range(1, count + 1)]

    async def test_skips_updates_during_upload_and_failed_uploads(self):
        writes = [
            dbwrite(1, "UPDATE", ARTIFACT, 1, data={"hash": "old"}),
            dbwrite(
                2,
                "DELETE",
                ARTIFACT,
                2,
                data={"id": 2, "kind": "dataset", "_aux": '{"so": 1}', "branch_id": 1},
            ),
            dbwrite(
                3, "DELETE", ARTIFACT, 3, data={"id": 3, "kind": "dataset", "_aux": None, "branch_id": 1}
            ),
        ]
        hook = FakeHook(writes=writes, records={"core.artifact": {1: artifact(1, aux={"so": 1})}})
        trigger = LaminDBArtifactEventTrigger(events=["updated", "deleted"])
        assert summary(await events_for(trigger, hook)) == [("deleted", 3, 3)]

    async def test_without_wait_for_upload(self):
        writes = [
            dbwrite(1, "INSERT", ARTIFACT, 1),
            dbwrite(2, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}'}),
        ]
        hook = FakeHook(writes=writes, records={"core.artifact": {1: artifact(1, aux={"so": 1})}})
        trigger = LaminDBArtifactEventTrigger(events=["created", "updated"], wait_for_upload=False)
        assert summary(await events_for(trigger, hook)) == [("created", 1, 1), ("updated", 1, 2)]

    async def test_internal_artifacts_and_filters(self):
        writes = [dbwrite(i, "INSERT", ARTIFACT, i) for i in (1, 2, 3)]
        records = {
            "core.artifact": {
                1: artifact(1, kind="__lamindb_run__", key="logs/run.txt"),
                2: artifact(2, key="raw/b.csv"),
                3: artifact(3, key="other/c.parquet"),
            }
        }
        hook = FakeHook(writes=writes, records=records)
        assert summary(await events_for(LaminDBArtifactEventTrigger(), hook)) == [
            ("created", 2, 2),
            ("created", 3, 3),
        ]
        assert summary(await events_for(LaminDBArtifactEventTrigger(include_internal=True), hook)) == [
            ("created", 1, 1),
            ("created", 2, 2),
            ("created", 3, 3),
        ]
        assert summary(await events_for(LaminDBArtifactEventTrigger(key_prefix="raw/"), hook)) == [
            ("created", 2, 2),
        ]
        assert summary(await events_for(LaminDBArtifactEventTrigger(suffix=[".parquet"]), hook)) == [
            ("created", 3, 3),
        ]

    async def test_dbwrite_filter_includes_upload_completions(self):
        trigger = LaminDBArtifactEventTrigger()
        await trigger._setup(FakeHook())
        dbwrite_filter = trigger._dbwrite_filter()
        assert evaluate(dbwrite_filter, dbwrite(1, "UPDATE", ARTIFACT, 1, data={"_aux": '{"so": 1}'}))
        assert not evaluate(dbwrite_filter, dbwrite(2, "UPDATE", ARTIFACT, 1, data={"key": "x"}))

    def test_serialize(self):
        trigger = LaminDBArtifactEventTrigger(key_prefix="raw/", suffix=".csv", events="updated")
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.lamindb.triggers.records.LaminDBArtifactEventTrigger"
        assert "registry" not in kwargs
        assert kwargs["suffix"] == [".csv"]
        assert LaminDBArtifactEventTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_kind_enum_and_filter_builder(self):
        trigger = LaminDBArtifactEventTrigger(
            kind=[ArtifactKind.DATASET, ArtifactKind.MODEL], filter=F(ArtifactField.SIZE) > 0
        )
        _, kwargs = trigger.serialize()
        assert kwargs["kind"] == ["dataset", "model"]
        assert [type(kind) for kind in kwargs["kind"]] == [str, str]
        assert kwargs["filter"] == {"size": {"gt": 0}}
        assert trigger._record_filter() == {
            "and": [{"size": {"gt": 0}}, {"kind": {"in": ["dataset", "model"]}}]
        }


class TestRecordSensorTrigger:
    @pytest.fixture
    def trigger(self, monkeypatch):
        def make(hook: FakeHook, **kwargs: Any) -> LaminDBRecordSensorTrigger:
            trigger = LaminDBRecordSensorTrigger(
                **{"registry": "core.artifact", "poll_interval": 0, **kwargs}
            )
            monkeypatch.setattr(trigger, "_get_hook", lambda: hook)
            return trigger

        return make

    async def test_success(self, trigger):
        hook = FakeHook(records={"core.artifact": {1: artifact(1), 2: artifact(2, aux={"so": 1})}})
        events = [e.payload async for e in trigger(hook, wait_for_upload=True).run()]
        assert events == [{"status": "success", "records": [artifact(1)]}]
        assert hook.closed

    async def test_waits_until_enough_records(self, trigger, monkeypatch):
        hook = FakeHook(records={"core.artifact": {1: artifact(1)}})

        async def add_record(delay):
            hook.records["core.artifact"][2] = artifact(2)

        monkeypatch.setattr("airflow.providers.lamindb.triggers.records.asyncio.sleep", add_record)
        events = [e.payload async for e in trigger(hook, min_count=2).run()]
        assert events == [{"status": "success", "records": [artifact(2), artifact(1)]}]

    async def test_error(self, trigger):
        hook = FakeHook()
        hook.fail_next = [LaminDBApiError("forbidden")]
        events = [e.payload async for e in trigger(hook).run()]
        assert events == [{"status": "error", "message": "forbidden"}]

    def test_serialize(self):
        trigger = LaminDBRecordSensorTrigger("core.run", {"name": {"eq": "x"}}, branch=None, min_count=2)
        classpath, kwargs = trigger.serialize()
        assert LaminDBRecordSensorTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_filter_builder_and_enums(self):
        trigger = LaminDBRecordSensorTrigger(LaminDBRegistry.RUN, F(RunField.NAME) == "x", branch=None)
        _, kwargs = trigger.serialize()
        assert type(kwargs["registry"]) is str
        assert kwargs["registry"] == "core.run"
        assert kwargs["filter"] == {"name": {"eq": "x"}}

    async def test_filter_builder(self, trigger):
        hook = FakeHook(
            records={"core.artifact": {1: artifact(1, key="raw/a.csv"), 2: artifact(2, key="b.csv")}}
        )
        events = [
            e.payload async for e in trigger(hook, filter=F(ArtifactField.KEY).startswith("raw/")).run()
        ]
        assert events == [{"status": "success", "records": [artifact(1, key="raw/a.csv")]}]


def test_branch_scope_filter():
    assert branch_scope_filter(None) is None
    assert branch_scope_filter("main") == {"branch_id": {"eq": 1}}
    assert branch_scope_filter(5) == {"branch_id": {"eq": 5}}
    assert branch_scope_filter("feature") == {"branch.name": {"eq": "feature"}}


def test_ready_records():
    records = [artifact(1), artifact(2, aux={"so": 1})]
    assert ready_records(records, wait_for_upload=False) == records
    assert ready_records(records, wait_for_upload=True) == [artifact(1)]
