from __future__ import annotations

from typing import Any

import pytest
from airflow.triggers.base import TriggerEvent

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers import base
from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger, _CursorStore
from unit.lamindb.fakes import INSTANCE, FakeHook, FakeStateStore, dbwrite


class EchoTrigger(LaminDBDbWriteEventTrigger):
    """Reports every write of a table."""

    def __init__(self, *, table: str = "t", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.table = table

    def _trigger_kwargs(self) -> dict[str, Any]:
        return {"table": self.table}

    def _dbwrite_filter(self):
        return {"table_name": {"eq": self.table}}

    async def _events_for(self, hook, instance, writes):
        await self._load_users(hook, writes)
        return [TriggerEvent(self._payload(instance, w, event="echo")) for w in writes]


class StopPolling(BaseException):
    pass


@pytest.fixture
def sleeps(monkeypatch):
    """Record sleeps of the polling loop; run ``on_sleep`` callbacks and stop after the last one."""
    state: dict[str, Any] = {"delays": [], "callbacks": []}

    async def fake_sleep(delay):
        state["delays"].append(delay)
        if not state["callbacks"]:
            raise StopPolling
        state["callbacks"].pop(0)()

    monkeypatch.setattr(base.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(base, "_MIN_COMMIT_DELAY", 0.0)
    return state


async def collect(trigger) -> list[dict[str, Any]]:
    events = []

    async def consume():
        async for event in trigger.run():
            events.append(event.payload)

    with pytest.raises(StopPolling):
        await consume()
    return events


async def _run_until_stopped(trigger):
    stream = trigger.run()
    try:
        while True:
            yield (await stream.__anext__()).payload
    except StopPolling:
        return


def stored(cursor: int, instance_id: str = INSTANCE.id) -> dict[str, Any]:
    return {"dbwrite_id": cursor, "instance_id": instance_id}


class TestPolling:
    async def test_first_start_reports_only_new_writes(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger(poll_interval=10)
        trigger.asset_state_store = store = FakeStateStore()
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].append(lambda: hook.writes.append(dbwrite(2, "INSERT", "t", 2)))

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [2]
        # the cursor past write 2 would only be committed in the next cycle
        assert store.writes == [(trigger.state_key, stored(1))]
        assert sleeps["delays"] == [10, 10]
        assert hook.closed

    async def test_resumes_from_stored_cursor(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        trigger.asset_state_store = FakeStateStore({trigger.state_key: stored(1)})
        writes = [dbwrite(i, "INSERT", "t", i) for i in (1, 2, 3)] + [dbwrite(4, "INSERT", "other", 4)]
        fake_hook_factory(trigger, FakeHook(writes=writes))

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [2, 3]
        assert events[0] == {
            "instance": "owner/name",
            "instance_id": INSTANCE.id,
            "event": "echo",
            "changed_by": {"id": 7, "handle": "alice", "name": "Alice"},
            "dbwrite": {
                "id": 2,
                "uid": "uid2",
                "event_type": "INSERT",
                "table_name": "t",
                "sqlrecord_id": 2,
                "created_at": "2026-09-30T08:00:00+00:00",
                "created_by_id": 7,
                "branch_id": 1,
                "space_id": 1,
                "run_id": None,
            },
        }

    async def test_ignores_cursor_of_other_instance(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        trigger.asset_state_store = store = FakeStateStore({trigger.state_key: stored(0, "other-instance")})
        fake_hook_factory(trigger, FakeHook(writes=[dbwrite(5, "INSERT", "t", 1)]))

        assert await collect(trigger) == []
        assert store.data[trigger.state_key] == stored(5)

    async def test_cursor_is_committed_one_cycle_later(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        trigger.asset_state_store = store = FakeStateStore({trigger.state_key: stored(0)})
        fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))

        # stopped before the next cycle: the cursor was not committed past the yielded event
        assert len(await collect(trigger)) == 1
        assert store.writes == []

        trigger.asset_state_store = store
        sleeps["callbacks"].append(lambda: None)
        assert len(await collect(trigger)) == 1  # redelivered (at least once)
        assert store.writes == [(trigger.state_key, stored(1))]

    async def test_commit_waits_for_min_delay(self, fake_hook_factory, sleeps, monkeypatch):
        monkeypatch.setattr(base, "_MIN_COMMIT_DELAY", 3600.0)
        trigger = EchoTrigger()
        trigger.asset_state_store = store = FakeStateStore({trigger.state_key: stored(0)})
        fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].extend([lambda: None, lambda: None])

        await collect(trigger)

        assert store.writes == []

    async def test_cursor_is_committed_under_steady_writes(self, fake_hook_factory, sleeps, monkeypatch):
        monkeypatch.setattr(base, "_MIN_COMMIT_DELAY", 5.0)
        clock = {"now": 0.0}
        monkeypatch.setattr(base.time, "monotonic", lambda: clock["now"])
        yielded_at: dict[int, float] = {}
        saved_at: list[tuple[int, float]] = []

        class TimedStore(FakeStateStore):
            async def aset(self, key, value):
                saved_at.append((value["dbwrite_id"], clock["now"]))
                await super().aset(key, value)

        trigger = EchoTrigger(poll_interval=2)
        trigger.asset_state_store = TimedStore({trigger.state_key: stored(0)})
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))

        def next_cycle(write_id: int) -> None:
            clock["now"] += 2
            hook.writes.append(dbwrite(write_id, "INSERT", "t", write_id))

        sleeps["callbacks"].extend(lambda i=i: next_cycle(i) for i in range(2, 8))

        async for event in _run_until_stopped(trigger):
            yielded_at[event["dbwrite"]["id"]] = clock["now"]

        assert [cursor for cursor, _ in saved_at] == [1, 2, 3, 4]
        # never past events yielded less than _MIN_COMMIT_DELAY ago (at least once)
        assert all(saved - yielded_at[cursor] >= 5.0 for cursor, saved in saved_at)

    async def test_drains_in_batches(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger(batch_size=2, max_batches_per_poll=2)
        trigger.asset_state_store = FakeStateStore({trigger.state_key: stored(0)})
        hook = fake_hook_factory(
            trigger, FakeHook(writes=[dbwrite(i, "INSERT", "t", i) for i in range(1, 6)])
        )
        sleeps["callbacks"].append(lambda: None)

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [1, 2, 3, 4, 5]
        assert [q["after_id"] for q in hook.dbwrite_queries] == [0, 2, 4]
        assert all(q["limit"] == 2 for q in hook.dbwrite_queries)

    async def test_backs_off_on_errors(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger(poll_interval=10)
        trigger.asset_state_store = FakeStateStore({trigger.state_key: stored(0)})
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        hook.fail_next = [LaminDBApiError("down"), LaminDBApiError("still down")]
        sleeps["callbacks"].extend([lambda: None, lambda: None])

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [1]
        assert sleeps["delays"] == [20, 40, 10]

    async def test_without_state_store(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].append(lambda: hook.writes.append(dbwrite(2, "INSERT", "t", 2)))

        assert [e["dbwrite"]["id"] for e in await collect(trigger)] == [2]


class MultiAssetStore:
    """Mimics ``AssetStateStoreAccessors`` with several watched assets."""

    def __init__(self, *accessors: FakeStateStore) -> None:
        self._by_name = {f"asset{i}": accessor for i, accessor in enumerate(accessors)}
        self._by_uri: dict[str, FakeStateStore] = {}


class TestCursorStore:
    async def test_multiple_assets(self):
        first, second, third = (
            FakeStateStore({"k": stored(5)}),
            FakeStateStore({"k": stored(3)}),
            FakeStateStore(),
        )
        cursor_store = _CursorStore(MultiAssetStore(first, second, third), "k")
        assert await cursor_store.load(INSTANCE.id) == 3
        await cursor_store.save(9, INSTANCE.id)
        assert first.data["k"] == second.data["k"] == third.data["k"] == stored(9)

    async def test_real_airflow_accessors_with_multiple_assets(self):
        from airflow.sdk import Asset
        from airflow.sdk.execution_time.context import AssetStateStoreAccessors

        store = AssetStateStoreAccessors(
            inlets=[Asset("a"), Asset("b")], outlets=[Asset(name="c", uri="s3://c")]
        )
        assert len(_CursorStore(store, "k")._accessors) == 3

    async def test_unsupported_store_disables_persistence(self, caplog):
        assert not _CursorStore(object(), "k").is_persistent
        assert "Unsupported asset state store object" in caplog.text

    async def test_invalid_values_are_ignored(self):
        cursor_store = _CursorStore(
            FakeStateStore({"k": {"dbwrite_id": "x", "instance_id": INSTANCE.id}}), "k"
        )
        assert await cursor_store.load(INSTANCE.id) is None
        assert not _CursorStore(None, "k").is_persistent


class TestSerialization:
    def test_roundtrip(self):
        trigger = EchoTrigger(
            table="x", instance="a/b", poll_interval=5, batch_size=50, max_batches_per_poll=3
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == f"{EchoTrigger.__module__}.EchoTrigger"
        assert kwargs == {
            "table": "x",
            "lamindb_conn_id": "lamindb_default",
            "instance": "a/b",
            "poll_interval": 5,
            "batch_size": 50,
            "max_batches_per_poll": 3,
        }
        assert EchoTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_state_key(self):
        key = EchoTrigger(table="x").state_key
        assert key.startswith("lamindb.EchoTrigger.")
        assert EchoTrigger(table="x", poll_interval=1, batch_size=10).state_key == key
        assert EchoTrigger(table="y").state_key != key
        assert EchoTrigger(table="x", instance="a/b").state_key != key

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"poll_interval": 0}, "poll_interval"),
            ({"batch_size": 201}, "batch_size"),
            ({"max_batches_per_poll": 0}, "max_batches_per_poll"),
        ],
    )
    def test_validation(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            EchoTrigger(**kwargs)
