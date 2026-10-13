from __future__ import annotations

from typing import Any

import pytest
from airflow.triggers.base import TriggerEvent

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers import base
from airflow.providers.lamindb.triggers.base import LaminDBDbWriteEventTrigger
from airflow.providers.lamindb.triggers.cursors import AssetCursorStore, RecordCursorStore
from airflow.providers.lamindb.triggers.records import LaminDBRecordEventTrigger
from airflow.providers.lamindb.triggers.replay import replay_key
from unit.lamindb.fakes import INSTANCE, FakeHook, FakeStateStore, dbwrite
from unit.lamindb.fakes import evaluate as fake_evaluate


class EchoTrigger(LaminDBDbWriteEventTrigger):
    """Reports every write of a table."""

    def __init__(self, *, table: str = "t", **kwargs: Any) -> None:
        kwargs.setdefault("cursor_store", AssetCursorStore(commit_delay=0))  # commit in the next cycle
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


def lamindb_cursor(hook, key: str, store: RecordCursorStore | None = None):
    return (store or RecordCursorStore()).open(hook=hook, asset_state_store=None, key=key, description="")


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

    async def test_commit_waits_for_commit_delay(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger(cursor_store=AssetCursorStore(commit_delay=3600))
        trigger.asset_state_store = store = FakeStateStore({trigger.state_key: stored(0)})
        fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].extend([lambda: None, lambda: None])

        await collect(trigger)

        assert store.writes == []

    async def test_cursor_is_committed_under_steady_writes(self, fake_hook_factory, sleeps, monkeypatch):
        clock = {"now": 0.0}
        monkeypatch.setattr(base.time, "monotonic", lambda: clock["now"])
        yielded_at: dict[int, float] = {}
        saved_at: list[tuple[int, float]] = []

        class TimedStore(FakeStateStore):
            async def aset(self, key, value):
                saved_at.append((value["dbwrite_id"], clock["now"]))
                await super().aset(key, value)

        trigger = EchoTrigger(poll_interval=2, cursor_store=AssetCursorStore(commit_delay=5.0))
        trigger.asset_state_store = TimedStore({trigger.state_key: stored(0)})
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))

        def next_cycle(write_id: int) -> None:
            clock["now"] += 2
            hook.writes.append(dbwrite(write_id, "INSERT", "t", write_id))

        sleeps["callbacks"].extend(lambda i=i: next_cycle(i) for i in range(2, 8))

        async for event in _run_until_stopped(trigger):
            yielded_at[event["dbwrite"]["id"]] = clock["now"]

        assert [cursor for cursor, _ in saved_at] == [1, 2, 3, 4]
        # never past events yielded less than commit_delay ago (at least once)
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

    async def test_cursor_in_lamindb(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger(cursor_store=RecordCursorStore(commit_delay=0))
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].extend(
            [lambda: hook.writes.append(dbwrite(hook.writes[-1]["id"] + 1, "INSERT", "t", 2)), lambda: None]
        )

        events = await collect(trigger)

        # the first start saves write 1, the record type and record inserts are writes 2 and 3
        assert [e["dbwrite"]["id"] for e in events] == [4]
        record = hook.records["core.record"][2]
        assert record["name"] == trigger.state_key
        assert record["description"].startswith("Cursor of the Airflow trigger EchoTrigger: {")
        assert record["extra_data"] == stored(4)

        restarted = EchoTrigger(cursor_store=RecordCursorStore(commit_delay=0))
        fake_hook_factory(restarted, hook)
        sleeps["callbacks"].append(lambda: None)
        assert await collect(restarted) == []  # resumes after write 4 (its own update is not a "t" write)

    async def test_without_state_store(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        hook = fake_hook_factory(trigger, FakeHook(writes=[dbwrite(1, "INSERT", "t", 1)]))
        sleeps["callbacks"].append(lambda: hook.writes.append(dbwrite(2, "INSERT", "t", 2)))

        assert [e["dbwrite"]["id"] for e in await collect(trigger)] == [2]


def ids_and_replays(events: list[dict[str, Any]]) -> list[tuple[int, Any]]:
    return [(e["dbwrite"]["id"], e.get("replay")) for e in events]


class TestReplay:
    """Replay requests in the asset state store, delivered next to the live events."""

    @staticmethod
    def setup(fake_hook_factory, request: Any, *, writes=None, **kwargs: Any):
        """A trigger whose cursor is past writes 1-5 of table t, with ``request`` in its state store."""
        trigger = EchoTrigger(poll_interval=10, **kwargs)
        initial = {trigger.state_key: stored(5)}
        if request is not None:
            initial[replay_key(trigger.state_key)] = request
        trigger.asset_state_store = store = FakeStateStore(initial)
        writes = writes if writes is not None else [dbwrite(i, "INSERT", "t", i) for i in range(1, 6)]
        hook = fake_hook_factory(trigger, FakeHook(writes=writes))
        return trigger, store, hook

    async def test_replays_a_range_next_to_live_events(self, fake_hook_factory, sleeps):
        trigger, store, hook = self.setup(
            fake_hook_factory, {"id": "r1", "from_dbwrite_id": 2, "to_dbwrite_id": 4}
        )
        sleeps["callbacks"].append(lambda: hook.writes.append(dbwrite(6, "INSERT", "t", 6)))

        events = await collect(trigger)

        replayed = {"id": "r1"}
        assert ids_and_replays(events) == [(2, replayed), (3, replayed), (4, replayed), (6, None)]
        request = store.data[replay_key(trigger.state_key)]
        assert request.pop("finished_at")
        assert request == {
            "id": "r1",
            "from_dbwrite_id": 2,
            "to_dbwrite_id": 4,
            "status": "done",
            "events": 3,
        }
        # the request was marked running when the trigger picked it up
        assert [v["status"] for k, v in store.writes if k == replay_key(trigger.state_key)] == [
            "running",
            "done",
        ]
        # replays never move the cursor
        assert all(k != trigger.state_key for k, _ in store.writes)
        assert sleeps["delays"] == [10, 10]

    @pytest.mark.parametrize("to_dbwrite_id", [None, 100])
    async def test_range_ends_at_the_live_cursor(self, fake_hook_factory, sleeps, to_dbwrite_id):
        request = {"from_dbwrite_id": 4} | ({"to_dbwrite_id": to_dbwrite_id} if to_dbwrite_id else {})
        trigger, _, _ = self.setup(fake_hook_factory, request)

        events = await collect(trigger)

        assert ids_and_replays(events) == [(4, {"id": None}), (5, {"id": None})]

    async def test_replays_a_list_of_writes(self, fake_hook_factory, sleeps):
        writes = [dbwrite(i, "INSERT", "t" if i != 3 else "other", i) for i in range(1, 6)]
        trigger, store, _ = self.setup(
            fake_hook_factory, {"id": "fix", "dbwrite_ids": [5, 3, 2, 2, 9]}, writes=writes
        )
        sleeps["callbacks"].append(lambda: None)

        events = await collect(trigger)

        # write 3 is not one of the trigger's writes, and write 9 comes later as a live event
        assert ids_and_replays(events) == [(2, {"id": "fix"}), (5, {"id": "fix"})]
        assert store.data[replay_key(trigger.state_key)]["events"] == 2

    async def test_replays_in_batches_across_polls(self, fake_hook_factory, sleeps):
        trigger, store, hook = self.setup(
            fake_hook_factory, {"from_dbwrite_id": 1}, batch_size=2, max_batches_per_poll=1
        )
        sleeps["callbacks"].extend([lambda: None] * 3)

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [1, 2, 3, 4, 5]
        replay_queries = [q for q in hook.dbwrite_queries if "lte" in str(q["filter"])]
        assert [q["after_id"] for q in replay_queries] == [0, 2, 4]
        assert store.data[replay_key(trigger.state_key)]["status"] == "done"

    async def test_restarted_trigger_starts_a_running_replay_over(self, fake_hook_factory, sleeps):
        trigger, _, _ = self.setup(
            fake_hook_factory, {"from_dbwrite_id": 5, "status": "running", "started_at": "2026-10-13"}
        )

        assert [e["dbwrite"]["id"] for e in await collect(trigger)] == [5]

    @pytest.mark.parametrize(
        ("request_value", "error"),
        [
            ({"from_dbwrite_id": 3, "to_dbwrite_id": 2}, "to_dbwrite_id 2 is lower than from_dbwrite_id 3"),
            ({"from": 3}, "Unknown replay request fields ['from']"),
            ({"dbwrite_ids": []}, "dbwrite_ids must be a non-empty list of database write ids"),
            ("everything", "A replay request must be a JSON object, got 'everything'"),
        ],
    )
    async def test_marks_invalid_requests(self, fake_hook_factory, sleeps, request_value, error):
        trigger, store, _ = self.setup(fake_hook_factory, request_value)
        sleeps["callbacks"].append(lambda: None)

        assert await collect(trigger) == []

        request = store.data[replay_key(trigger.state_key)]
        assert request["status"] == "invalid"
        assert request["error"] == error
        assert len(store.writes) == 1  # not picked up again

    @pytest.mark.parametrize("status", ["done", "invalid", "failed"])
    async def test_ignores_finished_requests(self, fake_hook_factory, sleeps, status):
        trigger, store, _ = self.setup(fake_hook_factory, {"from_dbwrite_id": 1, "status": status})

        assert await collect(trigger) == []
        assert store.writes == []

    async def test_replaced_request_stops_the_running_replay(self, fake_hook_factory, sleeps):
        trigger, store, _ = self.setup(
            fake_hook_factory, {"id": "r1", "from_dbwrite_id": 1}, batch_size=1, max_batches_per_poll=1
        )
        key = replay_key(trigger.state_key)
        sleeps["callbacks"].extend(
            [lambda: store.data.update({key: {"id": "r2", "dbwrite_ids": [5]}}), lambda: None]
        )

        events = await collect(trigger)

        assert ids_and_replays(events) == [(1, {"id": "r1"}), (5, {"id": "r2"})]
        assert store.data[key]["id"] == "r2"
        assert store.data[key]["status"] == "done"

    async def test_withdrawn_request_stops_the_running_replay(self, fake_hook_factory, sleeps):
        trigger, store, _ = self.setup(
            fake_hook_factory, {"from_dbwrite_id": 1}, batch_size=1, max_batches_per_poll=1
        )
        key = replay_key(trigger.state_key)
        sleeps["callbacks"].extend([lambda: store.data.pop(key), lambda: None])

        events = await collect(trigger)

        assert [e["dbwrite"]["id"] for e in events] == [1]
        assert key not in store.data

    async def test_api_errors_are_retried_without_slowing_live_polling(self, fake_hook_factory, sleeps):
        trigger, store, hook = self.setup(fake_hook_factory, {"from_dbwrite_id": 4})
        query = hook.aquery_dbwrites
        failures = [LaminDBApiError("down")]

        async def flaky(filter=None, **kwargs):
            if failures and "lte" in str(filter):
                raise failures.pop()
            return await query(filter, **kwargs)

        hook.aquery_dbwrites = flaky
        sleeps["callbacks"].extend([lambda: hook.writes.append(dbwrite(6, "INSERT", "t", 6)), lambda: None])

        events = await collect(trigger)

        assert ids_and_replays(events) == [(6, None), (4, {"id": None}), (5, {"id": None})]
        assert store.data[replay_key(trigger.state_key)]["status"] == "done"
        assert sleeps["delays"] == [10, 10, 10]

    async def test_other_errors_fail_the_request(self, fake_hook_factory, sleeps):
        class PoisonedTrigger(EchoTrigger):
            async def _events_for(self, hook, instance, writes):
                if any(w["id"] == 2 for w in writes):
                    raise KeyError("data")
                return await super()._events_for(hook, instance, writes)

        trigger = PoisonedTrigger(poll_interval=10)
        key = replay_key(trigger.state_key)
        trigger.asset_state_store = store = FakeStateStore(
            {trigger.state_key: stored(5), key: {"from_dbwrite_id": 1, "to_dbwrite_id": 3}}
        )
        hook = fake_hook_factory(
            trigger, FakeHook(writes=[dbwrite(i, "INSERT", "t", i) for i in range(1, 6)])
        )
        sleeps["callbacks"].extend([lambda: hook.writes.append(dbwrite(6, "INSERT", "t", 6))])

        events = await collect(trigger)

        assert ids_and_replays(events) == [(6, None)]
        assert store.data[key]["status"] == "failed"
        assert store.data[key]["error"] == "KeyError: 'data'"
        assert sleeps["delays"] == [10, 10]

    async def test_request_on_any_watched_asset(self, fake_hook_factory, sleeps):
        trigger = EchoTrigger()
        key = replay_key(trigger.state_key)
        first = FakeStateStore({trigger.state_key: stored(5)})
        second = FakeStateStore({trigger.state_key: stored(5), key: {"dbwrite_ids": [3]}})

        class TwoAssets:
            _by_name = {"a": first}
            _by_uri = {"b://x": second}

        trigger.asset_state_store = TwoAssets()
        fake_hook_factory(trigger, FakeHook(writes=[dbwrite(i, "INSERT", "t", i) for i in range(1, 6)]))
        sleeps["callbacks"].append(lambda: None)

        assert [e["dbwrite"]["id"] for e in await collect(trigger)] == [3]
        assert second.data[key]["status"] == "done"
        assert key not in first.data


class TestSerialization:
    def test_roundtrip(self):
        trigger = EchoTrigger(
            table="x",
            instance="a/b",
            poll_interval=5,
            batch_size=50,
            max_batches_per_poll=3,
            cursor_store=RecordCursorStore(space="airflow", record_type=None, commit_delay=1),
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
            "cursor_store": {
                "type": "record",
                "commit_delay": 1,
                "registry": "core.record",
                "field": "extra_data",
                "record_type": None,
                "reference_type": "airflow_trigger_cursor",
                "branch": "main",
                "space": "airflow",
                "name": None,
                "description": None,
            },
        }
        assert EchoTrigger(**kwargs).serialize() == (classpath, kwargs)
        assert EchoTrigger(**kwargs).cursor_store == trigger.cursor_store
        assert type(EchoTrigger(cursor_store=None).cursor_store) is AssetCursorStore

    def test_state_key(self):
        key = EchoTrigger(table="x").state_key
        assert key.startswith("lamindb.EchoTrigger.")
        assert EchoTrigger(table="x", poll_interval=1, batch_size=10).state_key == key
        assert EchoTrigger(table="x", cursor_store=RecordCursorStore()).state_key == key
        assert EchoTrigger(table="y").state_key != key
        assert EchoTrigger(table="x", instance="a/b").state_key != key

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"poll_interval": 0}, "poll_interval"),
            ({"batch_size": 201}, "batch_size"),
            ({"max_batches_per_poll": 0}, "max_batches_per_poll"),
            ({"cursor_store": "lamindb"}, "cursor_store must be a CursorStore"),
            ({"cursor_store": {"type": "variable"}}, "cursor_store type must be one of"),
            ({"cursor_store": {"type": "record", "spaces": "x"}}, "Unknown RecordCursorStore settings"),
            ({"cursor_store": {"type": "asset", "space": "x"}}, "Unknown AssetCursorStore settings"),
        ],
    )
    def test_validation(self, kwargs, message):
        with pytest.raises((ValueError, TypeError), match=message):
            EchoTrigger(**kwargs)


class TestCursorRecords:
    """``core.record`` watchers and the records that hold cursors in LaminDB."""

    async def test_cursor_records_are_not_reported(self, fake_hook_factory, sleeps):
        trigger = LaminDBRecordEventTrigger(
            "core.record", events=["created", "updated"], cursor_store=AssetCursorStore(commit_delay=0)
        )
        trigger.asset_state_store = FakeStateStore({trigger.state_key: stored(0)})
        hook = fake_hook_factory(trigger, FakeHook())
        await lamindb_cursor(hook, "other.trigger").save(1, INSTANCE.id)
        hook.insert("core.record", {"name": "sample-1", "is_type": False})

        events = await collect(trigger)

        assert [e["record"]["name"] for e in events] == ["sample-1"]

    async def test_own_cursor_saves_do_not_trigger_more_saves(self, fake_hook_factory, sleeps):
        trigger = LaminDBRecordEventTrigger(
            "core.record", events=["created", "updated"], cursor_store=RecordCursorStore(commit_delay=0)
        )
        hook = fake_hook_factory(trigger, FakeHook())
        sleeps["callbacks"] = [lambda: hook.insert("core.record", {"name": "sample-1", "is_type": False})]
        sleeps["callbacks"] += [lambda: None] * 6

        events = await collect(trigger)

        assert [e["record"]["name"] for e in events] == ["sample-1"]
        # one save after the sample, none for the cursor writes that the save itself causes
        assert len(hook.updates) == 1

    async def test_two_watchers_storing_cursors_in_lamindb_settle(self, fake_hook_factory):
        first = LaminDBRecordEventTrigger("core.record", cursor_store=RecordCursorStore())
        second = LaminDBRecordEventTrigger(
            "core.record", events=["updated"], cursor_store=RecordCursorStore()
        )
        hook = FakeHook()
        for trigger in (first, second):
            await trigger._setup(hook)
        await lamindb_cursor(hook, first.state_key).save(1, INSTANCE.id)
        await lamindb_cursor(hook, second.state_key).save(1, INSTANCE.id)

        cursor_writes = [w for w in hook.writes if w["table_name"] == "lamindb_record"]
        assert not await first._should_commit(hook, cursor_writes)
        assert not await second._should_commit(hook, cursor_writes)
        hook.insert("core.record", {"name": "sample-1", "is_type": False})
        assert await first._should_commit(hook, [hook.writes[-1]])

    async def test_cursor_records_are_recognized_by_the_triggers_own_settings(self):
        custom = RecordCursorStore(reference_type="my_cursor")
        hook = FakeHook()
        await lamindb_cursor(hook, "custom.trigger", custom).save(1, INSTANCE.id)
        same_settings = LaminDBRecordEventTrigger("core.record", cursor_store=custom)
        default_settings = LaminDBRecordEventTrigger("core.record")
        other_registry = LaminDBRecordEventTrigger(
            "core.record", cursor_store=RecordCursorStore(registry="core.ulabel")
        )

        reported = {}
        for name, trigger in [
            ("same", same_settings),
            ("default", default_settings),
            ("other_registry", other_registry),
        ]:
            reported[name] = len(await events_of(trigger, hook))

        assert reported == {"same": 0, "default": 2, "other_registry": 2}


async def events_of(trigger, hook) -> list[dict[str, Any]]:
    await trigger._setup(hook)
    writes = [w for w in hook.writes if fake_evaluate(trigger._dbwrite_filter(), w)]
    return [e.payload for e in await trigger._events_for(hook, INSTANCE, writes)]


@pytest.mark.parametrize("registry", ["core.record", "Record", "core.Record", "lamindb.record"])
def test_cursor_in_the_watched_registry_needs_a_marker(registry):
    with pytest.raises(ValueError, match="needs a reference_type"):
        LaminDBRecordEventTrigger(registry, cursor_store=RecordCursorStore(reference_type=None))
    # elsewhere, no marker is fine
    LaminDBRecordEventTrigger("core.ulabel", cursor_store=RecordCursorStore(reference_type=None))


def test_commit_delay_must_not_be_negative():
    with pytest.raises(ValueError, match="commit_delay"):
        AssetCursorStore(commit_delay=-1)
