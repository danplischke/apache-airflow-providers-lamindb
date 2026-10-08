from __future__ import annotations

import json
from typing import Any

import pytest

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.triggers.cursors import (
    CURSOR_RECORD_TYPE_NAME,
    CURSOR_REFERENCE_TYPE,
    AssetCursorStore,
    CursorStore,
    RecordCursorStore,
    is_cursor_record,
)
from unit.lamindb.fakes import INSTANCE, FakeHook, FakeStateStore


def stored(cursor: int, instance_id: str = INSTANCE.id) -> dict[str, Any]:
    return {"dbwrite_id": cursor, "instance_id": instance_id}


class MultiAssetStore:
    """Mimics ``AssetStateStoreAccessors`` with several watched assets."""

    def __init__(self, *accessors: FakeStateStore) -> None:
        self._by_name = {f"asset{i}": accessor for i, accessor in enumerate(accessors)}
        self._by_uri: dict[str, FakeStateStore] = {}


class TestAssetCursorStore:
    async def test_multiple_assets(self):
        first, second, third = (
            FakeStateStore({"k": stored(5)}),
            FakeStateStore({"k": stored(3)}),
            FakeStateStore(),
        )
        cursor_store = asset_cursor(MultiAssetStore(first, second, third))
        assert await cursor_store.load(INSTANCE.id) == 3
        await cursor_store.save(9, INSTANCE.id)
        assert first.data["k"] == second.data["k"] == third.data["k"] == stored(9)

    async def test_real_airflow_accessors_with_multiple_assets(self):
        from airflow.sdk import Asset
        from airflow.sdk.execution_time.context import AssetStateStoreAccessors

        store = AssetStateStoreAccessors(
            inlets=[Asset("a"), Asset("b")], outlets=[Asset(name="c", uri="s3://c")]
        )
        assert len(asset_cursor(store)._accessors) == 3

    async def test_unsupported_store_disables_persistence(self, caplog):
        assert not asset_cursor(object()).is_persistent
        assert "Unsupported asset state store object" in caplog.text

    async def test_invalid_values_are_ignored(self):
        cursor_store = asset_cursor(FakeStateStore({"k": {"dbwrite_id": "x", "instance_id": INSTANCE.id}}))
        assert await cursor_store.load(INSTANCE.id) is None
        assert not asset_cursor(None).is_persistent


DEFAULTS = RecordCursorStore()


def asset_cursor(asset_state_store: Any):
    return AssetCursorStore().open(hook=None, asset_state_store=asset_state_store, key="k", description="")


def lamindb_cursor(hook: FakeHook, settings: RecordCursorStore, key: str, description: str):
    return settings.open(hook=hook, asset_state_store=None, key=key, description=description)


def cursor_records(hook: FakeHook) -> dict[int, dict[str, Any]]:
    return hook.records.get("core.record", {})


class TestRecordCursorStore:
    async def test_first_save_creates_the_record_type_and_the_record(self):
        hook = FakeHook()
        cursor_store = lamindb_cursor(hook, DEFAULTS, "lamindb.T.abc", "Cursor of T")
        assert cursor_store.is_persistent
        assert await cursor_store.load(INSTANCE.id) is None

        await cursor_store.save(7, INSTANCE.id)

        record_type, record = cursor_records(hook).values()
        assert record_type["name"] == CURSOR_RECORD_TYPE_NAME
        assert record_type["is_type"] is True
        assert record == {
            "id": 2,
            "uid": "r2",
            "name": "lamindb.T.abc",
            "type_id": record_type["id"],
            "description": "Cursor of T",
            "extra_data": stored(7),
            "is_type": False,
            "reference_type": CURSOR_REFERENCE_TYPE,
            "branch_id": 1,
        }
        assert is_cursor_record(record_type)
        assert is_cursor_record(record)
        assert await lamindb_cursor(hook, DEFAULTS, "lamindb.T.abc", "").load(INSTANCE.id) == 7

    async def test_later_saves_update_the_record(self):
        hook = FakeHook()
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        await cursor_store.save(1, INSTANCE.id)
        await cursor_store.save(2, INSTANCE.id)

        assert len(cursor_records(hook)) == 2  # the record type and the record
        assert hook.updates == [("core.record", "r2", {"extra_data": stored(2)})]

    async def test_cursors_of_several_triggers_share_the_record_type(self):
        hook = FakeHook()
        await lamindb_cursor(hook, DEFAULTS, "first", "").save(1, INSTANCE.id)
        await lamindb_cursor(hook, DEFAULTS, "second", "").save(2, INSTANCE.id)

        records = list(cursor_records(hook).values())
        assert [r["name"] for r in records] == [CURSOR_RECORD_TYPE_NAME, "first", "second"]
        assert records[1]["type_id"] == records[2]["type_id"] == records[0]["id"]

    async def test_resumes_from_an_existing_record(self):
        hook = FakeHook()
        await lamindb_cursor(hook, DEFAULTS, "k", "").save(5, INSTANCE.id)
        restarted = lamindb_cursor(hook, DEFAULTS, "k", "")

        assert await restarted.load(INSTANCE.id) == 5
        await restarted.save(6, INSTANCE.id)
        assert hook.updates == [("core.record", "r2", {"extra_data": stored(6)})]

    async def test_ignores_cursors_of_other_instances_and_trashed_records(self):
        hook = FakeHook()
        await lamindb_cursor(hook, DEFAULTS, "k", "").save(5, "other-instance")
        assert await lamindb_cursor(hook, DEFAULTS, "k", "").load(INSTANCE.id) is None

        cursor_records(hook)[2]["branch_id"] = -1  # moved to the trash: the cursor is reset
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        assert await cursor_store.load(INSTANCE.id) is None
        await cursor_store.save(8, INSTANCE.id)
        assert cursor_records(hook)[3]["extra_data"] == stored(8)

    async def test_looks_the_record_up_if_laminhub_returns_no_row(self, monkeypatch):
        hook = FakeHook()
        insert = hook.ainsert_records

        async def insert_without_response(registry, records):
            await insert(registry, records)
            return []

        monkeypatch.setattr(hook, "ainsert_records", insert_without_response)
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        await cursor_store.save(1, INSTANCE.id)
        await cursor_store.save(2, INSTANCE.id)
        assert hook.updates == [("core.record", "r2", {"extra_data": stored(2)})]

    async def test_recreates_a_record_deleted_while_running(self):
        hook = FakeHook()
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        await cursor_store.save(1, INSTANCE.id)
        del cursor_records(hook)[2]

        await cursor_store.save(2, INSTANCE.id)

        assert [r["name"] for r in cursor_records(hook).values()] == [CURSOR_RECORD_TYPE_NAME, "k"]
        assert cursor_records(hook)[2]["extra_data"] == stored(2)

    async def test_other_update_errors_are_raised(self, monkeypatch):
        hook = FakeHook()
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        await cursor_store.save(1, INSTANCE.id)

        async def forbidden(*args, **kwargs):
            raise LaminDBApiError("forbidden", http_status_code=403)

        monkeypatch.setattr(hook, "aupdate_record", forbidden)
        with pytest.raises(LaminDBApiError, match="forbidden"):
            await cursor_store.save(2, INSTANCE.id)


class TestRecordCursorStoreSettings:
    async def test_registry_without_types_or_marker(self):
        hook = FakeHook()
        settings = RecordCursorStore(registry="core.ulabel", record_type=None, reference_type=None)
        cursor_store = lamindb_cursor(hook, settings, "k", "")
        await cursor_store.save(3, INSTANCE.id)

        assert "core.record" not in hook.records
        (record,) = hook.records["core.ulabel"].values()
        assert record == {
            "id": 1,
            "uid": "r1",
            "name": "k",
            "description": "",
            "extra_data": stored(3),
            "branch_id": 1,
        }
        assert await lamindb_cursor(hook, settings, "k", "").load(INSTANCE.id) == 3

    async def test_field_type_marker_name_and_description(self):
        hook = FakeHook()
        settings = RecordCursorStore(
            field="_aux",
            record_type="Cursors",
            reference_type="my_cursor",
            name="nightly-watcher",
            description="Cursor of the nightly watcher",
        )
        await lamindb_cursor(hook, settings, "lamindb.T.abc", "generated").save(3, INSTANCE.id)

        record_type, record = cursor_records(hook).values()
        assert (record_type["name"], record_type["reference_type"]) == ("Cursors", "my_cursor")
        assert record["name"] == "nightly-watcher"
        assert record["description"] == "Cursor of the nightly watcher"
        assert record["_aux"] == stored(3)
        assert record["reference_type"] == "my_cursor"
        assert "extra_data" not in record
        assert is_cursor_record(record, "my_cursor")
        assert not is_cursor_record(record)
        # another trigger can't pick up the cursor under the default settings
        assert await lamindb_cursor(hook, DEFAULTS, "nightly-watcher", "").load(INSTANCE.id) is None

    async def test_branch_and_space_by_name(self):
        hook = FakeHook(
            records={
                "core.branch": {7: {"id": 7, "name": "airflow"}},
                "core.space": {3: {"id": 3, "name": "Airflow"}},
            }
        )
        settings = RecordCursorStore(branch="airflow", space="Airflow")
        await lamindb_cursor(hook, settings, "k", "").save(3, INSTANCE.id)

        record_type, record = cursor_records(hook).values()
        assert (record["branch_id"], record["space_id"]) == (7, 3)
        assert (record_type["branch_id"], record_type["space_id"]) == (7, 3)
        assert await lamindb_cursor(hook, settings, "k", "").load(INSTANCE.id) == 3
        # the same record name in another space is another cursor
        assert await lamindb_cursor(hook, DEFAULTS, "k", "").load(INSTANCE.id) is None

    async def test_unknown_branch_or_space(self):
        hook = FakeHook()
        with pytest.raises(ValueError, match="Branch 'nope' does not exist"):
            await lamindb_cursor(hook, RecordCursorStore(branch="nope"), "k", "").load(INSTANCE.id)
        with pytest.raises(ValueError, match="Space 'nope' does not exist"):
            await lamindb_cursor(hook, RecordCursorStore(space="nope"), "k", "").load(INSTANCE.id)

    async def test_json_encoded_field(self):
        hook = FakeHook()
        cursor_store = lamindb_cursor(hook, DEFAULTS, "k", "")
        await cursor_store.save(3, INSTANCE.id)
        record = cursor_records(hook)[2]
        record["extra_data"] = json.dumps(stored(4))
        assert await lamindb_cursor(hook, DEFAULTS, "k", "").load(INSTANCE.id) == 4
        record["extra_data"] = "not json"
        assert await lamindb_cursor(hook, DEFAULTS, "k", "").load(INSTANCE.id) is None

    def test_coerce(self):
        assert CursorStore.coerce(None) == AssetCursorStore()
        assert CursorStore.coerce({"type": "record", "space": 3}) == RecordCursorStore(space=3)
        assert CursorStore.coerce(DEFAULTS.serialize()) == DEFAULTS
        assert CursorStore.coerce(AssetCursorStore(commit_delay=1).serialize()) == AssetCursorStore(
            commit_delay=1
        )
        with pytest.raises(ValueError, match=r"Unknown RecordCursorStore settings: \['spaces'\]"):
            CursorStore.coerce({"type": "record", "spaces": 3})
        with pytest.raises(ValueError, match="type must be one of \\['asset', 'record'\\]"):
            CursorStore.coerce({"space": 3})
        with pytest.raises(TypeError):
            CursorStore.coerce("lamindb")
        with pytest.raises(TypeError):
            RecordCursorStore("core.record")  # keyword-only
