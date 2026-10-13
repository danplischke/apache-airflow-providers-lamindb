from __future__ import annotations

from typing import Any

import pytest

from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger
from airflow.providers.lamindb.triggers.replay import (
    MAX_REPLAY_DBWRITE_IDS,
    ReplayRequest,
    replay_key,
    request_replay,
)


class TaskStateStore:
    """Stand-in for the synchronous asset state store of a task with one asset."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value


class TestReplayRequest:
    @pytest.mark.parametrize(
        ("kwargs", "value"),
        [
            ({"from_dbwrite_id": 5}, {"from_dbwrite_id": 5}),
            ({"from_dbwrite_id": 5, "to_dbwrite_id": 5}, {"from_dbwrite_id": 5, "to_dbwrite_id": 5}),
            ({"dbwrite_ids": (3, 1)}, {"dbwrite_ids": [3, 1]}),
            ({"id": "run-1", "from_dbwrite_id": 1}, {"id": "run-1", "from_dbwrite_id": 1}),
        ],
    )
    def test_valid(self, kwargs, value):
        request = ReplayRequest(**kwargs)
        assert request.to_value() == value
        assert ReplayRequest.from_value(value) == request

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({}, "from_dbwrite_id must be a positive integer"),
            ({"from_dbwrite_id": 0}, "from_dbwrite_id must be a positive integer"),
            ({"from_dbwrite_id": True}, "from_dbwrite_id must be a positive integer"),
            ({"from_dbwrite_id": "5"}, "from_dbwrite_id must be a positive integer"),
            ({"from_dbwrite_id": 5, "to_dbwrite_id": 4}, "to_dbwrite_id 4 is lower than from_dbwrite_id 5"),
            ({"from_dbwrite_id": 5, "to_dbwrite_id": 5.5}, "to_dbwrite_id must be a positive integer"),
            ({"dbwrite_ids": ()}, "non-empty list"),
            ({"dbwrite_ids": (1, -2)}, r"positive integers, got \[-2\]"),
            ({"dbwrite_ids": (1,), "from_dbwrite_id": 1}, "not both"),
            ({"dbwrite_ids": (1,) * (MAX_REPLAY_DBWRITE_IDS + 1)}, "replay a range instead"),
            ({"id": "", "from_dbwrite_id": 1}, "id must be a string"),
            ({"id": 7, "from_dbwrite_id": 1}, "id must be a string"),
        ],
    )
    def test_invalid(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            ReplayRequest(**kwargs)

    def test_from_value_ignores_status_fields(self):
        value = {"id": "r", "dbwrite_ids": [2], "status": "done", "events": 1, "finished_at": "2026-10-13"}
        assert ReplayRequest.from_value(value) == ReplayRequest(id="r", dbwrite_ids=(2,))

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            ("everything", "must be a JSON object"),
            ({"from": 1}, r"Unknown replay request fields \['from'\]"),
            ({"dbwrite_ids": "1,2"}, "non-empty list"),
        ],
    )
    def test_from_value_rejects(self, value, message):
        with pytest.raises(ValueError, match=message):
            ReplayRequest.from_value(value)


class TestRequestReplay:
    def test_stores_the_request_under_the_triggers_key(self):
        trigger = LaminDBArtifactEventTrigger(key_prefix="raw/")
        store = TaskStateStore()

        value = request_replay(store, trigger, from_dbwrite_id=10, to_dbwrite_id=20, request_id="run-1")

        assert value == {"id": "run-1", "from_dbwrite_id": 10, "to_dbwrite_id": 20}
        assert store.data == {f"{trigger.state_key}.replay": value}
        assert replay_key(trigger.state_key) == f"{trigger.state_key}.replay"

    def test_accepts_the_state_key_and_any_iterable_of_ids(self):
        store = TaskStateStore()

        request_replay(store, "lamindb.Trigger.abc", dbwrite_ids=iter([4, 2]))

        assert store.data == {"lamindb.Trigger.abc.replay": {"dbwrite_ids": [4, 2]}}

    def test_rejects_invalid_requests_before_storing(self):
        store = TaskStateStore()
        with pytest.raises(ValueError, match="lower than"):
            request_replay(store, "key", from_dbwrite_id=5, to_dbwrite_id=1)
        assert store.data == {}
