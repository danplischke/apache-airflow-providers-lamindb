from __future__ import annotations

import pytest

from airflow.providers.lamindb.utils.dbwrite import (
    changed_fields,
    classify_record_write,
    dbwrite_summary,
    is_branch_move,
    is_upload_completion,
    previous_values,
    resolve_status_transitions,
)
from airflow.providers.lamindb.utils.lamindb import (
    artifact_filter,
    branch_status,
    is_internal_artifact,
    normalize_statuses,
    parse_json_value,
    storage_ongoing,
)
from unit.lamindb.fakes import dbwrite

MAIN = 1
TRASH = -1
FEATURE = 42


class TestClassifyRecordWrite:
    @pytest.mark.parametrize(
        ("write", "expected"),
        [
            (dbwrite(1, "INSERT", "t", 1, branch_id=MAIN), "created"),
            (dbwrite(1, "INSERT", "t", 1, branch_id=FEATURE), None),
            (dbwrite(1, "UPDATE", "t", 1, data={"key": "old"}, branch_id=MAIN), "updated"),
            (dbwrite(1, "UPDATE", "t", 1, data={"key": "old"}, branch_id=FEATURE), None),
            # merge of a contribution branch into main
            (dbwrite(1, "UPDATE", "t", 1, data={"branch_id": FEATURE}, branch_id=MAIN), "created"),
            # restore from trash
            (dbwrite(1, "UPDATE", "t", 1, data={"branch_id": TRASH}, branch_id=MAIN), "created"),
            # record.delete() moves the record to the trash
            (dbwrite(1, "UPDATE", "t", 1, data={"branch_id": MAIN}, branch_id=TRASH), "deleted"),
            # moves between other branches
            (dbwrite(1, "UPDATE", "t", 1, data={"branch_id": FEATURE}, branch_id=TRASH), None),
            (dbwrite(1, "DELETE", "t", 1, data={"id": 1, "branch_id": MAIN}, branch_id=MAIN), "deleted"),
            # hard delete of a record that is in the trash already
            (dbwrite(1, "DELETE", "t", 1, data={"id": 1, "branch_id": TRASH}, branch_id=TRASH), None),
        ],
    )
    def test_branch_aware(self, write, expected):
        assert classify_record_write(write, MAIN) == expected

    @pytest.mark.parametrize(
        ("event_type", "expected"), [("INSERT", "created"), ("UPDATE", "updated"), ("DELETE", "deleted")]
    )
    def test_raw(self, event_type, expected):
        write = dbwrite(1, event_type, "t", 1, data={"branch_id": MAIN}, branch_id=TRASH)
        assert classify_record_write(write, None) == expected

    def test_unknown_event_type(self):
        assert classify_record_write(dbwrite(1, "TRUNCATE", "t", 1), MAIN) is None
        assert classify_record_write(dbwrite(1, "TRUNCATE", "t", 1), None) is None


def test_write_helpers():
    update = dbwrite(3, "UPDATE", "t", 1, data={"size": 1, "hash": "x"})
    assert changed_fields(update) == ["hash", "size"]
    assert previous_values(update) == {"size": 1, "hash": "x"}
    assert changed_fields(dbwrite(3, "DELETE", "t", 1, data={"id": 1})) == []
    assert previous_values(dbwrite(3, "INSERT", "t", 1)) == {}
    assert dbwrite_summary(update) == {
        "id": 3,
        "uid": "uid3",
        "event_type": "UPDATE",
        "table_name": "t",
        "sqlrecord_id": 1,
        "created_at": "2026-09-30T08:00:00+00:00",
        "created_by_id": 7,
        "branch_id": 1,
        "space_id": 1,
        "run_id": None,
    }
    assert is_branch_move(dbwrite(1, "UPDATE", "t", 1, data={"branch_id": 5}, branch_id=1))
    assert not is_branch_move(dbwrite(1, "UPDATE", "t", 1, data={"key": 5}, branch_id=1))


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        ({"_aux": '{"so": 1}'}, True),
        ({"_aux": '{"so": 1, "ss": 1}'}, True),
        ({"_aux": {"so": 1}}, True),
        ({"_aux": "null"}, False),
        ({"_aux": '{"ss": 1}'}, False),
        ({"key": "x"}, False),
    ],
)
def test_is_upload_completion(data, expected):
    assert is_upload_completion(dbwrite(1, "UPDATE", "lamindb_artifact", 1, data=data)) is expected


def test_is_upload_completion_requires_update():
    assert not is_upload_completion(dbwrite(1, "DELETE", "lamindb_artifact", 1, data={"_aux": '{"so": 1}'}))


def test_resolve_status_transitions():
    writes = [
        dbwrite(10, "UPDATE", "lamindb_branch", 5, data={"_status_code": 0}),  # 0 -> 1
        dbwrite(11, "UPDATE", "lamindb_branch", 6, data={"name": "x"}),  # not a status change
        dbwrite(12, "UPDATE", "lamindb_branch", 5, data={"_status_code": 1}),  # 1 -> 2 (next write after)
        dbwrite(13, "UPDATE", "lamindb_branch", 6, data={"_status_code": 2}),  # 2 -> -1 (current)
        dbwrite(14, "UPDATE", "lamindb_branch", 7, data={"_status_code": 2}),  # 2 -> ? (deleted)
    ]
    transitions = resolve_status_transitions(writes, later_codes={5: 2}, current_codes={5: -1, 6: -1})
    assert [(w["id"], prev, new) for w, prev, new in transitions] == [
        (10, 0, 1),
        (12, 1, 2),
        (13, 2, -1),
        (14, 2, None),
    ]


class TestLamindbSemantics:
    def test_branch_status(self):
        assert [branch_status(code) for code in (-2, -1, 0, 1, 2, None, 99)] == [
            "closed",
            "merged",
            "standalone",
            "draft",
            "review",
            "standalone",
            "standalone",
        ]

    def test_normalize_statuses(self):
        assert normalize_statuses(None, name="x") is None
        assert normalize_statuses("merged", name="x") == ["merged"]
        assert normalize_statuses(("review", "merged"), name="x") == ["review", "merged"]
        with pytest.raises(ValueError, match="Invalid branch status"):
            normalize_statuses(["approved"], name="x")

    def test_json_values(self):
        assert parse_json_value('{"so": 1}') == {"so": 1}
        assert parse_json_value("not json") == "not json"
        assert parse_json_value(None) is None
        assert storage_ongoing({"so": 1})
        assert not storage_ongoing(None)

    def test_is_internal_artifact(self):
        assert is_internal_artifact({"kind": "__lamindb_run__"})
        assert not is_internal_artifact({"kind": "dataset"})
        assert not is_internal_artifact({"kind": None})

    def test_artifact_filter(self):
        assert artifact_filter(include_internal=True) is None
        assert artifact_filter() == {
            "or": [{"kind": {"isnull": True}}, {"kind": {"notin": ["__lamindb_run__", "__lamindb_config__"]}}]
        }
        assert artifact_filter(
            key_prefix="raw/", suffix=".csv", kind="dataset", extra={"size": {"gt": 0}}
        ) == {
            "and": [
                {"size": {"gt": 0}},
                {"key": {"startswith": "raw/"}},
                {"suffix": {"in": [".csv"]}},
                {"kind": {"in": ["dataset"]}},
            ]
        }
        assert artifact_filter(key="a.csv", include_internal=True) == {"key": {"eq": "a.csv"}}
