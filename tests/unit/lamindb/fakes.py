from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from airflow.providers.lamindb.exceptions import LaminDBApiError
from airflow.providers.lamindb.hooks.lamindb import LaminDBInstance, Registry, resolve_registry

INSTANCE = LaminDBInstance(
    owner="owner", name="name", id="instance-uuid", api_url="https://api.example.com/api"
)

SCHEMA: dict[str, Any] = {
    "core": {
        "artifact": {"table_name": "lamindb_artifact", "fields": {"id": {}, "key": {}, "branch": {}}},
        "branch": {"table_name": "lamindb_branch", "fields": {"id": {}, "name": {}}},
        "branchblock": {"table_name": "lamindb_branchblock", "fields": {"id": {}, "branch": {}}},
        "record": {
            "table_name": "lamindb_record",
            "fields": {"id": {}, "name": {}, "type": {}, "branch": {}},
        },
        "ulabel": {"table_name": "lamindb_ulabel", "fields": {"id": {}, "name": {}, "branch": {}}},
        "user": {"table_name": "lamindb_user", "fields": {"id": {}, "handle": {}}},
        "space": {"table_name": "lamindb_space", "fields": {"id": {}, "name": {}}},
    },
    "bionty": {"gene": {"table_name": "bionty_gene", "fields": {"id": {}, "symbol": {}, "branch": {}}}},
}

_JSON_PATH = re.compile(r'^data\["(?P<key>[^"]+)"\]$')
_MISSING = object()


def _field(row: Mapping[str, Any], path: str) -> Any:
    if match := _JSON_PATH.match(path):
        return (row.get("data") or {}).get(match["key"])
    if "." in path:
        relation, _, attr = path.partition(".")
        related = row.get(relation)
        return related.get(attr) if isinstance(related, Mapping) else _MISSING
    return row.get(path)


def evaluate(node: Mapping[str, Any] | None, row: Mapping[str, Any]) -> bool:
    """Evaluate a LaminHub REST filter like the server does, including ``data["key"]`` JSON paths."""
    if not node:
        return True
    ((key, value),) = node.items()
    if key == "and":
        return all(evaluate(child, row) for child in value)
    if key == "or":
        return any(evaluate(child, row) for child in value)
    field = _field(row, key)
    ((operator, operand),) = value.items()
    if field is _MISSING:
        return False
    if operator == "eq":
        return bool(field == operand)
    if operator == "ne":
        return bool(field != operand)
    if operator == "in":
        return field in operand
    if operator == "notin":
        return field is not None and field not in operand
    if operator == "isnull":
        return (field is None) == operand
    if operator == "gt":
        return field is not None and field > operand
    if operator == "lt":
        return field is not None and field < operand
    if operator == "startswith":
        return isinstance(field, str) and field.startswith(operand)
    raise NotImplementedError(operator)


def dbwrite(
    id: int,
    event_type: str,
    table_name: str,
    sqlrecord_id: int,
    *,
    data: dict[str, Any] | None = None,
    branch_id: int | None = 1,
    created_by_id: int | None = 7,
) -> dict[str, Any]:
    return {
        "id": id,
        "uid": f"uid{id}",
        "event_type": event_type,
        "table_name": table_name,
        "sqlrecord_id": sqlrecord_id,
        "data": data,
        "branch_id": branch_id,
        "space_id": 1,
        "run_id": None,
        "created_at": "2026-09-30T08:00:00+00:00",
        "created_by_id": created_by_id,
    }


class FakeHook:
    """In-memory stand-in for the async API of ``LaminDBHook`` used by the triggers."""

    instance_slug = "owner/name"

    def __init__(
        self,
        *,
        writes: Iterable[dict[str, Any]] = (),
        records: dict[str, dict[int, dict[str, Any]]] | None = None,
        users: dict[int, dict[str, Any]] | None = None,
        latest_dbwrite_id: int | None = None,
    ) -> None:
        self.writes = sorted(writes, key=lambda w: w["id"])
        self.records = records or {}
        self.users = users if users is not None else {7: {"id": 7, "handle": "alice", "name": "Alice"}}
        self.latest_dbwrite_id = latest_dbwrite_id
        self.dbwrite_queries: list[dict[str, Any]] = []
        self.closed = False
        self.fail_next: list[BaseException] = []
        self.updates: list[tuple[str, str, dict[str, Any]]] = []

    def _maybe_fail(self) -> None:
        if self.fail_next:
            raise self.fail_next.pop(0)

    async def aget_instance(self) -> LaminDBInstance:
        self._maybe_fail()
        return INSTANCE

    async def aget_schema(self) -> dict[str, Any]:
        return SCHEMA

    async def aget_registry(self, registry: str | Registry) -> Registry:
        return resolve_registry(SCHEMA, registry)

    async def aget_latest_dbwrite_id(self) -> int:
        if self.latest_dbwrite_id is not None:
            return self.latest_dbwrite_id
        return max((w["id"] for w in self.writes), default=0)

    async def aquery_dbwrites(
        self,
        filter: Mapping[str, Any] | None = None,
        *,
        after_id: int | None = None,
        limit: int = 200,
        descending: bool = False,
    ) -> list[dict[str, Any]]:
        self._maybe_fail()
        self.dbwrite_queries.append({"filter": filter, "after_id": after_id, "limit": limit})
        rows = [w for w in self.writes if (after_id is None or w["id"] > after_id) and evaluate(filter, w)]
        if descending:
            rows.reverse()
        return rows[:limit]

    async def aget_records_by_ids(
        self, registry: str | Registry, ids: Iterable[int], filter: Mapping[str, Any] | None = None
    ) -> dict[int, dict[str, Any]]:
        name = registry.name if isinstance(registry, Registry) else registry
        table = self.records.get(name, {})
        return {i: table[i] for i in ids if i in table and evaluate(filter, table[i])}

    async def aquery_records(
        self,
        registry: str | Registry,
        filter: Mapping[str, Any] | None = None,
        *,
        order_by: Any = None,
        limit: int = 50,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self._maybe_fail()
        name = registry.name if isinstance(registry, Registry) else registry
        rows = [r for r in self.records.get(name, {}).values() if evaluate(filter, r)]
        rows.sort(key=lambda r: r["id"], reverse=order_by == ["-id"])
        return rows[:limit]

    async def aget_users(self, ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        return {i: self.users[i] for i in ids if i in self.users}

    def _log_write(self, event_type: str, registry: str, record_id: int, data: dict[str, Any] | None) -> None:
        """Log a write in the database write log, like LaminHub does for every write."""
        write_id = max((w["id"] for w in self.writes), default=0) + 1
        table_name = resolve_registry(SCHEMA, registry).table_name
        self.writes.append(dbwrite(write_id, event_type, table_name, record_id, data=data))

    async def ainsert_records(
        self, registry: str | Registry, records: Iterable[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        name = registry.name if isinstance(registry, Registry) else registry
        return [self.insert(name, values) for values in records]

    def insert(self, registry: str, values: Mapping[str, Any]) -> dict[str, Any]:
        """Create a record and log its ``INSERT``; what a user's write looks like to the triggers."""
        table = self.records.setdefault(registry, {})
        record_id = max(table, default=0) + 1
        table[record_id] = record = {"branch_id": 1, **values, "id": record_id, "uid": f"r{record_id}"}
        self._log_write("INSERT", registry, record_id, None)
        return dict(record)

    async def aupdate_record(
        self, registry: str | Registry, uid: str, values: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        name = registry.name if isinstance(registry, Registry) else registry
        record = next((r for r in self.records.get(name, {}).values() if r["uid"] == uid), None)
        if record is None:
            raise LaminDBApiError(f"{name} {uid} not found", http_status_code=404)
        previous = {key: record.get(key) for key in values}
        record.update(values)
        self._log_write("UPDATE", name, record["id"], previous)
        self.updates.append((name, uid, dict(values)))
        return dict(record)

    async def aget_branch(self, branch: str | int) -> dict[str, Any] | None:
        self._maybe_fail()
        for record in self.records.get("core.branch", {}).values():
            if record["id"] == branch or record["name"] == branch:
                return record
        return None

    async def aclose(self) -> None:
        self.closed = True


class FakeStateStore:
    """Stand-in for a single-asset ``AssetStateStoreAccessors``."""

    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self.data: dict[str, Any] = dict(initial or {})
        self.writes: list[tuple[str, Any]] = []

    async def aget(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    async def aset(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.writes.append((key, value))
