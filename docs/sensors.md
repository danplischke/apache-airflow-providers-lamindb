# LaminDB Sensors

The sensors wait for a condition inside a running DAG. They support `deferrable=True` (or
`[operators] default_deferrable`), which moves the wait to the triggerer so that no worker slot is
occupied. For scheduling DAGs *on* LaminDB changes, use the [event triggers](triggers.md) instead.

Network errors, rate limiting (429) and LaminHub server errors (5xx) don't end the wait: the sensors
log them and check again at the next poke, until their `timeout`. Other errors fail the task, for
example an invalid filter, a missing permission or an instance on lamindb older than 2.11.

## LaminDBBranchStatusSensor

[`LaminDBBranchStatusSensor`][airflow.providers.lamindb.sensors.branches.LaminDBBranchStatusSensor]
waits until a branch reaches a status (`target_status`, default `merged`), for example until a Change
Request is merged. If the branch reaches a `failure_status` (default `closed`), the task fails without
retries. The branch, with its `status`, is returned via XCom.

```python title="tests/system/lamindb/example_lamindb_sensors.py"
--8<-- "tests/system/lamindb/example_lamindb_sensors.py:howto_sensor_lamindb_branch_status"
```

## LaminDBRecordSensor

[`LaminDBRecordSensor`][airflow.providers.lamindb.sensors.records.LaminDBRecordSensor] waits until at
least `min_count` records of a registry match a [filter](filters.md) on a branch (`branch`, default
`main`). The matching records are returned via XCom: at most `limit` records, newest first.

```python title="tests/system/lamindb/example_lamindb_sensors.py"
--8<-- "tests/system/lamindb/example_lamindb_sensors.py:howto_sensor_lamindb_record"
```

The same filter as a [LaminHub REST filter](https://docs.lamin.ai/rest):
`{"and": [{"transform.key": {"eq": "preprocess.py"}}, {"_status_code": {"eq": 0}}]}`.

## LaminDBArtifactSensor

[`LaminDBArtifactSensor`][airflow.providers.lamindb.sensors.records.LaminDBArtifactSensor] waits for
artifacts by `key`, `key_prefix`, `suffix` or `kind` (e.g. `ArtifactKind.DATASET`) and an additional
[filter](filters.md), and ignores artifacts whose upload to storage is still in progress.

```python title="tests/system/lamindb/example_lamindb_sensors.py"
--8<-- "tests/system/lamindb/example_lamindb_sensors.py:howto_sensor_lamindb_artifact"
```

## LaminDBHook

[`LaminDBHook`][airflow.providers.lamindb.hooks.lamindb.LaminDBHook] can be used in tasks to query the
LaminHub REST API directly. Every method also has an async variant with an `a` prefix, such as
`aquery_records`:

- `query_records(registry, filter, order_by=..., limit=...)`: query any registry (paginated) with a
  [filter](filters.md)
- `get_records_by_ids(registry, ids)`
- `get_branch(name_or_id)`
- `query_dbwrites(filter, after_id=...)`: the database write log
- `get_instance()` and `get_schema()`
- `insert_records(registry, records)` and `update_record(registry, uid, values)`: write records; the
  API key needs write access to the instance. Inserts are not retried after errors that may have reached
  LaminHub, so a failed request never creates a record twice.

```python title="tests/system/lamindb/example_lamindb_sensors.py"
--8<-- "tests/system/lamindb/example_lamindb_sensors.py:howto_hook_lamindb"
```
