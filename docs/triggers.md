# LaminDB Triggers: event-driven scheduling

The provider's event triggers let DAGs run when something changes in a LaminDB instance, using
Airflow's [event-driven scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html):
attach a trigger to an [`Asset`][airflow.sdk.Asset] through an [`AssetWatcher`][airflow.sdk.AssetWatcher],
and schedule DAGs on that asset.

## How it works

LaminHub logs every `INSERT`, `UPDATE` and `DELETE` on an instance's database in the
`hubmodule.dbwrite` registry, shown as *Changes → Database writes* in the LaminHub UI. The triggers poll
this log through the LaminHub REST API. Each log entry has a strictly increasing id that serves as a
cursor, and each entry records which record was written on which branch, along with the previous values
of the changed fields. From this, the triggers derive semantic events such as "artifact created on
`main`" or "branch moved from `review` to `merged`".

- **Only hosted instances.** The database write log is a LaminHub feature. Instances that are not
  managed by LaminHub are not supported.
- **Resuming after restarts.** The cursor is stored (see [Where the cursor is stored](#where-the-cursor-is-stored)),
  so a restarted triggerer resumes where it left off. It's only saved when the trigger processed new
  writes that match its filter, at most once per poll. On the very first start, a trigger only reports
  changes from then on.
- **At-least-once delivery.** The cursor is committed past events only once they were handed to
  Airflow at least the store's `commit_delay` ago (by default 5 seconds), checked at the start of
  each poll, so at least one poll cycle later. Events are not lost if the triggerer crashes, but after
  a crash an event may be delivered twice. Each payload contains the `dbwrite` entry, whose
  `id`/`uid` identify the change, so DAGs can deduplicate if needed.
- **Replays.** A trigger in an `AssetWatcher` delivers the events of past changes again on request,
  without moving its cursor. See [Replaying events](#replaying-events).
- **Batching.** Every change is a separate trigger event (and asset event). Airflow may combine
  several asset events into one DAG run; iterate over `triggering_asset_events`. In Airflow 3.3,
  events that arrive while the scheduler creates a run, such as the rest of a burst of changes, can
  miss that run and only come with the run for the asset's next event.
- **Changing a trigger resets its cursor.** The state key is derived from the trigger's arguments
  except the polling settings (`poll_interval`, `batch_size`, `max_batches_per_poll`) and
  `cursor_store`, so changing a filter, the connection or the instance starts a fresh cursor.
  Changing `cursor_store` moves the cursor to another place; see
  [Where the cursor is stored](#where-the-cursor-is-stored).

!!! note

    Define a watched asset in a single DAG file. Airflow registers watchers per DAG file. If another DAG
    file defines the same asset without watchers, the DAG processor keeps removing the watcher again.
    In other DAG files, refer to the asset with `Asset.ref(name=...)`.

## Common arguments

All event triggers accept:

`lamindb_conn_id`
:   The [LaminDB connection](connections/lamindb.md) (default `lamindb_default`).

`instance`
:   Instance `owner/name`; overrides the connection's instance.

`poll_interval`
:   Seconds between polls (default 30).

`batch_size` / `max_batches_per_poll`
:   Paging of the write log (defaults 200 and 10).

`cursor_store`
:   Where and how the cursor is stored: `AssetCursorStore()` (default) or `RecordCursorStore()`;
    see [Where the cursor is stored](#where-the-cursor-is-stored).

## Where the cursor is stored

Pass a trigger a cursor store from
[`airflow.providers.lamindb.triggers.cursors`][airflow.providers.lamindb.triggers.cursors] as
`cursor_store`. Both take `commit_delay`: the minimum seconds between handing events to Airflow and
committing the cursor past them (default 5). Longer delays save the cursor less often, but redeliver
more events after a crash.

`AssetCursorStore()` (default)
:   In the watched asset's *asset state store*, which Airflow 3.3 added for event-driven scheduling.
    The cursor lives in Airflow's metadata database next to the asset events, so restoring that
    database also restores matching cursors. A trigger outside an `AssetWatcher` has no asset state
    store and keeps its cursor in memory only.

`RecordCursorStore()`
:   In a record of the watched instance, one per trigger. By default it's a `core.record` on `main`,
    named after the trigger's state key and grouped under the record type `Airflow trigger cursors`.
    The cursor is in the record's `extra_data`, and the description says which trigger it belongs to.
    Use it for triggers outside an `AssetWatcher`, or to keep cursors when Airflow's metadata
    database is reset. Keep in mind:

    - The connection's API key needs write access to the instance, in the cursor records' space. The
      record type and record are created on the first save.
    - Each save is a database write, so it appears in the instance's history and write log. Triggers
      for artifacts, branches and other registries never see it. `LaminDBRecordEventTrigger` skips
      cursor records in the registry it watches: it doesn't report them, and it doesn't save its cursor
      after a batch of only cursor writes, so triggers never react to each other's saves.
    - The cursor outlives Airflow's metadata database. After restoring an older backup of that
      database, the trigger resumes from the newer cursor, and the asset events in between are not
      delivered again.
    - To reset the cursor, delete the record (or move it to the trash) and restart the triggerer. The
      trigger then starts again from the latest write.

Configure the cursor record with the arguments of
[`RecordCursorStore`][airflow.providers.lamindb.triggers.cursors.RecordCursorStore], all keyword-only
and optional:

`registry` (default `core.record`)
:   Registry of the cursor records. It needs the fields `name`, `branch`, `space` and the
    `field` below, plus `reference_type` and `type`/`is_type` unless those are turned off.

`field` (default `extra_data`)
:   JSON field that holds the cursor, `{"dbwrite_id": ..., "instance_id": ...}`.

`record_type` (default `Airflow trigger cursors`)
:   Name of the record type that groups the cursor records. `None` for no type, for registries
    without types.

`reference_type` (default `airflow_trigger_cursor`)
:   Marker that identifies cursor records. `None` for no marker; then record triggers on
    `registry` report the cursor records like any other record. A record trigger that watches
    `registry` itself needs a marker to recognize its own saves, so it rejects `None`.

`branch` (default `main`)
:   Branch of the cursor records and their type, by name or id.

`space` (default `None`: LaminHub's default space)
:   Space of the cursor records and their type, by name or id, for example a restricted space that only
    the Airflow API key can write to.

`name` (default `None`: the trigger's state key)
:   Name of the cursor record. A fixed name must be unique per trigger, or triggers overwrite each
    other's cursors.

`description` (default `None`: describes the trigger)
:   Description of the cursor record.

```python
from airflow.providers.lamindb.triggers.cursors import RecordCursorStore

LaminDBArtifactEventTrigger(
    key_prefix="raw/",
    cursor_store=RecordCursorStore(space="Airflow", name="raw-artifacts-watcher", commit_delay=30),
)
```

A trigger serializes its cursor store as a dict with a `type` key (`asset` or `record`) and the
settings, and accepts that dict as `cursor_store` too.

A record trigger recognizes cursor records by the `registry` and `reference_type` of its *own*
`RecordCursorStore`, and by the defaults if it stores its cursor in the asset state store.
Cursor records with other settings are ordinary records to it:

- A record trigger with the asset state store reports them as events (but never writes to LaminDB).
- Two record triggers with a `RecordCursorStore` that watch the same registry, and store their
  cursors there with different `reference_type` markers, report each other's saves and save again
  in response, every poll. Give them the same `registry` and `reference_type`.

Changing `branch`, `space`, `name`, `registry` or the markers points the trigger at another
record, so its cursor starts over from the latest write. The old record stays until you delete it.

Every event payload contains `instance`, `instance_id`, `changed_by` (`id`, `handle` and
`name` of the LaminDB user) and `dbwrite`: the write log entry with `id`, `uid`,
`event_type`, `table_name`, `sqlrecord_id`, `created_at`, `branch_id`, `space_id` and `run_id`.
In a DAG, the payload is available as `event.extra["payload"]`.

## Artifacts

[`LaminDBArtifactEventTrigger`][airflow.providers.lamindb.triggers.records.LaminDBArtifactEventTrigger]
reports artifacts that are `created`, `updated` or `deleted` (argument `events`, default
`["created"]`). Filter them with `key`, `key_prefix`, `suffix`, `kind` and a general
[filter](filters.md).

- With `wait_for_upload=True` (default), `created` fires only once the artifact's upload to
  storage has completed. LaminDB creates the artifact record before it uploads the file and deletes
  the record again if the upload fails. Downstream tasks can load the artifact right away.
- Artifacts that LaminDB creates internally (run logs, environments) are ignored unless
  `include_internal=True`.

```python title="tests/system/lamindb/example_lamindb_event_scheduling.py"
--8<-- "tests/system/lamindb/example_lamindb_event_scheduling.py:howto_trigger_lamindb_artifact_events"
```

## Records of any registry

[`LaminDBRecordEventTrigger`][airflow.providers.lamindb.triggers.records.LaminDBRecordEventTrigger]
watches any registry, e.g. `core.collection`, `core.run`, `core.transform`, `core.record`,
`core.ulabel` or `bionty.celltype`.

Branch-aware semantics (`branch`, default `main`)
:   `created` covers records inserted on the branch *and* records that arrive on it by merging a
    contribution branch or by restoring them from the trash or archive. `updated` covers changes of
    records on the branch. `deleted` covers records deleted from the branch *and* records moved off
    it, e.g. to the trash by `record.delete()`. With `branch=None`, the trigger reports raw inserts,
    updates and deletes on all branches. For registries without branches, `branch` is ignored.

Filters
:   `filter` is a [filter](filters.md) such as `F(CollectionField.KEY).startswith("raw/")` or
    `F(CollectionField.CREATED_BY, UserField.HANDLE) == "alice"`, or its
    [LaminHub REST](https://docs.lamin.ai/rest) form such as `{"created_by.handle": {"eq": "alice"}}`.
    The API evaluates it against the current record. Hard-deleted records can't be queried anymore,
    so `deleted` events only support filters on direct fields of the record, such as
    `F(CollectionField.CREATED_BY_ID) == 7`.

Updates
:   `changed_fields` restricts `updated` events to changes of certain fields, e.g.
    `[CollectionField.KEY]`.

Payload
:   `event`, `registry`, `table_name`, `record_id`, `record` (the current record or
    `None` for hard deletes), `previous` (the previous values of the changed fields, or the full
    deleted row) and `changed_fields`.

```python title="tests/system/lamindb/example_lamindb_event_scheduling.py"
--8<-- "tests/system/lamindb/example_lamindb_event_scheduling.py:howto_trigger_lamindb_record_events"
```

## Branches: status changes, comments and readmes

[`LaminDBBranchStatusEventTrigger`][airflow.providers.lamindb.triggers.branches.LaminDBBranchStatusEventTrigger]
reports status changes of branches (Change Requests): `standalone`, `draft`, `review`, `merged` and
`closed`. Filter with `from_status`, `to_status` and `branch_name` (glob patterns such as
`"release-*"`). The payload contains `from_status`, `to_status` and the `branch` with its current
`status`.

[`LaminDBBranchBlockEventTrigger`][airflow.providers.lamindb.triggers.branches.LaminDBBranchBlockEventTrigger]
reports new comments and readmes on branches (`kinds`, `branch_name`). The payload contains the
`kind`, the `block` with its Markdown `content`, and the `branch`.

```python title="tests/system/lamindb/example_lamindb_event_scheduling.py"
--8<-- "tests/system/lamindb/example_lamindb_event_scheduling.py:howto_trigger_lamindb_branch_events"
```

## Processing events one by one

Airflow can batch several events into one DAG run. Map a task over the events to give each one
its own task instance, with its own retries and its own failure:

```python title="tests/system/lamindb/example_lamindb_event_scheduling.py"
--8<-- "tests/system/lamindb/example_lamindb_event_scheduling.py:howto_process_lamindb_events_one_by_one"
```

- An event that fails doesn't stop the others. Its task instance retries on its own and then
  fails, labeled with the event's write id, with the payload as its input. The failed task
  instances of a run are its dead letters.
- To reprocess a failed event, clear its task instance once the cause is fixed. Clearing the whole
  DAG run reprocesses all its events.
- To deliver a single event again without a replay, for example after its DAG run was deleted,
  create an asset event with the same payload through Airflow's REST API:
  `POST /api/v2/assets/events` with `{"asset_id": ..., "extra": {"payload": {...}}}`.
- Events can arrive twice, after a triggerer crash or in a replay. If processing an event twice
  does harm, deduplicate on `payload["dbwrite"]["id"]`.

## Replaying events

To deliver the events of past changes again, for example after fixing a bug in the DAG that
processed them, ask the trigger for a replay. It reads the requested database writes again and
delivers their events next to its live events. Its cursor doesn't move, so live events keep
flowing. Replayed events have `"replay": {"id": ...}` in their payload.

Ask for a replay from a task that has the watched asset as an inlet:

```python title="tests/system/lamindb/example_lamindb_event_scheduling.py"
--8<-- "tests/system/lamindb/example_lamindb_event_scheduling.py:howto_replay_lamindb_events"
```

[`request_replay`][airflow.providers.lamindb.triggers.replay.request_replay] stores the request in
the asset state store of the watched asset, under the trigger's `state_key` plus `.replay`. Pass it
the trigger from the `AssetWatcher` (or one with the same arguments) or its `state_key`. Without a
task, write the request through Airflow's REST API. `GET /api/v2/assets/{asset_id}/state-store`
lists the keys; the trigger's state key is the one that starts with `lamindb.` and the trigger's
class name.

```bash
curl -X PUT "$AIRFLOW_URL/api/v2/assets/$ASSET_ID/state-store/$STATE_KEY.replay" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"value": {"id": "fix-1234", "from_dbwrite_id": 1000, "to_dbwrite_id": 2000}}'
```

A request contains:

`from_dbwrite_id`, `to_dbwrite_id`
:   A range of write ids, both inclusive. `to_dbwrite_id` defaults to the last write the trigger
    has processed and is capped there: later writes are delivered as live events anyway.

`dbwrite_ids`
:   A list of write ids instead of a range, at most 10,000. For example the `dbwrite.id` of failed
    events.

`id` (optional)
:   Identifies the request in the replayed payloads.

How a replay runs:

- **Next to the live events.** The trigger looks for a request at every poll and delivers up to
  `max_batches_per_poll` batches of it per poll, after the live events.
- **Status.** The trigger records the outcome in the request: `status` is `running` once it picked
  the request up, then `done` with the number of `events`, `invalid` with an `error` for a request it
  can't read, or `failed` with an `error` if processing the writes failed. LaminHub API errors don't
  fail a replay; it continues at the next poll. Requests that are `done`, `invalid` or `failed` stay
  until they are replaced; write a new request to replay again.
- **One request at a time.** A new request replaces the previous one, and deleting the request
  stops a running replay. If the triggerer restarts during a replay, the replay starts over:
  replays are delivered at least once, too.
- **Current state.** Only writes that match the trigger's filters are replayed, and their events
  are derived again: payloads describe the records as they are now, and writes whose records no
  longer match the filters, or no longer exist, may not be delivered again.
- **Needs the asset state store.** Replays work for triggers in an `AssetWatcher`, with either
  cursor store.
