LaminDB Triggers: event-driven scheduling
=========================================

The provider's event triggers let DAGs run when something changes in a LaminDB instance, using
Airflow's `event-driven scheduling
<https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html>`__:
attach a trigger to an :class:`~airflow.sdk.Asset` through an :class:`~airflow.sdk.AssetWatcher`,
and schedule DAGs on that asset.

How it works
------------

LaminHub logs every ``INSERT``, ``UPDATE`` and ``DELETE`` on an instance's database in the
``hubmodule.dbwrite`` registry, shown as *Changes → Database writes* in the LaminHub UI. The triggers poll
this log through the LaminHub REST API. Each log entry has a strictly increasing id that serves as a
cursor, and each entry records which record was written on which branch, along with the previous values
of the changed fields. From this, the triggers derive semantic events such as "artifact created on
``main``" or "branch moved from ``review`` to ``merged``".

* **Only hosted instances.** The database write log is a LaminHub feature. Instances that are not
  managed by LaminHub are not supported.
* **Resuming after restarts.** The cursor is stored (see `Where the cursor is stored`_), so a restarted
  triggerer resumes where it left off. It's only saved when the trigger processed new writes that
  match its filter, at most once per poll. On the very first start, a trigger only reports changes
  from then on.
* **At-least-once delivery.** The cursor is committed one poll cycle (at least the store's
  ``commit_delay``, by default 5 seconds) after the events were handed to Airflow, so events are not lost if the triggerer crashes. After a crash an
  event may be delivered twice. Each payload contains the ``dbwrite`` entry, whose ``id``/``uid``
  identify the change, so DAGs can deduplicate if needed.
* **Batching.** Every change is a separate trigger event (and asset event). Airflow may combine
  several asset events into one DAG run; iterate over ``triggering_asset_events``.
* **Changing a trigger resets its cursor.** The state key is derived from the trigger's arguments
  (except the polling settings), so changing a filter starts a fresh cursor.

.. note::

    Define a watched asset in a single DAG file. Airflow registers watchers per DAG file. If another DAG
    file defines the same asset without watchers, the DAG processor keeps removing the watcher again.
    In other DAG files, refer to the asset with ``Asset.ref(name=...)``.

Common arguments
----------------

All event triggers accept:

``lamindb_conn_id``
    The :ref:`LaminDB connection <howto/connection:lamindb>` (default ``lamindb_default``).
``instance``
    Instance ``owner/name``; overrides the connection's instance.
``poll_interval``
    Seconds between polls (default 30).
``batch_size`` / ``max_batches_per_poll``
    Paging of the write log (defaults 200 and 10).
``cursor_store``
    Where and how the cursor is stored: ``AssetCursorStore()`` (default) or ``RecordCursorStore()``;
    see `Where the cursor is stored`_.

Where the cursor is stored
--------------------------

Pass a trigger a cursor store from :mod:`airflow.providers.lamindb.triggers.cursors` as ``cursor_store``.
Both take ``commit_delay``: the minimum seconds between handing events to Airflow and committing the
cursor past them (default 5). Longer delays save the cursor less often, but redeliver more events after
a crash.

``AssetCursorStore()`` (default)
    In the watched asset's *asset state store*, which Airflow 3.3 added for event-driven scheduling.
    The cursor lives in Airflow's metadata database next to the asset events, so restoring that
    database also restores matching cursors. A trigger outside an ``AssetWatcher`` has no asset state
    store and keeps its cursor in memory only.

``RecordCursorStore()``
    In a record of the watched instance, one per trigger. By default it's a ``core.record`` on ``main``,
    named after the trigger's state key and grouped under the record type ``Airflow trigger cursors``.
    The cursor is in the record's ``extra_data``, and the description says which trigger it belongs to.
    Use it for triggers outside an ``AssetWatcher``, or to keep cursors when Airflow's metadata
    database is reset. Keep in mind:

    * The connection's API key needs write access to the instance, in the cursor records' space. The
      record type and record are created on the first save.
    * Each save is a database write, so it appears in the instance's history and write log. Triggers
      for artifacts, branches and other registries never see it. ``LaminDBRecordEventTrigger`` skips
      cursor records in the registry it watches: it doesn't report them, and it doesn't save its cursor
      after a batch of only cursor writes, so triggers never react to each other's saves.
    * The cursor outlives Airflow's metadata database. After restoring an older backup of that
      database, the trigger resumes from the newer cursor, and the asset events in between are not
      delivered again.
    * To reset the cursor, delete the record (or move it to the trash) and restart the triggerer. The
      trigger then starts again from the latest write.

Configure the cursor record with the arguments of
:class:`~airflow.providers.lamindb.triggers.cursors.RecordCursorStore`, all keyword-only and optional:

``registry`` (default ``core.record``)
    Registry of the cursor records. It needs the fields ``name``, ``branch``, ``space`` and the
    ``field`` below, plus ``reference_type`` and ``type``/``is_type`` unless those are turned off.
``field`` (default ``extra_data``)
    JSON field that holds the cursor, ``{"dbwrite_id": ..., "instance_id": ...}``.
``record_type`` (default ``Airflow trigger cursors``)
    Name of the record type that groups the cursor records. ``None`` for no type, for registries
    without types.
``reference_type`` (default ``airflow_trigger_cursor``)
    Marker that identifies cursor records. ``None`` for no marker; then record triggers on
    ``registry`` report the cursor records like any other record. A record trigger that watches
    ``registry`` itself needs a marker to recognize its own saves, so it rejects ``None``.
``branch`` (default ``main``)
    Branch of the cursor records and their type, by name or id.
``space`` (default ``None``: LaminHub's default space)
    Space of the cursor records and their type, by name or id, for example a restricted space that only
    the Airflow API key can write to.
``name`` (default ``None``: the trigger's state key)
    Name of the cursor record. A fixed name must be unique per trigger, or triggers overwrite each
    other's cursors.
``description`` (default ``None``: describes the trigger)
    Description of the cursor record.

.. code-block:: python

    from airflow.providers.lamindb.triggers.cursors import RecordCursorStore

    LaminDBArtifactEventTrigger(
        key_prefix="raw/",
        cursor_store=RecordCursorStore(space="Airflow", name="raw-artifacts-watcher", commit_delay=30),
    )

A trigger serializes its cursor store as a dict with a ``type`` key (``asset`` or ``record``) and the
settings, and accepts that dict as ``cursor_store`` too.

A record trigger recognizes cursor records by the ``registry`` and ``reference_type`` of its *own*
``RecordCursorStore``, and by the defaults if it stores its cursor in the asset state store.
Cursor records with other settings are ordinary records to it:

* A record trigger with the asset state store reports them as events (but never writes to LaminDB).
* Two record triggers with a ``RecordCursorStore`` that watch the same registry, and store their
  cursors there with different ``reference_type`` markers, report each other's saves and save again
  in response, every poll. Give them the same ``registry`` and ``reference_type``.

Changing ``branch``, ``space``, ``name``, ``registry`` or the markers points the trigger at another
record, so its cursor starts over from the latest write. The old record stays until you delete it.

Every event payload contains ``instance``, ``instance_id``, ``changed_by`` (``id``, ``handle`` and
``name`` of the LaminDB user) and ``dbwrite``: the write log entry with ``id``, ``uid``,
``event_type``, ``table_name``, ``sqlrecord_id``, ``created_at``, ``branch_id``, ``space_id`` and ``run_id``.
In a DAG, the payload is available as ``event.extra["payload"]``.

.. _howto/trigger:LaminDBArtifactEventTrigger:

Artifacts
---------

:class:`~airflow.providers.lamindb.triggers.records.LaminDBArtifactEventTrigger` reports artifacts that
are ``created``, ``updated`` or ``deleted`` (argument ``events``, default ``["created"]``). Filter them
with ``key``, ``key_prefix``, ``suffix``, ``kind`` and a general :doc:`filter <filters>`.

* With ``wait_for_upload=True`` (default), ``created`` fires only once the artifact's upload to
  storage has completed. LaminDB creates the artifact record before it uploads the file and deletes
  the record again if the upload fails. Downstream tasks can load the artifact right away.
* Artifacts that LaminDB creates internally (run logs, environments) are ignored unless
  ``include_internal=True``.

.. literalinclude:: ../tests/system/lamindb/example_lamindb_event_scheduling.py
    :language: python
    :start-after: [START howto_trigger_lamindb_artifact_events]
    :end-before: [END howto_trigger_lamindb_artifact_events]

.. _howto/trigger:LaminDBRecordEventTrigger:

Records of any registry
-----------------------

:class:`~airflow.providers.lamindb.triggers.records.LaminDBRecordEventTrigger` watches any registry,
e.g. ``core.collection``, ``core.run``, ``core.transform``, ``core.record``, ``core.ulabel`` or
``bionty.celltype``.

Branch-aware semantics (``branch``, default ``main``)
    ``created`` covers records inserted on the branch *and* records that arrive on it by merging a
    contribution branch or by restoring them from the trash or archive. ``updated`` covers changes of
    records on the branch. ``deleted`` covers records deleted from the branch *and* records moved off
    it, e.g. to the trash by ``record.delete()``. With ``branch=None``, the trigger reports raw inserts,
    updates and deletes on all branches. For registries without branches, ``branch`` is ignored.

Filters
    ``filter`` is a :doc:`filter <filters>` such as
    ``F(CollectionField.KEY).startswith("raw/")`` or
    ``F(CollectionField.CREATED_BY, UserField.HANDLE) == "alice"``, or its `LaminHub REST
    <https://docs.lamin.ai/rest>`__ form such as ``{"created_by.handle": {"eq": "alice"}}``. The API
    evaluates it against the current record. Hard-deleted records can't be queried anymore, so
    ``deleted`` events only support filters on direct fields of the record, such as
    ``F(CollectionField.CREATED_BY_ID) == 7``.

Updates
    ``changed_fields`` restricts ``updated`` events to changes of certain fields, e.g.
    ``[CollectionField.KEY]``.

Payload
    ``event``, ``registry``, ``table_name``, ``record_id``, ``record`` (the current record or
    ``None`` for hard deletes), ``previous`` (the previous values of the changed fields, or the full
    deleted row) and ``changed_fields``.

.. literalinclude:: ../tests/system/lamindb/example_lamindb_event_scheduling.py
    :language: python
    :start-after: [START howto_trigger_lamindb_record_events]
    :end-before: [END howto_trigger_lamindb_record_events]

.. _howto/trigger:LaminDBBranchStatusEventTrigger:

Branches: status changes, comments and readmes
----------------------------------------------

:class:`~airflow.providers.lamindb.triggers.branches.LaminDBBranchStatusEventTrigger` reports status
changes of branches (Change Requests): ``standalone``, ``draft``, ``review``, ``merged`` and ``closed``.
Filter with ``from_status``, ``to_status`` and ``branch_name`` (glob patterns such as
``"release-*"``). The payload contains ``from_status``, ``to_status`` and the ``branch`` with its current
``status``.

:class:`~airflow.providers.lamindb.triggers.branches.LaminDBBranchBlockEventTrigger` reports new
comments and readmes on branches (``kinds``, ``branch_name``). The payload contains the ``kind``, the
``block`` with its Markdown ``content``, and the ``branch``.

.. literalinclude:: ../tests/system/lamindb/example_lamindb_event_scheduling.py
    :language: python
    :start-after: [START howto_trigger_lamindb_branch_events]
    :end-before: [END howto_trigger_lamindb_branch_events]
