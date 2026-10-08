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
* **Resuming after restarts.** The cursor is stored in the watched asset's *asset state store*,
  which Airflow 3.3 added for event-driven scheduling. A restarted triggerer resumes where it left
  off. On the very first start, a trigger only reports changes from then on.
* **At-least-once delivery.** The cursor is committed one poll cycle (at least 5 seconds) after the
  events were handed to Airflow, so events are not lost if the triggerer crashes. After a crash an
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
