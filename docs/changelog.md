# Changelog

## 0.0.1

Initial release.

- Supports lamindb 2.8.1 and newer only. The hook, sensors and lineage tasks reject instances on older
  lamindb with `LaminDBVersionError`, and event triggers don't poll them. An older `lamindb_version`
  fails when the DAG is parsed, and a lineage task whose virtualenv or pod has an older lamindb fails
  when it starts.
- Lineage: the `@task.lamindb_venv` and `@task.lamindb_k8s` decorators record task runs,
  optionally grouped under a parent flow with `LaminDBFlow`. `LaminDBFlowInitOperator` and
  `LaminDBFlowFinishOperator` provide the low-level lifecycle operations. LaminDB runs in a
  virtualenv or a Kubernetes pod, never on the
  worker. The virtualenvs install `lamindb-core` in the instance's version (with pandas below 3
  for lamindb before 2.10) and get their credentials from the `lamindb` connection.
- Lineage scoping: `@task.lamindb_venv` and `@task.lamindb_k8s` tasks are tracked standalone by
  default, with no parent flow run. `LaminDBFlow` (`from airflow.providers.lamindb import LaminDBFlow`)
  is an optional context manager that groups the tasks created inside it, mapped ones included, under
  a flow run per DAG run. **Migration:** `auto_flow` is removed and now fails with an error; tracked
  tasks are no longer parented implicitly by `with LaminDBFlowInitOperator() >> LaminDBFlowFinishOperator():`
  or by flow operators elsewhere in the DAG, which now warn with `LaminDBFlowWarning`. Wrap the
  tasks in `with LaminDBFlow(...):` instead; note that plain Airflow tasks created in the block become
  members too and wait for the flow init. `auto_flow` and `lamindb_scope` in `default_args` fail
  instead of being dropped. The flow operators remain exported as low-level building blocks. See
  [Migrating to LaminDBFlow](lineage.md#migrating-to-lamindbflow).
- The Kubernetes extra is `[k8s]`; the former `[cncf.kubernetes]` still installs the same provider.
- `LaminDBHook` and the `lamindb` connection type for the LaminHub REST API, shared by all
  operators, triggers and sensors. It queries and writes records (`insert_records`, `update_record`).
- Event triggers for event-driven scheduling based on the LaminHub database write log, with
  cursors in the asset state store: `LaminDBRecordEventTrigger`, `LaminDBArtifactEventTrigger`,
  `LaminDBBranchStatusEventTrigger` and `LaminDBBranchBlockEventTrigger`. With
  `cursor_store=RecordCursorStore()`, the cursors are kept in records of the watched instance
  instead.
- Deferrable sensors: `LaminDBBranchStatusSensor`, `LaminDBRecordSensor` and `LaminDBArtifactSensor`.
  They keep waiting through network errors, rate limiting and LaminHub server errors.
- Filters built in Python with `F` and enums for registries (`LaminDBRegistry`), registry fields
  (`ArtifactField`, `RunField`, ...), operators (`FilterOperator`) and values (`ArtifactKind`,
  `TransformKind`, `RunStatus`). Filters are validated when a sensor, trigger or hook receives them.
