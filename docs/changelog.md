# Changelog

## 0.0.1

Initial release.

- Supports lamindb 2.8.1 and newer only. The hook, sensors and lineage tasks reject instances on older
  lamindb with `LaminDBVersionError`, and event triggers don't poll them. An older `lamindb_version`
  fails when the DAG is parsed, and a lineage task whose virtualenv or pod has an older lamindb fails
  when it starts.
- Lineage: DAG runs are recorded as LaminDB flow runs and tasks as steps with
  `LaminDBFlowInitOperator`, `LaminDBFlowFinishOperator` and the `@task.lamindb_venv` and
  `@task.lamindb_k8s` decorators. LaminDB runs in a virtualenv or a Kubernetes pod, never on the
  worker. The virtualenvs install `lamindb-core` in the instance's version (with pandas below 3
  for lamindb before 2.10) and get their credentials from the `lamindb` connection.
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
