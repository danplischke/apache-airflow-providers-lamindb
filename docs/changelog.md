# Changelog

## 0.0.3

- `LaminDBSetup.requirements()` lists default requirements installed in the virtualenvs of LaminDB
  steps and flow operators. A task's own `requirements` take precedence for the same package; pod
  steps don't install them.
- `LaminDBSetup.venv_cache_path()` sets the folder Airflow caches those virtualenvs in, for tasks
  that don't set `venv_cache_path` themselves.

## 0.0.2

**Breaking:** the modules are reorganised by layer. Old import paths are removed; update imports as
follows.

- `client/` is the LaminHub REST client and query language. It does not depend on the Airflow
  task SDK.
  - `hooks.models` → `client.models`.
  - `utils.filters` → `client.filters`, `utils.enums` → `client.enums`, `utils.dbwrite` →
    `client.dbwrite`.
  - `utils.lamindb` is split: branch statuses → `client.branches`, artifact helpers
    (`artifact_filter`, `storage_ongoing`, `is_internal_artifact`) → `client.artifacts`,
    `normalize_str_list` → `client.enums`, `parse_json_value` → `client.dbwrite`, and
    `resolve_branch_id`/`resolve_space_id` → `triggers.cursors`.
  - From `hooks.lamindb`: `LaminDBInstance`, `parse_instance_slug`, `chunk_ids`, `MAX_PAGE_SIZE` and
    `DEFAULT_HUB_API_URL` → `client.client`; `Registry`, `DBWRITE_REGISTRY`, `resolve_registry` and
    `normalize_registry_name` → `client.registries`.
  - New `LaminHubClient` and `LaminHubConfig` in `client.client`.
- `LaminDBHook` delegates to a `LaminHubClient` (`get_client()`/`aget_client()`); its public query
  and write methods are unchanged. `get_conn()` now returns the `LaminHubClient`, not an
  `httpx.Client`; `get_async_conn()` is removed.
- `execution/` is what runs tasks in virtualenvs and pods. It replaces `utils.remote`, `utils.runtime`,
  `utils.models` and `utils.dag_run`:
  - `utils.remote` is split into `execution.setup` (`LaminDBSetup`; update `[lamindb] setup_class`
    subclasses), `execution.source`, `execution.requirements`, `execution.environment` and
    `execution.parameters`.
  - `utils.runtime` → `execution.runtime`, `utils.models` → `execution.config`,
    `utils.dag_run` → `execution.dag_run`.
- `RemoteLaminDBStepMixin`, `task_setting`, `check_same_setup`, `check_flow_instance` and
  `reject_auto_flow` → `operators.step`.
- `triggers.models` → `triggers.events`.

**Breaking:** filters and responses are typed.

- Filters are `FilterExpression`s only. Hook, client, sensor and trigger `filter` arguments, `&`/`|`,
  `And`/`Or`, `combine_filters` and `artifact_filter(extra=...)` raise a `TypeError` for dicts;
  parse JSON filters with `FilterExpression.from_dict`. `Filter` and `FilterLike` are removed.
- `combine_filters`, `artifact_filter` and `branch_scope_filter` return a `FilterExpression` (call
  `to_dict()` for the JSON form); `normalize_filter`, `filter_field_paths`, `is_local_filter` and
  `matches_filter` take one.
- `order_by` takes field names or `RecordOrder`, not dicts.
- `get_schema()` returns an `InstanceSchemaResponse` (`resolve_registry` takes one),
  `get_branch()` a `BranchRecordResponse` and `get_users()` `{id: UserRecordResponse}`, also in the
  async variants. `branch_summary()` returns a `BranchSummary`. Records of `query_records`,
  `get_records_by_ids` and `query_dbwrites` stay dicts, because their columns depend on the registry
  and `select`.
- `registry` arguments are annotated as `LaminDBRegistry | Registry | str` (sensors and triggers:
  `LaminDBRegistry | str`).

**Breaking:** virtualenvs install `lamindb` instead of `lamindb-core` plus a hand-maintained list of
`numpy`, `pandas` and `pandera` (`LAMINDB_CORE_COMPANIONS`/`LAMINDB_CORE_COMPANIONS_PANDAS_2`, now
removed). lamindb's own metadata sets the pandas bound for each version. A listed `lamindb-core` is no
longer completed with these packages. `execution.requirements.lamindb_requirements()` is now
`lamindb_requirement()` and returns one requirement.

**Breaking:** facts that lamindb and the instance schema already provide are no longer copied.

- `execution.runtime` takes run status codes from `lamindb.base.types.RUN_STATUS_TO_CODE`; its
  `STATUS_*` constants are removed. `lamindb_version_at_least` is removed; use
  `lamindb_version_supported`.
- `execution.dag_run.MAX_REFERENCE_LENGTH` is now the one in `execution.runtime`.
- `client.dbwrite.DBWRITE_SUMMARY_FIELDS` is removed: event payloads include the declared
  `DbWriteResponse` fields except `data`, as before.
- `triggers.branches.BRANCH_TABLE` and `BRANCH_BLOCK_TABLE` are removed: the branch triggers take
  the table names from the instance schema, like the record triggers.
- `client.dbwrite.is_branch_move` is removed; it was unused. `classify_record_write` reports branch
  moves as `created` or `deleted` events.
- Requirements are recognised as lamindb with `packaging`, so pip's name normalisation applies, for
  example to `lamindb.core`.
- Tests check the branch status codes and field enums that the worker keeps without lamindb against
  lamindb itself.

Fixes:

- The package requires Python `>=3.10,<3.15`. `!=3.15` excluded only 3.15.0, not later 3.15 releases.
- The code generated for virtualenvs and pods is composed from `ast` nodes. A callable name that is
  not a Python identifier, or a step setting that is not a Python literal, now fails on the worker
  instead of in the virtualenv or pod.
- An artifact write with a malformed `_aux` or `kind` no longer stops the artifact event trigger.
  `storage_ongoing` and `is_internal_artifact` return `False` for it.
- `ArtifactRecordResponse.is_internal` and `is_internal_artifact` use the same kinds as the server-side
  `artifact_filter` (`INTERNAL_ARTIFACT_KINDS`, now in `client.enums`), not any `__lamindb` prefix.

## 0.0.1

Initial release.

- Supports lamindb 2.8.1 and newer only. The hook, sensors and lineage tasks reject instances on older
  lamindb with `LaminDBVersionError`, and event triggers don't poll them. An older `lamindb_version`
  fails when the DAG is parsed, and a lineage task whose virtualenv or pod has an older lamindb fails
  when it starts.
- Lineage: the `@task.lamindb_virtualenv` and `@task.lamindb_kubernetes` decorators record task runs,
  optionally grouped under a parent flow with `LaminDBFlow`. `LaminDBFlowInitOperator` and
  `LaminDBFlowFinishOperator` provide the low-level lifecycle operations. LaminDB runs in a
  virtualenv or a Kubernetes pod, never on the
  worker. The virtualenvs install `lamindb-core` in the instance's version (with pandas below 3
  for lamindb before 2.10) and get their credentials from the `lamindb` connection.
- Lineage scoping: `@task.lamindb_virtualenv` and `@task.lamindb_kubernetes` tasks are tracked standalone by
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
