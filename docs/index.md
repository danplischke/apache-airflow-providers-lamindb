# apache-airflow-providers-lamindb

Provider package for [LaminDB](https://lamin.ai). It records DAG runs in LaminDB and lets DAGs react to
changes in LaminDB instances hosted on LaminHub:

- **Lineage**: record DAG runs as LaminDB flow runs and tasks as steps, with lamindb running in a
  virtualenv or a Kubernetes pod. See [Lineage](lineage.md).
- **Event-driven scheduling**: run DAGs when artifacts or records of any registry are created,
  updated or deleted, when branches (Change Requests) change their status, or when comments and readmes
  are added to branches. See [Triggers](triggers.md).
- **Deferrable sensors**: wait inside a DAG until a branch is merged or an artifact is available.
  See [Sensors](sensors.md).
- **Hook**: query the LaminHub REST API from tasks.
- **Filters**: select records with Python expressions such as
  `F(ArtifactField.KEY).startswith("raw/")`, with enums for registries, fields and operators.
  See [Filters](filters.md).

## Requirements

The minimum Apache Airflow version supported by this provider is `3.3.0`, which introduced the asset
state store that the event triggers use to resume after restarts.

| PIP package | Version required |
|---|---|
| `apache-airflow` | `>=3.3.0` |
| `apache-airflow-providers-standard` | `>=1.15.0` |
| `httpx` | `>=0.27.0` |
| `packaging` | `>=22` |

The worker doesn't need the `lamindb` Python package. The triggers, sensors and hook talk to the
[LaminHub REST API](https://docs.lamin.ai/rest), and the lineage operators install `lamindb-core` in a
virtualenv or use a Kubernetes pod's image. `@task.lamindb_k8s` needs
`pip install "apache-airflow-providers-lamindb[cncf.kubernetes]"`.

## Installation

```bash
pip install apache-airflow-providers-lamindb
```
