from __future__ import annotations

# NOTE! Keep this in sync with ``provider.yaml``. ``tests/unit/lamindb/test_get_provider_info.py``
# verifies that both describe the same provider.


def get_provider_info() -> dict[str, object]:
    return {
        "package-name": "lamindb-airflow",
        "name": "LaminDB",
        "description": "`LaminDB <https://lamin.ai/>`__ lineage for DAG runs and event-driven scheduling "
        "via the LaminHub REST API.\n",
        "integrations": [
            {
                "integration-name": "LaminDB",
                "external-doc-url": "https://docs.lamin.ai/",
                "how-to-guide": [
                    "/docs/lamindb-airflow/lineage.rst",
                    "/docs/lamindb-airflow/triggers.rst",
                    "/docs/lamindb-airflow/sensors.rst",
                ],
                "tags": ["service", "software"],
            }
        ],
        "operators": [
            {
                "integration-name": "LaminDB",
                "python-modules": ["airflow.providers.lamindb.operators.flow"],
            }
        ],
        "hooks": [
            {
                "integration-name": "LaminDB",
                "python-modules": ["airflow.providers.lamindb.hooks.lamindb"],
            }
        ],
        "triggers": [
            {
                "integration-name": "LaminDB",
                "python-modules": [
                    "airflow.providers.lamindb.triggers.base",
                    "airflow.providers.lamindb.triggers.records",
                    "airflow.providers.lamindb.triggers.branches",
                ],
            }
        ],
        "sensors": [
            {
                "integration-name": "LaminDB",
                "python-modules": [
                    "airflow.providers.lamindb.sensors.records",
                    "airflow.providers.lamindb.sensors.branches",
                ],
            }
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow.providers.lamindb.hooks.lamindb.LaminDBHook",
                "hook-name": "LaminDB",
                "connection-type": "lamindb",
                "conn-fields": {
                    "instance": {
                        "label": "Instance",
                        "description": "LaminDB instance slug in the form ``owner/name`` "
                        "(for example ``laminlabs/lamindata``).",
                        "schema": {"type": ["string", "null"]},
                    }
                },
                "ui-field-behaviour": {
                    "hidden-fields": ["port", "login", "schema", "extra"],
                    "relabeling": {"host": "LaminHub API URL", "password": "API Key"},
                    "placeholders": {
                        "host": "https://aws.us-east-1.lamin.ai/api",
                        "password": "Lamin API key (leave empty for public instances)",
                        "instance": "owner/name",
                    },
                },
            }
        ],
        "task-decorators": [
            {
                "name": "lamindb_venv",
                "class-name": "airflow.providers.lamindb.decorators.python_virtualenv.lamindb_venv_task",
            },
            {
                "name": "lamindb_k8s",
                "class-name": "airflow.providers.lamindb.decorators.kubernetes.lamindb_k8s_task",
            },
        ],
    }
