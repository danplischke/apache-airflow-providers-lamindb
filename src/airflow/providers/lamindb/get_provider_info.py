"""Provider metadata registered via the ``apache_airflow_provider`` entry point. Keep in sync with provider.yaml."""


def get_provider_info() -> dict[str, object]:
    return {
        "package-name": "lamindb-airflow",
        "name": "LaminDB",
        "description": "Map Airflow DAG runs to `LaminDB <https://lamin.ai>`__ flows and tasks to steps.",
        "integrations": [
            {
                "integration-name": "LaminDB",
                "external-doc-url": "https://docs.lamin.ai",
                "tags": ["software"],
            }
        ],
        "operators": [
            {
                "integration-name": "LaminDB",
                "python-modules": [
                    "airflow.providers.lamindb.operators.flow",
                    "airflow.providers.lamindb.operators.step",
                ],
            }
        ],
        "task-decorators": [
            {"name": "lamindb", "class-name": "airflow.providers.lamindb.decorators.python.lamindb_task"},
            {
                "name": "lamindb_venv",
                "class-name": "airflow.providers.lamindb.decorators.python_virtualenv.lamindb_venv_task",
            },
            {"name": "lamindb_k8s", "class-name": "airflow.providers.lamindb.decorators.kubernetes.lamindb_k8s_task"},
        ],
    }
