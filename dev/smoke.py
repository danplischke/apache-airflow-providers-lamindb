"""Smoke test of a base install: no dev dependencies, no lamindb and no k8s extra.

Run by ``just smoke`` and the smoke-test job in qa.yml. Airflow validates get_provider_info whenever it
is imported, so a broken one breaks ``import airflow`` for every installation that has this package.
"""

from __future__ import annotations

import importlib.util

from airflow.sdk import task

from airflow.providers.lamindb.get_provider_info import get_provider_info

get_provider_info()
print("import and provider info OK")

assert importlib.util.find_spec("lamindb") is None, "lamindb must not be a dependency of the worker"
print("lamindb not installed")

for name in ("lamindb_virtualenv", "lamindb_kubernetes"):
    assert hasattr(task, name), f"@task.{name} is not registered"
print("task decorators registered")

if importlib.util.find_spec("airflow.providers.cncf") is None:
    try:
        task.lamindb_kubernetes(lambda: None)
    except ImportError as e:
        if "apache-airflow-providers-lamindb[k8s]" not in str(e):
            raise SystemExit(f"@task.lamindb_kubernetes gives no install hint: {e}") from e
    else:
        raise SystemExit("@task.lamindb_kubernetes worked without apache-airflow-providers-cncf-kubernetes")
    print("@task.lamindb_kubernetes gives the install hint")
else:
    print("cncf.kubernetes is installed; install hint not checked")
