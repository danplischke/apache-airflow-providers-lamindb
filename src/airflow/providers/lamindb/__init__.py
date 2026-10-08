"""Apache Airflow provider for LaminDB.

Maps DAG runs to LaminDB flows and tasks to steps, and schedules DAGs on LaminHub events.
"""

from __future__ import annotations

from typing import Any

import packaging.version

from airflow import __version__ as airflow_version  # type: ignore[attr-defined]

__all__ = [
    "LaminDBFlowFinishOperator",
    "LaminDBFlowInitOperator",
    "__version__",
]

__version__ = "0.1.0"

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "3.3.0"
):
    raise RuntimeError(f"The package `lamindb-airflow:{__version__}` needs Apache Airflow 3.3.0+")


def __getattr__(name: str) -> Any:
    # Lazy: Airflow imports this package for get_provider_info while its own
    # configuration is still initialising, so importing operators here is circular.
    if name in __all__:
        from airflow.providers.lamindb import operators

        return getattr(operators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
