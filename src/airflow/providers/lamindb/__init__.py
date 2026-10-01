"""Apache Airflow provider for LaminDB: map DAG runs to flows and tasks to steps."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

__all__ = [
    "LaminDBFlowFinishOperator",
    "LaminDBFlowInitOperator",
    "LaminDBStepOperator",
    "LaminDBVenvFlowFinishOperator",
    "LaminDBVenvFlowInitOperator",
    "__version__",
]


def __getattr__(name: str) -> Any:
    # Lazy: Airflow imports this package for get_provider_info while its own
    # configuration is still initialising, so importing operators here is circular.
    if name in __all__:
        from airflow.providers.lamindb import operators

        return getattr(operators, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
