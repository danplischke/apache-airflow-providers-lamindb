"""LaminDB registries (Django models) and how they are addressed in the LaminHub REST API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.lamindb.client.enums import LaminDBRegistry
    from airflow.providers.lamindb.client.models import InstanceSchemaResponse


@dataclass(frozen=True)
class Registry:
    """A LaminDB registry (a Django model), addressed by schema module and model name."""

    module: str
    model: str
    table_name: str

    @property
    def name(self) -> str:
        return f"{self.module}.{self.model}"


DBWRITE_REGISTRY = Registry(module="hubmodule", model="dbwrite", table_name="hubmodule_dbwrite")
"""The LaminHub database write log ("Changes → Database writes" in the LaminHub UI)."""


def normalize_registry_name(registry: str) -> tuple[str, str]:
    """``(module, model)`` of a registry name in lower case, with ``core`` as the default module."""
    module, _, model = registry.strip().rpartition(".")
    module = (module or "core").lower()
    return ("core" if module == "lamindb" else module), model.lower()


def resolve_registry(schema: InstanceSchemaResponse, registry: LaminDBRegistry | Registry | str) -> Registry:
    """
    Resolve ``module.model`` (for example ``core.artifact`` or ``bionty.Gene``) using an instance schema.

    Without a module prefix, ``core`` is assumed. Model names are case-insensitive.
    """
    if isinstance(registry, Registry):
        return registry
    module, model = normalize_registry_name(registry)
    if (module, model) == (DBWRITE_REGISTRY.module, DBWRITE_REGISTRY.model):
        return DBWRITE_REGISTRY
    models = schema.root.get(module)
    if models is None:
        raise ValueError(f"Unknown schema module {module!r}; available modules: {sorted(schema.root)}")
    description = models.get(model)
    if description is None:
        raise ValueError(f"Unknown registry {module}.{model!s}; available models: {sorted(models)}")
    return Registry(module=module, model=model, table_name=description.table_name or f"{module}_{model}")
