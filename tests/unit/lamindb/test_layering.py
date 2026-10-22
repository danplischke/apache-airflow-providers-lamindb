from __future__ import annotations

import ast
from pathlib import Path

import pytest

import airflow.providers.lamindb as provider

PACKAGE = "airflow.providers.lamindb"
ROOT = Path(provider.__file__).parent

ALLOWED = {
    # the REST client is usable outside Airflow tasks; only the provider's exceptions and version
    # and the shipped runtime's version check come from outside it
    "client": {f"{PACKAGE}.client", f"{PACKAGE}.exceptions", f"{PACKAGE}.execution.runtime", PACKAGE},
    "hooks": {f"{PACKAGE}.client", f"{PACKAGE}.exceptions", "airflow.sdk"},
    "execution": {
        f"{PACKAGE}.client",
        f"{PACKAGE}.exceptions",
        f"{PACKAGE}.execution",
        f"{PACKAGE}.hooks",
        "airflow.sdk",
        "airflow.exceptions",
    },
}


def _imports(path: Path) -> set[str]:
    modules = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return {module for module in modules if module == "airflow" or module.startswith("airflow.")}


def _allowed(module: str, prefixes: set[str]) -> bool:
    # the bare package (for ``__version__``) must not admit its subpackages
    return any(
        module == prefix or (prefix != PACKAGE and module.startswith(f"{prefix}.")) for prefix in prefixes
    )


@pytest.mark.parametrize("package", sorted(ALLOWED))
def test_package_imports_follow_layers(package: str) -> None:
    violations = {
        f"{path.relative_to(ROOT)}: {module}"
        for path in sorted((ROOT / package).rglob("*.py"))
        for module in _imports(path)
        if not _allowed(module, ALLOWED[package])
    }
    assert not violations
