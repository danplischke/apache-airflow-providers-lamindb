from __future__ import annotations

import importlib
import json
import re
from importlib.resources import files
from pathlib import Path

import jsonschema
import yaml

import airflow
from airflow.providers.lamindb import __version__
from airflow.providers.lamindb.get_provider_info import get_provider_info

ROOT = Path(__file__).parents[3]
PROVIDER_YAML = yaml.safe_load((ROOT / "provider.yaml").read_text())


def test_get_provider_info_matches_provider_yaml():
    expected = {k: v for k, v in PROVIDER_YAML.items() if k not in ("state", "lifecycle", "versions")}
    assert get_provider_info() == expected


def test_versions_are_consistent():
    pyproject_version = re.search(r'^version = "(.+)"$', (ROOT / "pyproject.toml").read_text(), re.M)
    assert pyproject_version is not None
    assert pyproject_version.group(1) == __version__ == PROVIDER_YAML["versions"][0]


def test_provider_info_schema():
    schema = json.loads((files(airflow) / "provider_info.schema.json").read_text())
    jsonschema.validate(get_provider_info(), schema)


def test_modules_and_hooks_are_importable():
    info = get_provider_info()
    for section in ("operators", "hooks", "triggers", "sensors"):
        for entry in info[section]:
            for module in entry["python-modules"]:
                importlib.import_module(module)
    for decorator in info["task-decorators"]:
        module, _, name = decorator["class-name"].rpartition(".")
        assert callable(getattr(importlib.import_module(module), name))
    for connection_type in info["connection-types"]:
        module, _, name = connection_type["hook-class-name"].rpartition(".")
        hook_class = getattr(importlib.import_module(module), name)
        assert hook_class.conn_type == connection_type["connection-type"]
        assert hook_class.hook_name == connection_type["hook-name"]


def test_discovered_by_providers_manager():
    from airflow.providers_manager import ProvidersManager
    from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime

    manager = ProvidersManager()
    assert "lamindb-airflow" in manager.providers
    assert "extra__lamindb__instance" in manager.connection_form_widgets
    hook_info = ProvidersManagerTaskRuntime().hooks["lamindb"]
    assert hook_info is not None
    assert hook_info.connection_type == "lamindb"
