from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from airflow.providers.lamindb.utils.remote import RemoteLaminDB

if TYPE_CHECKING:
    from unit.lamindb.fakes import FakeHook

INSTANCE_LAMINDB_VERSION = "2.10.0"


@pytest.fixture
def fake_hook_factory(monkeypatch):
    """Patch triggers to use a ``FakeHook``; returns a function that installs one."""

    def install(trigger, hook: FakeHook) -> FakeHook:
        monkeypatch.setattr(trigger, "_get_hook", lambda: hook)
        return hook

    return install


@pytest.fixture
def lamindb_connection(monkeypatch):
    """The ``lamindb_default`` connection; LaminHub reports lamindb 2.10.0 for its instance."""
    conn = {"conn_type": "lamindb", "password": "api-key", "extra": {"instance": "owner/name"}}
    monkeypatch.setenv("AIRFLOW_CONN_LAMINDB_DEFAULT", json.dumps(conn))
    monkeypatch.setattr(RemoteLaminDB, "instance_lamindb_version", lambda self: INSTANCE_LAMINDB_VERSION)
    return conn
