from __future__ import annotations

import time

import httpx
import respx

from airflow.providers.lamindb.client.client import LaminHubClient, LaminHubConfig
from airflow.providers.lamindb.client.models import BranchRecordResponse
from unit.lamindb.hooks.test_lamindb import API, HUB, INSTANCE_ID, SCHEMA, SETTINGS, make_jwt


async def test_client_works_without_an_airflow_connection():
    client = LaminHubClient(
        LaminHubConfig(hub_api_url=HUB, api_key="key", owner="owner", name="name"), retry_backoff=0
    )
    with respx.mock(assert_all_called=False) as router:
        token = router.post(f"{HUB}/account/jwt").mock(
            return_value=httpx.Response(200, json={"accessToken": make_jwt(time.time() + 3600)})
        )
        settings = router.get(f"{HUB}/instances/owner/name/settings").mock(
            return_value=httpx.Response(200, json=SETTINGS)
        )
        router.get(f"{API}/instances/{INSTANCE_ID}/schema").mock(
            return_value=httpx.Response(200, json=SCHEMA)
        )
        router.post(f"{API}/instances/{INSTANCE_ID}/modules/core/branch").mock(
            return_value=httpx.Response(200, json=[{"id": 1, "name": "main"}])
        )
        try:
            assert client.get_instance().id == INSTANCE_ID
            # the async methods share the instance and access token of the sync ones
            assert await client.aget_branch("main") == BranchRecordResponse(id=1, name="main")
            assert settings.call_count == 1
            assert token.call_count == 1
        finally:
            client.close()
            await client.aclose()
