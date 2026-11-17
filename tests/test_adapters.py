"""Unit tests for n8n, Calnode, and Windmill HTTP adapters using httpx.MockTransport."""

from __future__ import annotations

import json

import httpx
import pytest

from adapters.calnode import CalnodeAdapter
from adapters.n8n import N8nAdapter
from adapters.windmill import WindmillAdapter
from gate.config import GateConfig
from gate.errors import DownstreamError, DownstreamOutcomeUnknownError
from workflows.windmill.ops_task_assignment import main as windmill_assign_script


@pytest.mark.asyncio
async def test_calnode_adapter_real_v1_path_and_idempotency():
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["path"] = request.url.path
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content.decode()) if request.content else None
        return httpx.Response(
            201,
            json={
                "id": "95a85e0b-4df8-4c36-ba40-5244c34835a6",
                "status": "confirmed",
                "start_at": "2026-11-18T14:00:00Z",
                "end_at": "2026-11-18T14:30:00Z",
                "invite_delivery": "none",
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfg = GateConfig(
        calnode_base_url="http://calnode.local",
        calnode_api_key="cno_test",
        calnode_calendar_id="consultation-30m",
    )
    adapter = CalnodeAdapter(cfg, http_client=client)

    res = await adapter.execute(
        "create_booking",
        {"slotId": "2026-11-18T14:00:00Z", "attendeeEmail": "customer-a@allowlist.test.local"},
        idempotency_key="idem:cal:01",
    )
    await client.aclose()

    assert res["bookingId"] == "95a85e0b-4df8-4c36-ba40-5244c34835a6"
    assert res["inviteDelivery"] == "none"
    assert captured["path"] == "/v1/bookings"
    assert captured["headers"]["idempotency-key"] == "idem:cal:01"
    assert captured["body"]["event_type_slug"] == "consultation-30m"
    assert captured["body"]["start_at"] == "2026-11-18T14:00:00Z"


@pytest.mark.asyncio
async def test_n8n_adapter_timeout_raises_unknown_outcome():
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Timed out waiting for n8n")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfg = GateConfig(n8n_base_url="http://n8n.local", n8n_webhook_secret="sec")
    adapter = N8nAdapter(cfg, http_client=client)

    with pytest.raises(DownstreamOutcomeUnknownError):
        await adapter.execute("dispatch_followup", {"messageBody": "Hi"}, "idem:n8n:timeout")
    await client.aclose()


@pytest.mark.asyncio
async def test_windmill_adapter_error_and_script_determinism():
    # Test Windmill script determinism (idempotent assignment ID)
    out1 = windmill_assign_script(
        bookingId="bkg_1",
        conversationId="conv_1",
        assignee="ops-specialist-1@allowlist.test.local",
        idempotencyKey="wm:key:1",
    )
    out2 = windmill_assign_script(
        bookingId="bkg_1",
        conversationId="conv_1",
        assignee="ops-specialist-1@allowlist.test.local",
        idempotencyKey="wm:key:1",
    )
    assert out1["assignmentId"] == out2["assignmentId"]
    assert out1["state"] == "COMPLETED"

    # Test Windmill adapter surfaces HTTP 500 as DownstreamError
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="worker crash")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfg = GateConfig(windmill_base_url="http://windmill.local", windmill_token="tok")
    adapter = WindmillAdapter(cfg, http_client=client)

    with pytest.raises(DownstreamError):
        await adapter.execute(
            "start_assignment",
            {
                "bookingId": "bkg_1",
                "conversationId": "conv_1",
                "assignee": "ops@test",
            },
            "idem:wm:err",
        )
    await client.aclose()
