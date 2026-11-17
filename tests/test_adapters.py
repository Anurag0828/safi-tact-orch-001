"""Unit tests for n8n, Calnode, Windmill HTTP adapters & JetStream fail-closed publisher."""

from __future__ import annotations

import json

import httpx
import pytest

from adapters.calnode import CalnodeAdapter
from adapters.n8n import N8nAdapter
from adapters.windmill import WindmillAdapter
from gate.config import GateConfig
from gate.errors import DownstreamError, DownstreamOutcomeUnknownError, StoreError
from gate.history import JetStreamHistoryPublisher
from workflows.windmill.ops_task_assignment import main as windmill_assign_script


@pytest.mark.asyncio
async def test_calnode_adapter_real_v1_path_slots_and_idempotency():
    captured: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(
            {
                "method": request.method,
                "path": request.url.path,
                "headers": dict(request.headers),
                "body": json.loads(request.content.decode()) if request.content else None,
            }
        )
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "hosts": {"h1": {"name": "SAFI Admin"}},
                    "slots": [
                        {"start": "2026-11-18T14:00:00Z", "end": "2026-11-18T14:30:00Z"}
                    ],
                },
            )
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

    slots = await adapter.list_slots()
    assert slots["eventTypeSlug"] == "consultation-30m"
    assert slots["slots"][0]["slotId"] == "2026-11-18T14:00:00Z"

    res = await adapter.execute(
        "create_booking",
        {"slotId": "2026-11-18T14:00:00Z", "attendeeEmail": "customer-a@allowlist.test.local"},
        idempotency_key="idem:cal:01",
    )
    await client.aclose()

    assert res["bookingId"] == "95a85e0b-4df8-4c36-ba40-5244c34835a6"
    assert res["inviteDelivery"] == "none"
    assert captured[1]["path"] == "/v1/bookings"
    assert captured[1]["headers"]["idempotency-key"] == "idem:cal:01"
    assert captured[1]["body"]["event_type_slug"] == "consultation-30m"


@pytest.mark.asyncio
async def test_n8n_adapter_timeout_ssrf_and_invalid_json():
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("Timed out waiting for n8n")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfg = GateConfig(n8n_base_url="http://n8n.local:5678", n8n_webhook_secret="sec")
    adapter = N8nAdapter(cfg, http_client=client)

    # Timeout raises DownstreamOutcomeUnknownError
    with pytest.raises(DownstreamOutcomeUnknownError):
        await adapter.execute("dispatch_followup", {"messageBody": "Hi"}, "idem:n8n:timeout")

    # SSRF resumeUrl to foreign origin is rejected before any HTTP call
    with pytest.raises(DownstreamError, match="origin does not match"):
        await adapter.execute(
            "dispatch_followup",
            {"messageBody": "Hi", "resumeUrl": "http://evil.attacker.example/steal"},
            "idem:n8n:ssrf",
        )
    await client.aclose()

    # Invalid JSON response raises DownstreamError (never raw JSONDecodeError)
    client2 = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(200, text="not-json"))
    )
    adapter2 = N8nAdapter(cfg, http_client=client2)
    with pytest.raises(DownstreamError, match="Invalid JSON"):
        await adapter2.execute("ingest_lead", {"leadEmail": "a@b.c"}, "idem:n8n:badjson")
    await client2.aclose()


@pytest.mark.asyncio
async def test_windmill_adapter_polling_timeout_error_and_script_determinism():
    # 1. Script determinism
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

    # 2. Polling to CompletedJob succeeds & reads retry_count
    calls = {"poll": 0}

    def success_handler(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(200, text='"01a1-job-ok"')
        calls["poll"] += 1
        if calls["poll"] == 1:
            return httpx.Response(200, json={"id": "01a1-job-ok", "type": "QueuedJob"})
        return httpx.Response(
            200,
            json={
                "id": "01a1-job-ok",
                "type": "CompletedJob",
                "success": True,
                "retry_count": 1,
                "duration_ms": 42,
                "result": {"assignee": "ops-specialist-1@allowlist.test.local", "state": "COMPLETED"},
            },
        )

    client_ok = httpx.AsyncClient(transport=httpx.MockTransport(success_handler))
    cfg = GateConfig(windmill_base_url="http://windmill.local", windmill_token="tok")
    adapter_ok = WindmillAdapter(cfg, http_client=client_ok, max_poll_attempts=5, poll_interval_seconds=0.01)
    res = await adapter_ok.execute(
        "start_assignment",
        {"bookingId": "bkg_1", "conversationId": "conv_1", "assignee": "ops-specialist-1@allowlist.test.local"},
        "idem:wm:ok",
    )
    await client_ok.aclose()
    assert res["jobId"] == "01a1-job-ok"
    assert res["state"] == "COMPLETED"
    assert res["retryCount"] == 1
    assert res["error"] is None

    # 3. Poll timeout raises DownstreamOutcomeUnknownError (never falsely marks QueuedJob as DONE)
    def stuck_handler(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(200, text='"01a1-job-stuck"')
        return httpx.Response(200, json={"id": "01a1-job-stuck", "type": "QueuedJob"})

    client_stuck = httpx.AsyncClient(transport=httpx.MockTransport(stuck_handler))
    adapter_stuck = WindmillAdapter(cfg, http_client=client_stuck, max_poll_attempts=2, poll_interval_seconds=0.01)
    with pytest.raises(DownstreamOutcomeUnknownError, match="01a1-job-stuck"):
        await adapter_stuck.execute(
            "start_assignment",
            {"bookingId": "bkg_1", "conversationId": "conv_1", "assignee": "ops@test"},
            "idem:wm:stuck",
        )
    await client_stuck.aclose()

    # 4. HTTP 500 surfaces as DownstreamError
    client_err = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _r: httpx.Response(500, text="worker crash"))
    )
    adapter_err = WindmillAdapter(cfg, http_client=client_err)
    with pytest.raises(DownstreamError):
        await adapter_err.execute(
            "start_assignment",
            {"bookingId": "bkg_1", "conversationId": "conv_1", "assignee": "ops@test"},
            "idem:wm:err",
        )
    await client_err.aclose()


@pytest.mark.asyncio
async def test_jetstream_required_fails_closed_when_unreachable():
    cfg = GateConfig(nats_url="nats://127.0.0.1:1", jetstream_required=True)
    pub = JetStreamHistoryPublisher(cfg)
    with pytest.raises(StoreError, match="JetStream is required"):
        await pub.connect()
