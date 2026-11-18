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


@pytest.mark.asyncio
async def test_d1_jetstream_blackholed_host_fails_closed_within_budget():
    """D1: Blackholed/unreachable NATS with JETSTREAM_REQUIRED=true fails closed within <= 20s (never hangs)."""
    import asyncio
    import time

    t0 = time.monotonic()
    cfg = GateConfig(
        nats_url="nats://192.0.2.1:4222",
        jetstream_required=True,
        nats_connect_timeout_seconds=1.0,
        nats_connect_total_budget_seconds=3.5,
    )
    pub = JetStreamHistoryPublisher(cfg)
    with pytest.raises(StoreError, match="JetStream is required"):
        await asyncio.wait_for(pub.connect(), timeout=20.0)
    elapsed = time.monotonic() - t0
    assert elapsed < 10.0, f"Connect took too long: {elapsed:.2f}s"
    assert pub.connected is False


@pytest.mark.asyncio
async def test_d1_jetstream_optional_falls_back_without_exception(test_actor):
    """D1: Unreachable NATS with JETSTREAM_REQUIRED=false falls back cleanly within <= 10s."""
    import asyncio
    from gate.models import ControlEventType, Outcome

    cfg = GateConfig(
        nats_url="nats://192.0.2.1:4222",
        jetstream_required=False,
        nats_connect_timeout_seconds=1.0,
        nats_connect_total_budget_seconds=3.0,
    )
    pub = JetStreamHistoryPublisher(cfg)
    await asyncio.wait_for(pub.connect(), timeout=10.0)
    assert pub.connected is False
    assert pub.fallback is not None

    ev = await pub.record_control_event(
        conversation_id="conv_d1_opt",
        actor=test_actor,
        event_type=ControlEventType.MODE_GATED,
        operation_id="gate:barge_in",
        idempotency_key="d1_opt_1",
        outcome=Outcome.OK,
    )
    assert len(pub.fallback.events) == 1
    assert pub.fallback.events[0].eventId == ev.eventId


@pytest.mark.asyncio
async def test_d1_gate_app_startup_fails_closed_when_nats_required_and_down(tmp_path):
    """D1: FastAPI lifespan fails closed (raises StoreError within <= 20s) when JETSTREAM_REQUIRED=true and NATS is down."""
    import asyncio
    from gate.app import create_app

    cfg = GateConfig(
        store_path=str(tmp_path / "startup_fail.db"),
        nats_url="nats://127.0.0.1:1",
        jetstream_required=True,
        nats_connect_timeout_seconds=1.0,
        nats_connect_total_budget_seconds=3.5,
        gate_api_token="tok",
        n8n_webhook_secret="sec",
        allowed_recipients=frozenset({"customer-a@allowlist.test.local"}),
    )
    app = create_app(config=cfg)
    with pytest.raises(StoreError, match="JetStream is required"):
        async with asyncio.timeout(20.0):
            async with app.router.lifespan_context(app):
                pass


@pytest.mark.asyncio
async def test_d1_midrun_nats_outage_blocks_downstream_and_never_silent_done(
    test_config, gate_store, authz_provider, mock_adapters, test_actor
):
    """D1: Mid-run JetStream outage either blocks before downstream (0 effects) or fails closed (<=1 effect, NOT DONE)."""
    from gate.dispatch import GateDispatcher
    from gate.history import InMemoryHistoryPublisher
    from gate.models import ActionState, ActionSubmission, ConversationMode, EngineName
    from gate.state_machine import ConversationRuntimeRegistry

    authz_provider.authorize_bootstrap_actor(test_actor.actorGuid)

    # Case A: NATS dies BEFORE downstream executes -> 0 downstream effects + FAILED_CLOSED
    class PreDispatchDeadHistory(InMemoryHistoryPublisher):
        async def ensure_available(self) -> None:
            raise StoreError("Simulated mid-run NATS outage before dispatch")

    disp_a = GateDispatcher(
        test_config, gate_store, PreDispatchDeadHistory(), authz_provider, ConversationRuntimeRegistry(), mock_adapters
    )
    with pytest.raises(StoreError, match="mid-run NATS outage"):
        await disp_a.submit_action(
            ActionSubmission(
                conversationId="conv_d1_pre",
                subjectLineDisplay="D1 Pre Outage",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="d1_pre_1",
                payload={"recipientEmail": "customer-a@allowlist.test.local", "messageBody": "hi"},
                recipients=["customer-a@allowlist.test.local"],
                actor=test_actor,
            )
        )
    assert len(mock_adapters[EngineName.N8N].call_log) == 0
    act_a = await gate_store.get_action_by_idempotency_key("conv_d1_pre", "d1_pre_1")
    assert act_a["state"] == ActionState.FAILED.value
    assert (await gate_store.get_conversation("conv_d1_pre"))["mode"] == ConversationMode.FAILED_CLOSED.value

    # Case B: NATS dies DURING downstream execution -> <=1 effect, action FAILED(unknown) (NOT DONE!), conv FAILED_CLOSED
    class PostExecDeadHistory(InMemoryHistoryPublisher):
        async def record_control_event(self, *args, **kwargs):
            raise StoreError("Simulated NATS drop during ACTION_DISPATCHED publish")

    disp_b = GateDispatcher(
        test_config, gate_store, PostExecDeadHistory(), authz_provider, ConversationRuntimeRegistry(), mock_adapters
    )
    with pytest.raises(StoreError, match="NATS drop"):
        await disp_b.submit_action(
            ActionSubmission(
                conversationId="conv_d1_post",
                subjectLineDisplay="D1 Post Outage",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="d1_post_1",
                payload={"recipientEmail": "customer-a@allowlist.test.local", "messageBody": "hi"},
                recipients=["customer-a@allowlist.test.local"],
                actor=test_actor,
            )
        )
    assert len(mock_adapters[EngineName.N8N].call_log) == 1
    act_b = await gate_store.get_action_by_idempotency_key("conv_d1_post", "d1_post_1")
    assert act_b["state"] == ActionState.FAILED.value  # NEVER silently DONE without audit event!
    rec_b = await gate_store.get_idempotency_record("conv_d1_post", "d1_post_1")
    assert rec_b["outcome"] == "unknown"
    assert (await gate_store.get_conversation("conv_d1_post"))["mode"] == ConversationMode.FAILED_CLOSED.value


@pytest.mark.asyncio
async def test_a10_corrupt_sqlite_store_fails_closed(tmp_path):
    """A10.4: Corrupted SQLite store file fails closed with StoreError instead of silently defaulting to UNGATED."""
    from gate.store import GateStore

    bad_db = tmp_path / "corrupt_gate.db"
    bad_db.write_bytes(b"NOT A VALID SQLITE DATABASE HEADER CORRUPTION TEST 1234567890" * 4)
    store = GateStore(str(bad_db))
    with pytest.raises(StoreError):
        await store.initialize()
