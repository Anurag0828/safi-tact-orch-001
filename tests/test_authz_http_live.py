"""D4: Live-style AUTHZ_MODE=http tests against a real local mock SAFI authorization HTTP server."""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
import pytest_asyncio

from gate.app import create_app
from gate.authz import HttpPermissionProvider
from gate.config import GateConfig
from gate.history import InMemoryHistoryPublisher
from gate.models import ActionState, ActorContext, ActorKind, ControlEventType, ConversationMode, EngineName, Outcome
from gate.store import GateStore
from tests.conftest import MockEngineAdapter


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class MockSafiAuthzState:
    def __init__(self) -> None:
        self.mode: str = "allow"  # allow | deny | timeout | status_500 | malformed
        self.revoked_actors: set[str] = set()
        self.timeout_delay_s: float = 0.6
        self.expected_token: str = "safi-authz-secret-tok"
        self.request_log: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(self, entry: dict[str, Any]) -> None:
        with self._lock:
            self.request_log.append(entry)


def _start_mock_safi_authz_server(state: MockSafiAuthzState) -> tuple[ThreadingHTTPServer, str]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def do_POST(self) -> None:  # noqa: N802
            ts = _utc_now()
            mono = time.monotonic()
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else "{}"
            body = json.loads(raw)
            auth = self.headers.get("Authorization", "")
            actor_guid = body.get("actorGuid", "")
            conv_id = body.get("conversationId", "")
            op = body.get("operation", "")

            if auth != f"Bearer {state.expected_token}":
                state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "401_bad_token"})
                self.send_response(401)
                self.end_headers()
                return

            if state.mode == "timeout":
                state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "timeout"})
                time.sleep(state.timeout_delay_s)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"allowed": true}')
                return

            if state.mode == "status_500":
                state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "500"})
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"internal authz error")
                return

            if state.mode == "malformed":
                state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "malformed"})
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b"{not-valid-json")
                return

            if state.mode == "deny" or actor_guid in state.revoked_actors:
                state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "deny"})
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"allowed": false}')
                return

            state.record({"ts": ts, "mono": mono, "actorGuid": actor_guid, "conversationId": conv_id, "operation": op, "decision": "allow"})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"allowed": true}')

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{port}"


class TimestampedMockAdapter(MockEngineAdapter):
    def __init__(self, engine_name: str) -> None:
        super().__init__(engine_name)
        self.exec_monos: list[tuple[str, float, str]] = []

    async def execute(self, operation: str, payload: dict[str, Any], idempotency_key: str) -> dict[str, Any]:
        self.exec_monos.append((idempotency_key, time.monotonic(), _utc_now()))
        return await super().execute(operation, payload, idempotency_key)


@pytest_asyncio.fixture
async def http_authz_env(tmp_path):
    state = MockSafiAuthzState()
    server, url = _start_mock_safi_authz_server(state)
    cfg = GateConfig(
        store_path=str(tmp_path / "d4_http_authz.db"),
        allowed_recipients=frozenset({"customer-a@allowlist.test.local"}),
        gate_api_token="gate-tok-d4",
        gate_require_auth=True,
        n8n_webhook_secret="wh-sec-d4",
        authz_mode="http",
        safi_authz_url=url,
        safi_authz_token=state.expected_token,
        authz_timeout_seconds=0.25,
        jetstream_required=False,
    )
    store = GateStore(cfg.store_path)
    await store.initialize()
    history = InMemoryHistoryPublisher()
    adapters = {
        EngineName.N8N: TimestampedMockAdapter("n8n"),
        EngineName.CALNODE: TimestampedMockAdapter("calnode"),
        EngineName.WINDMILL: TimestampedMockAdapter("windmill"),
    }
    app = create_app(
        config=cfg,
        store=store,
        history=history,
        authz=HttpPermissionProvider(cfg),
        adapters=adapters,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"Authorization": "Bearer gate-tok-d4"},
    ) as client:
        yield client, app, state, store, history, adapters
    await store.close()
    server.shutdown()
    server.server_close()


def _actor() -> dict[str, Any]:
    return ActorContext(
        actorGuid="user_d4_owner",
        actorKind=ActorKind.USER,
        meetingContext={"meetingId": "mtg_d4"},
        portContext={"port": "safichat-http"},
        permissionProvenance={"role": "BUSINESS_OWNER", "scope": "conversation:admin"},
    ).model_dump(mode="json")


@pytest.mark.asyncio
async def test_d4_case1_unset_safi_authz_url_fails_closed(tmp_path):
    """D4.1: AUTHZ_MODE=http with SAFI_AUTHZ_URL unset denies every action (503 fail-closed, 0 effects)."""
    cfg = GateConfig(
        store_path=str(tmp_path / "d4_unset.db"),
        allowed_recipients=frozenset({"customer-a@allowlist.test.local"}),
        gate_api_token="gate-tok-d4",
        gate_require_auth=True,
        authz_mode="http",
        safi_authz_url="",
        jetstream_required=False,
    )
    store = GateStore(cfg.store_path)
    await store.initialize()
    adapters = {
        EngineName.N8N: TimestampedMockAdapter("n8n"),
        EngineName.CALNODE: TimestampedMockAdapter("calnode"),
        EngineName.WINDMILL: TimestampedMockAdapter("windmill"),
    }
    app = create_app(config=cfg, store=store, history=InMemoryHistoryPublisher(), authz=HttpPermissionProvider(cfg), adapters=adapters)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver", headers={"Authorization": "Bearer gate-tok-d4"}) as client:
        r = await client.post(
            "/safichat/ops/marketing/followup",
            json={
                "conversationId": "conv_d4_unset",
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Must fail closed",
                "idempotencyKey": "d4_unset_1",
                "actor": _actor(),
            },
        )
    await store.close()
    assert r.status_code == 503
    assert r.json()["errorCode"] == "PERMISSION_CHECK_FAILED"
    assert len(adapters[EngineName.N8N].call_log) == 0


@pytest.mark.asyncio
async def test_d4_cases_2_to_7_allow_deny_timeout_5xx_malformed_revocation_and_recheck_order(http_authz_env):
    """D4.2–D4.7: Live HTTP mock SAFI authz covering allow, deny, timeout, 500, malformed, mid-flow revocation, and pre-dispatch recheck timestamps."""
    client, _app, state, store, history, adapters = http_authz_env
    n8n = adapters[EngineName.N8N]

    # Case 2 & 7: Allow -> executes + fresh recheck immediately precedes dispatch
    state.mode = "allow"
    r_allow = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_main",
            "subjectLineDisplay": "D4 HTTP Authz",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Allowed msg",
            "idempotencyKey": "d4_allow_1",
            "actor": _actor(),
        },
    )
    assert r_allow.status_code == 200 and r_allow.json()["state"] == ActionState.DONE.value
    assert len(n8n.call_log) == 1
    assert len(state.request_log) >= 2  # entry authz + immediate pre-dispatch recheck
    last_check_mono = state.request_log[-1]["mono"]
    exec_mono = n8n.exec_monos[-1][1]
    assert last_check_mono <= exec_mono, "Fresh authz recheck MUST precede downstream execution!"

    # Case 3: Deny -> 403, zero downstream effect
    state.mode = "deny"
    r_deny = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_main",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Denied msg",
            "idempotencyKey": "d4_deny_1",
            "actor": _actor(),
        },
    )
    assert r_deny.status_code == 403
    assert len(n8n.call_log) == 1

    # Case 4: Timeout -> 503 fail-closed, zero effect, conversation -> FAILED_CLOSED
    state.mode = "allow"
    r_seed_to = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_timeout",
            "subjectLineDisplay": "D4 Timeout",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Seed",
            "idempotencyKey": "d4_to_seed",
            "actor": _actor(),
        },
    )
    assert r_seed_to.status_code == 200
    before_to = len(n8n.call_log)
    state.mode = "timeout"
    r_to = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_timeout",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Must timeout fail closed",
            "idempotencyKey": "d4_to_1",
            "actor": _actor(),
        },
    )
    assert r_to.status_code == 503
    assert len(n8n.call_log) == before_to
    assert (await store.get_conversation("conv_d4_timeout"))["mode"] == ConversationMode.FAILED_CLOSED.value

    # Case 5a: 500 response -> 503 fail-closed, zero effect, FAILED_CLOSED
    state.mode = "allow"
    await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_500",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Seed 500",
            "idempotencyKey": "d4_500_seed",
            "actor": _actor(),
        },
    )
    before_500 = len(n8n.call_log)
    state.mode = "status_500"
    r_500 = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_500",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Must fail closed 500",
            "idempotencyKey": "d4_500_1",
            "actor": _actor(),
        },
    )
    assert r_500.status_code == 503
    assert len(n8n.call_log) == before_500
    assert (await store.get_conversation("conv_d4_500"))["mode"] == ConversationMode.FAILED_CLOSED.value

    # Case 5b: Malformed JSON -> 503 fail-closed, zero effect, FAILED_CLOSED
    state.mode = "allow"
    await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_malformed",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Seed malformed",
            "idempotencyKey": "d4_mal_seed",
            "actor": _actor(),
        },
    )
    before_mal = len(n8n.call_log)
    state.mode = "malformed"
    r_mal = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_malformed",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Must fail closed malformed",
            "idempotencyKey": "d4_mal_1",
            "actor": _actor(),
        },
    )
    assert r_mal.status_code == 503
    assert len(n8n.call_log) == before_mal
    assert (await store.get_conversation("conv_d4_malformed"))["mode"] == ConversationMode.FAILED_CLOSED.value

    # Case 6: Revocation mid-flow — HELD while permitted -> revoke at mock -> approve & ungate release both blocked!
    state.mode = "allow"
    await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_revoke",
            "subjectLineDisplay": "D4 Revoke Mid-Flow",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Seed revoke",
            "idempotencyKey": "d4_rev_seed",
            "actor": _actor(),
        },
    )
    r_barge = await client.post(
        "/safichat/ops/gate/barge-in",
        json={"conversationId": "conv_d4_revoke", "actor": _actor()},
    )
    assert r_barge.status_code == 200 and r_barge.json()["mode"] == "GATED"

    r_held = await client.post(
        "/safichat/ops/marketing/followup",
        json={
            "conversationId": "conv_d4_revoke",
            "recipientEmail": "customer-a@allowlist.test.local",
            "messageBody": "Held before revocation",
            "idempotencyKey": "d4_rev_held_1",
            "actor": _actor(),
        },
    )
    assert r_held.status_code == 200 and r_held.json()["state"] == "HELD"
    held_action_id = r_held.json()["actionId"]
    before_rev = len(n8n.call_log)

    # Revoke actor at mock SAFI authz server!
    state.revoked_actors.add("user_d4_owner")

    r_app_rev = await client.post(
        "/safichat/ops/gate/approve",
        json={"conversationId": "conv_d4_revoke", "actionId": held_action_id, "actor": _actor()},
    )
    assert r_app_rev.status_code == 403
    assert len(n8n.call_log) == before_rev

    r_ung_rev = await client.post(
        "/safichat/ops/gate/ungate",
        json={
            "conversationId": "conv_d4_revoke",
            "resolutions": [{"actionId": held_action_id, "decision": "RELEASE"}],
            "actor": _actor(),
        },
    )
    assert r_ung_rev.status_code == 403
    assert len(n8n.call_log) == before_rev
    assert any(
        e.conversationId == "conv_d4_revoke"
        and e.type == ControlEventType.ACTION_FAILED
        and e.outcome == Outcome.DENIED
        for e in history.events
    )
