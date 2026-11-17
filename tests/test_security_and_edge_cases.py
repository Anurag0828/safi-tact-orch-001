"""Comprehensive security, cross-conversation isolation, concurrency, and fail-closed regression tests."""

from __future__ import annotations

import asyncio

import pytest

from gate.config import GateConfig
from gate.dispatch import GateDispatcher
from gate.errors import (
    ActionNotFoundError,
    AmbiguousSubjectLineError,
    DownstreamOutcomeUnknownError,
    GateFailedClosedError,
    IdempotencyConflictError,
    InvalidModeTransitionError,
    StoreError,
    UnauthorizedError,
)
from gate.history import JetStreamHistoryPublisher
from gate.models import (
    ActionResolution,
    ActionState,
    ActionSubmission,
    ActorContext,
    ActorKind,
    ControlEventType,
    ConversationMode,
    EngineName,
    Outcome,
    ReleaseDecision,
)
from gate.modes import GateModeController
from gate.state_machine import ConversationRuntimeRegistry


def _make_actor(guid: str, scope: str = "conversation:admin", allowed_ops: list[str] | None = None) -> ActorContext:
    prov = {"role": "BUSINESS_OWNER", "scope": scope}
    if allowed_ops is not None:
        prov["allowedOperations"] = allowed_ops
    return ActorContext(
        actorGuid=guid,
        actorKind=ActorKind.USER,
        meetingContext={"meetingId": f"mtg_{guid}"},
        portContext={"port": "safichat-test"},
        permissionProvenance=prov,
    )


@pytest.fixture
def isolated_system(test_config, gate_store, history_publisher, authz_provider, mock_adapters):
    authz_provider.authorize_bootstrap_actor("user_alpha")
    authz_provider.authorize_bootstrap_actor("user_beta")
    runtimes = ConversationRuntimeRegistry()
    dispatcher = GateDispatcher(
        test_config, gate_store, history_publisher, authz_provider, runtimes, mock_adapters
    )
    modes = GateModeController(
        test_config, gate_store, history_publisher, authz_provider, runtimes, dispatcher
    )
    return dispatcher, modes, authz_provider, gate_store, history_publisher, mock_adapters


@pytest.mark.asyncio
async def test_no_cross_conversation_dos_via_incomplete_context(isolated_system):
    """Unauthorized attacker sending empty meetingContext for victim's convId gets 403 and NEVER fails-closed victim."""
    dispatcher, modes, _, store, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    # User A initializes conv_victim cleanly
    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_victim",
            subjectLineDisplay="Victim Deal",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="k1",
            payload={"messageBody": "hello"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )

    # Attacker (User B) sends incomplete context targeting User A's conv_victim
    attacker_incomplete = ActorContext(actorGuid="user_beta", actorKind=ActorKind.USER)
    with pytest.raises(UnauthorizedError):
        await modes.barge_in(actor=attacker_incomplete, conversation_id="conv_victim")

    # Victim's conversation MUST still be UNGATED (NOT FAILED_CLOSED!)
    victim = await store.get_conversation("conv_victim")
    assert victim["mode"] == ConversationMode.UNGATED.value

    # Attacker targeting nonexistent conversation MUST NOT create a bogus FAILED_CLOSED row
    stranger_incomplete = ActorContext(actorGuid="stranger_999", actorKind=ActorKind.USER)
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_bogus_nonexistent",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="bogus_k1",
                payload={"messageBody": "x"},
                recipients=["customer-a@allowlist.test.local"],
                actor=stranger_incomplete,
            )
        )
    assert await store.get_conversation("conv_bogus_nonexistent") is None


@pytest.mark.asyncio
async def test_idempotency_key_strictly_scoped_per_conversation_and_operation(isolated_system):
    """User B reusing User A's idempotencyKey on conv_B gets independent execution (ZERO cross-conv data leak)."""
    dispatcher, _, _, _, _, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")
    user_b = _make_actor("user_beta")

    res_a = await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_iso_a",
            subjectLineDisplay="Deal A",
            engine=EngineName.CALNODE,
            operation="create_booking",
            idempotencyKey="shared_key_001",
            payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    res_b = await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_iso_b",
            subjectLineDisplay="Deal B",
            engine=EngineName.CALNODE,
            operation="create_booking",
            idempotencyKey="shared_key_001",
            payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_b,
        )
    )

    assert res_a["idempotentReplay"] is False
    assert res_b["idempotentReplay"] is False
    assert res_a["result"]["bookingId"] != res_b["result"]["bookingId"]
    assert len(mock_adapters[EngineName.CALNODE].call_log) == 2

    # Same conversation + same key + DIFFERENT operation -> 409 IDEMPOTENCY_CONFLICT
    with pytest.raises(IdempotencyConflictError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_iso_a",
                subjectLineDisplay="Deal A",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="shared_key_001",
                payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )


@pytest.mark.asyncio
async def test_subject_line_scoped_to_caller_no_cross_user_leak(isolated_system):
    """Subject Line collisions across different users never leak other users' conversationIds."""
    dispatcher, modes, _, _, _, _ = isolated_system
    user_a = _make_actor("user_alpha")
    user_b = _make_actor("user_beta")
    stranger = _make_actor("stranger_999")

    # Both User A and User B create conversations with the exact same Subject Line
    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_subj_a1",
            subjectLineDisplay="Monthly Renewal",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="sa1",
            payload={"messageBody": "a1"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_subj_b1",
            subjectLineDisplay="Monthly Renewal",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="sb1",
            payload={"messageBody": "b1"},
            recipients=["customer-b@allowlist.test.local"],
            actor=user_b,
        )
    )

    # Stranger querying "Monthly Renewal" gets 403 UNAUTHORIZED (zero candidate enumeration!)
    with pytest.raises(UnauthorizedError):
        await modes.barge_in(actor=stranger, subject_line="Monthly Renewal")

    # User A querying "Monthly Renewal" uniquely resolves to User A's conv_subj_a1 (no collision with User B!)
    barge_a = await modes.barge_in(actor=user_a, subject_line="Monthly Renewal")
    assert barge_a["conversationId"] == "conv_subj_a1"
    assert barge_a["mode"] == ConversationMode.GATED.value

    # When User A has TWO conversations with "Monthly Renewal", ambiguity returns ONLY User A's two IDs
    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_subj_a2",
            subjectLineDisplay="Monthly Renewal",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="sa2",
            payload={"messageBody": "a2"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    with pytest.raises(AmbiguousSubjectLineError) as exc_info:
        await modes.barge_in(actor=user_a, subject_line="Monthly Renewal")
    candidate_ids = {c["conversationId"] for c in exc_info.value.details["candidates"]}
    assert candidate_ids == {"conv_subj_a1", "conv_subj_a2"}
    assert "conv_subj_b1" not in candidate_ids


@pytest.mark.asyncio
async def test_windmill_job_status_idor_blocked(app_client, test_actor):
    """User B cannot read User A's Windmill jobId by passing conv_B to /operations/job-status."""
    client, app = app_client
    app.state.authz.authorize_bootstrap_actor("user_beta")
    user_b = _make_actor("user_beta")

    # User A runs a Windmill job on conv_A
    r_a = await client.post(
        "/safichat/ops/operations/assign",
        json={
            "conversationId": "conv_wm_a",
            "subjectLineDisplay": "WM A",
            "bookingId": "bkg_a",
            "assignee": "ops-specialist-1@allowlist.test.local",
            "idempotencyKey": "wm_a_1",
            "actor": test_actor.model_dump(mode="json"),
        },
    )
    assert r_a.status_code == 200
    job_a = r_a.json()["result"]["jobId"]

    # User A CAN read their own job on conv_wm_a
    r_ok = await client.post(
        "/safichat/ops/operations/job-status",
        json={
            "conversationId": "conv_wm_a",
            "jobId": job_a,
            "actor": test_actor.model_dump(mode="json"),
        },
    )
    assert r_ok.status_code == 200

    # User B initializes conv_wm_b, then tries to read User A's job_a using conv_wm_b -> BLOCKED (404)
    r_idor = await client.post(
        "/safichat/ops/operations/job-status",
        json={
            "conversationId": "conv_wm_b",
            "jobId": job_a,
            "actor": user_b.model_dump(mode="json"),
        },
    )
    assert r_idor.status_code == 404
    assert r_idor.json()["errorCode"] == "ACTION_NOT_FOUND"


@pytest.mark.asyncio
async def test_proxy_unlisted_delegator_and_operation_scope_enforced(isolated_system):
    """Proxy with unlisted delegatedBy fails closed; read_only & proxy cannot execute disallowed operations."""
    dispatcher, modes, _, _, _, _ = isolated_system

    rogue_proxy = ActorContext(
        actorGuid="proxy_n8n_intake",
        actorKind=ActorKind.PROXY,
        meetingContext={"source": "n8n"},
        portContext={"port": "n8n"},
        permissionProvenance={"role": "WORKFLOW_PROXY", "delegatedBy": "unlisted_user_xyz"},
    )
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_unmapped_proxy",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="p_fail_1",
                payload={"messageBody": "hi"},
                recipients=["customer-a@allowlist.test.local"],
                actor=rogue_proxy,
            )
        )

    # Read-only user cannot invoke write/dispatch operations
    readonly_user = _make_actor("user_alpha", scope="read_only")
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_ro_test",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="ro_1",
                payload={"messageBody": "hi"},
                recipients=["customer-a@allowlist.test.local"],
                actor=readonly_user,
            )
        )

    # Registered proxy with valid delegator CANNOT call human gate:barge_in
    valid_user = _make_actor("user_alpha")
    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_proxy_scope",
            subjectLineDisplay="Proxy Scope",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="ps_1",
            payload={"messageBody": "ok"},
            recipients=["customer-a@allowlist.test.local"],
            actor=valid_user,
        )
    )
    valid_proxy = ActorContext(
        actorGuid="proxy_n8n_intake",
        actorKind=ActorKind.PROXY,
        meetingContext={"source": "n8n"},
        portContext={"port": "n8n"},
        permissionProvenance={"role": "WORKFLOW_PROXY", "delegatedBy": "user_alpha"},
    )
    with pytest.raises(UnauthorizedError):
        await modes.barge_in(actor=valid_proxy, conversation_id="conv_proxy_scope")


@pytest.mark.asyncio
async def test_unexpected_adapter_exception_never_leaves_action_stuck_in_dispatching(isolated_system):
    """Unhandled KeyError/RuntimeError in adapter transitions action to FAILED(unknown) and conv to FAILED_CLOSED."""
    dispatcher, _, _, store, history, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")

    async def exploding_execute(op, payload, idem):
        raise KeyError("missing_required_field")

    mock_adapters[EngineName.CALNODE].execute = exploding_execute

    with pytest.raises(DownstreamOutcomeUnknownError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_explode",
                subjectLineDisplay="Explode Test",
                engine=EngineName.CALNODE,
                operation="create_booking",
                idempotencyKey="exp_1",
                payload={},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )

    # Zero actions left stuck in DISPATCHING
    inflight = await store.get_inflight_actions("conv_explode")
    assert inflight == []

    action = await store.get_action_by_idempotency_key("conv_explode", "exp_1")
    assert action["state"] == ActionState.FAILED.value

    conv = await store.get_conversation("conv_explode")
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value
    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.UNKNOWN
        for e in history.events
    )


@pytest.mark.asyncio
async def test_failed_closed_blocks_idempotency_replay(isolated_system):
    """While FAILED_CLOSED, even replaying a previously DONE idempotency key raises GateFailedClosedError."""
    dispatcher, _, _, store, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    sub = ActionSubmission(
        conversationId="conv_fc_replay",
        subjectLineDisplay="FC Replay",
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="fc_k1",
        payload={"messageBody": "hi"},
        recipients=["customer-a@allowlist.test.local"],
        actor=user_a,
    )
    res = await dispatcher.submit_action(sub)
    assert res["state"] == ActionState.DONE.value

    await store.update_conversation_mode("conv_fc_replay", ConversationMode.FAILED_CLOSED)
    with pytest.raises(GateFailedClosedError):
        await dispatcher.submit_action(sub)


@pytest.mark.asyncio
async def test_barge_in_reconciles_inflight_approved_action_and_interrupts_ungate(isolated_system):
    """barge_in while GATED waits for an in-flight approve_action; approve/reject blocked in TRANSITIONING; unknown resolution rejected."""
    dispatcher, modes, _, store, _, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")
    conv_id = "conv_mode_races"

    await store.get_or_create_conversation(conv_id, "Race Modes", owner_guid="user_alpha")
    await modes.barge_in(actor=user_a, conversation_id=conv_id)

    h1 = await dispatcher.submit_action(
        ActionSubmission(
            conversationId=conv_id,
            engine=EngineName.CALNODE,
            operation="create_booking",
            idempotencyKey="rm_1",
            payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    h2 = await dispatcher.submit_action(
        ActionSubmission(
            conversationId=conv_id,
            engine=EngineName.CALNODE,
            operation="create_booking",
            idempotencyKey="rm_2",
            payload={"slotId": "slot_2", "attendeeEmail": "customer-a@allowlist.test.local"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )

    # Unknown / typo'd actionId in return_to_ungated resolutions raises ActionNotFoundError
    with pytest.raises(ActionNotFoundError):
        await modes.return_to_ungated(
            actor=user_a,
            conversation_id=conv_id,
            resolutions=[
                ActionResolution(actionId=h1["actionId"], decision=ReleaseDecision.RELEASE),
                ActionResolution(actionId=h2["actionId"], decision=ReleaseDecision.CANCEL),
                ActionResolution(actionId="act_typo_does_not_exist", decision=ReleaseDecision.CANCEL),
            ],
        )

    # Slow execution during return_to_ungated: test concurrent approve rejected & barge_in interrupts remaining items
    started = asyncio.Event()
    finish = asyncio.Event()

    async def slow_book(op, payload, idem):
        started.set()
        await finish.wait()
        return {"bookingId": f"bkg_{idem}", "status": "confirmed"}

    mock_adapters[EngineName.CALNODE].execute = slow_book

    ungate_task = asyncio.create_task(
        modes.return_to_ungated(
            actor=user_a,
            conversation_id=conv_id,
            resolutions=[
                ActionResolution(actionId=h1["actionId"], decision=ReleaseDecision.RELEASE),
                ActionResolution(actionId=h2["actionId"], decision=ReleaseDecision.RELEASE),
            ],
        )
    )
    await started.wait()

    # While TRANSITIONING, reject_action on h2 MUST fail with InvalidModeTransitionError
    with pytest.raises(InvalidModeTransitionError):
        await modes.reject_action(conv_id, h2["actionId"], actor=user_a)

    # Concurrent barge_in during TRANSITIONING reconciles h1 and stops h2 from being released!
    barge_task = asyncio.create_task(modes.barge_in(actor=user_a, conversation_id=conv_id))
    await asyncio.sleep(0.05)
    finish.set()

    barge_res = await barge_task
    ungate_res = await ungate_task

    assert barge_res["mode"] == ConversationMode.GATED.value
    assert len(barge_res["reconciledInFlight"]) == 1
    assert barge_res["reconciledInFlight"][0]["actionId"] == h1["actionId"]
    assert barge_res["reconciledInFlight"][0]["undone"] is False
    assert ungate_res["interruptedByBargeIn"] is True

    # h2 MUST still be HELD (not dispatched after barge_in!)
    held_remaining = await store.get_held_actions(conv_id)
    assert [a["action_id"] for a in held_remaining] == [h2["actionId"]]


def test_fail_closed_config_defaults_and_no_memory_duplicate_when_jetstream_required():
    """GateConfig defaults fail-closed (gate_require_auth=True, jetstream_required=True, auto_create=False)."""
    cfg = GateConfig()
    assert cfg.gate_require_auth is True
    assert cfg.jetstream_required is True
    assert cfg.jetstream_auto_create_stream is False

    pub = JetStreamHistoryPublisher(cfg)
    assert pub.fallback is None


@pytest.mark.asyncio
async def test_authorize_read_operation_persists_owner_across_restart(isolated_system):
    """Read-initialized conversation persists owner_guid to SQLite so restart restores ACL."""
    _, modes, _, store, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    await modes.authorize_read_operation(
        user_a, "conv_read_owner_persist", "calnode:list_slots", subject_line_display="Slots Read"
    )
    owners = await store.list_conversation_owners()
    assert owners.get("conv_read_owner_persist") == "user_alpha"


@pytest.mark.asyncio
async def test_dispatch_allowlist_bypass_via_payload_recipients_blocked_without_row_creation(isolated_system):
    """Unlisted recipients hidden inside payload are rejected BEFORE creating any conversation row."""
    from gate.errors import RecipientNotAllowlistedError

    dispatcher, _, _, store, _, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")

    # Case 1: empty recipients[] + unlisted email in payload -> blocked & no rogue row
    with pytest.raises(RecipientNotAllowlistedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_bypass_1",
                subjectLineDisplay="Bypass 1",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="bp_1",
                payload={"recipientEmail": "attacker@evil.example", "messageBody": "hi"},
                recipients=[],
                actor=user_a,
            )
        )
    assert await store.get_conversation("conv_bypass_1") is None
    assert len(mock_adapters[EngineName.N8N].call_log) == 0

    # Case 2: decoy allowlisted recipient + unlisted attendeeEmail in payload -> blocked
    with pytest.raises(RecipientNotAllowlistedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_bypass_2",
                engine=EngineName.CALNODE,
                operation="create_booking",
                idempotencyKey="bp_2",
                payload={"attendeeEmail": "unlisted@evil.example", "slotId": "2026-11-18T14:00:00Z"},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )
    assert await store.get_conversation("conv_bypass_2") is None

    # Case 3: controlled outbound op with zero recipients anywhere -> blocked
    with pytest.raises(RecipientNotAllowlistedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_bypass_3",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey="bp_3",
                payload={"messageBody": "no recipient supplied"},
                recipients=[],
                actor=user_a,
            )
        )
    assert await store.get_conversation("conv_bypass_3") is None


@pytest.mark.asyncio
async def test_windmill_arbitrary_script_path_and_calnode_calendar_escape_blocked(test_config, isolated_system):
    """Windmill script allowlist and Calnode bounded-calendar checks reject scope escapes & traversal."""
    from adapters.calnode import CalnodeAdapter
    from adapters.windmill import WindmillAdapter

    wm = WindmillAdapter(test_config)
    with pytest.raises(UnauthorizedError):
        await wm.execute("start_assignment", {"scriptPath": "f/other/evil_script"}, "wm_esc_1")
    with pytest.raises(UnauthorizedError):
        await wm.execute("start_assignment", {"scriptPath": "../admin/reset"}, "wm_esc_2")

    cal = CalnodeAdapter(test_config)
    with pytest.raises(UnauthorizedError):
        await cal.list_slots(calendar_id="other-unbounded-cal")
    with pytest.raises(UnauthorizedError):
        await cal.execute(
            "create_booking",
            {"calendarId": "../v1/admin", "slotId": "2026-11-18T14:00:00Z", "attendeeEmail": "customer-a@allowlist.test.local"},
            "cal_esc_1",
        )

    # Through GateDispatcher, adapter UnauthorizedError is recorded as FAILED(denied)
    dispatcher, _, _, store, history, mock_adapters = isolated_system
    mock_adapters[EngineName.WINDMILL] = wm
    user_a = _make_actor("user_alpha")
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_wm_escape",
                engine=EngineName.WINDMILL,
                operation="start_assignment",
                idempotencyKey="wm_esc_gate_1",
                payload={
                    "scriptPath": "f/other/evil",
                    "bookingId": "bkg_1",
                    "conversationId": "conv_wm_escape",
                    "assignee": "ops-specialist-1@allowlist.test.local",
                },
                recipients=["ops-specialist-1@allowlist.test.local"],
                actor=user_a,
            )
        )
    action = await store.get_action_by_idempotency_key("conv_wm_escape", "wm_esc_gate_1")
    assert action["state"] == ActionState.FAILED.value
    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.DENIED
        for e in history.events
    )


@pytest.mark.asyncio
async def test_n8n_adapter_does_not_send_admin_api_key_on_webhooks(test_config):
    """Least privilege: n8n webhook calls send X-SAFI-Webhook-Secret only, never X-N8N-API-KEY."""
    import httpx
    from adapters.n8n import N8nAdapter

    captured_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_headers.update(dict(request.headers))
        return httpx.Response(200, json={"status": "dispatched", "executionId": "99"})

    transport = httpx.MockTransport(handler)
    cfg = GateConfig(n8n_api_key="admin-n8n-key", n8n_webhook_secret="webhook-only-secret")
    async with httpx.AsyncClient(transport=transport) as client:
        adapter = N8nAdapter(cfg, http_client=client)
        res = await adapter.execute(
            "dispatch_followup",
            {"conversationId": "conv_n8n_lp", "recipientEmail": "customer-a@allowlist.test.local", "messageBody": "hi"},
            "lp_1",
        )
        assert res["status"] == "dispatched"

    lower_headers = {k.lower(): v for k, v in captured_headers.items()}
    assert lower_headers.get("x-safi-webhook-secret") == "webhook-only-secret"
    assert "x-n8n-api-key" not in lower_headers


@pytest.mark.asyncio
async def test_shared_sqlite_concurrent_writes_no_transaction_interleaving(isolated_system):
    """20 concurrent conversations writing actions simultaneously never interleave transactions."""
    dispatcher, _, _, store, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    async def run_one(i: int):
        return await dispatcher.submit_action(
            ActionSubmission(
                conversationId=f"conv_conc_{i}",
                subjectLineDisplay=f"Concurrent {i}",
                engine=EngineName.N8N,
                operation="dispatch_followup",
                idempotencyKey=f"conc_key_{i}",
                payload={"messageBody": f"msg {i}"},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )

    results = await asyncio.gather(*(run_one(i) for i in range(20)))
    assert len(results) == 20
    assert all(r["state"] == ActionState.DONE.value for r in results)

    for i in range(20):
        rec = await store.get_idempotency_record(f"conv_conc_{i}", f"conc_key_{i}")
        assert rec is not None
        assert rec["outcome"] == Outcome.OK.value


@pytest.mark.asyncio
async def test_approve_action_toctou_race_reconciled_by_barge_in(isolated_system):
    """approve_action marks action in-flight inside state_lock so concurrent barge_in reconciles it."""
    dispatcher, modes, _, store, _, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")
    conv_id = "conv_approve_race"

    await store.get_or_create_conversation(conv_id, "Approve Race", owner_guid="user_alpha")
    await modes.barge_in(actor=user_a, conversation_id=conv_id)

    held = await dispatcher.submit_action(
        ActionSubmission(
            conversationId=conv_id,
            engine=EngineName.CALNODE,
            operation="create_booking",
            idempotencyKey="ar_1",
            payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    assert held["state"] == ActionState.HELD.value

    started = asyncio.Event()
    finish = asyncio.Event()

    async def slow_book(op, payload, idem):
        started.set()
        await finish.wait()
        return {"bookingId": "bkg_approve_race", "status": "confirmed"}

    mock_adapters[EngineName.CALNODE].execute = slow_book

    approve_task = asyncio.create_task(modes.approve_action(conv_id, held["actionId"], actor=user_a))
    await started.wait()

    barge_task = asyncio.create_task(modes.barge_in(actor=user_a, conversation_id=conv_id))
    await asyncio.sleep(0.05)
    finish.set()

    approve_res = await approve_task
    barge_res = await barge_task

    assert approve_res["state"] == ActionState.DONE.value
    assert barge_res["mode"] == ConversationMode.GATED.value
    assert len(barge_res["reconciledInFlight"]) == 1
    assert barge_res["reconciledInFlight"][0]["actionId"] == held["actionId"]
    assert barge_res["reconciledInFlight"][0]["undone"] is False


@pytest.mark.asyncio
async def test_return_to_ungated_store_error_fails_closed_not_gated(isolated_system):
    """StoreError during return_to_ungated transitions conversation to FAILED_CLOSED (never reverts to GATED)."""
    dispatcher, modes, _, store, history, _ = isolated_system
    user_a = _make_actor("user_alpha")
    conv_id = "conv_ungate_store_err"

    await store.get_or_create_conversation(conv_id, "Ungate StoreErr", owner_guid="user_alpha")
    await modes.barge_in(actor=user_a, conversation_id=conv_id)

    orig_publish = history.record_control_event

    async def failing_publish(*args, **kwargs):
        if kwargs.get("event_type") == ControlEventType.MODE_UNGATED:
            raise StoreError("Simulated JetStream publish failure on MODE_UNGATED")
        return await orig_publish(*args, **kwargs)

    history.record_control_event = failing_publish

    with pytest.raises(StoreError):
        await modes.return_to_ungated(actor=user_a, conversation_id=conv_id, resolutions=[])

    conv = await store.get_conversation(conv_id)
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value


@pytest.mark.asyncio
async def test_reconcile_to_ungated_blocked_when_held_actions_exist(isolated_system):
    """Reconciling FAILED_CLOSED directly to UNGATED is blocked if HELD actions exist; GATED succeeds."""
    from gate.errors import HeldActionsUnresolvedError

    dispatcher, modes, _, store, _, _ = isolated_system
    user_a = _make_actor("user_alpha")
    conv_id = "conv_reconcile_held"

    await store.get_or_create_conversation(conv_id, "Reconcile Held", owner_guid="user_alpha")
    await modes.barge_in(actor=user_a, conversation_id=conv_id)
    held = await dispatcher.submit_action(
        ActionSubmission(
            conversationId=conv_id,
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="rh_1",
            payload={"messageBody": "held"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )
    assert held["state"] == ActionState.HELD.value

    await store.update_conversation_mode(conv_id, ConversationMode.FAILED_CLOSED)

    with pytest.raises(HeldActionsUnresolvedError):
        await modes.reconcile_failed_closed(
            actor=user_a, conversation_id=conv_id, target_mode=ConversationMode.UNGATED
        )

    rec = await modes.reconcile_failed_closed(
        actor=user_a, conversation_id=conv_id, target_mode=ConversationMode.GATED
    )
    assert rec["mode"] == ConversationMode.GATED.value
    assert len(await store.get_held_actions(conv_id)) == 1


@pytest.mark.asyncio
async def test_nonexistent_conversation_barge_in_does_not_squat_acl(isolated_system):
    """Calling barge_in on a nonexistent conversationId raises 404 without pinning ownership in ACL."""
    from gate.errors import ConversationNotFoundError

    _, modes, authz, _, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    with pytest.raises(ConversationNotFoundError):
        await modes.barge_in(actor=user_a, conversation_id="conv_never_existed")

    assert "conv_never_existed" not in authz.conversation_access_map


@pytest.mark.asyncio
async def test_downstream_unknown_error_exposes_real_action_id_and_engine(isolated_system):
    """DownstreamOutcomeUnknownError carries real Gate actionId ('act_...') and engine name."""
    dispatcher, _, _, _, _, mock_adapters = isolated_system
    user_a = _make_actor("user_alpha")

    async def timeout_execute(op, payload, idem):
        raise DownstreamOutcomeUnknownError("calnode", "HTTP timeout")

    mock_adapters[EngineName.CALNODE].execute = timeout_execute

    with pytest.raises(DownstreamOutcomeUnknownError) as exc_info:
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_unknown_aid",
                engine=EngineName.CALNODE,
                operation="create_booking",
                idempotencyKey="uaid_1",
                payload={"attendeeEmail": "customer-a@allowlist.test.local"},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )
    assert exc_info.value.details["actionId"].startswith("act_")
    assert exc_info.value.details["engine"] == "calnode"


@pytest.mark.asyncio
async def test_concurrent_barge_in_emits_mode_gated_once(isolated_system):
    """Concurrent barge_in calls on the same conversation emit MODE_GATED exactly once."""
    _, modes, _, store, history, _ = isolated_system
    user_a = _make_actor("user_alpha")
    conv_id = "conv_conc_barge"

    await store.get_or_create_conversation(conv_id, "Concurrent Barge", owner_guid="user_alpha")

    r1, r2 = await asyncio.gather(
        modes.barge_in(actor=user_a, conversation_id=conv_id),
        modes.barge_in(actor=user_a, conversation_id=conv_id),
    )
    assert r1["mode"] == ConversationMode.GATED.value
    assert r2["mode"] == ConversationMode.GATED.value

    mode_gated_events = [
        e for e in history.events
        if e.conversationId == conv_id and e.type == ControlEventType.MODE_GATED
    ]
    assert len(mode_gated_events) == 1


@pytest.mark.asyncio
async def test_cross_engine_idempotency_conflict_rejected(isolated_system):
    """Same (conversationId, idempotencyKey, operation) on a DIFFERENT engine raises IdempotencyConflictError."""
    dispatcher, _, _, _, _, _ = isolated_system
    user_a = _make_actor("user_alpha")

    await dispatcher.submit_action(
        ActionSubmission(
            conversationId="conv_eng_conflict",
            engine=EngineName.N8N,
            operation="dispatch_followup",
            idempotencyKey="same_k",
            payload={"messageBody": "hi"},
            recipients=["customer-a@allowlist.test.local"],
            actor=user_a,
        )
    )

    with pytest.raises(IdempotencyConflictError):
        await dispatcher.submit_action(
            ActionSubmission(
                conversationId="conv_eng_conflict",
                engine=EngineName.CALNODE,
                operation="dispatch_followup",
                idempotencyKey="same_k",
                payload={"messageBody": "hi"},
                recipients=["customer-a@allowlist.test.local"],
                actor=user_a,
            )
        )
