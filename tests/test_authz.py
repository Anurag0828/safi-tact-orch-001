"""Tests for Authorization, Revocation, Fail-Closed Identity, and Scope enforcement (A09, R-AUTH-01/02, R-GATE-09, R-SEC-04)."""

from __future__ import annotations

import httpx
import pytest

from gate.authz import HttpPermissionProvider, LocalPolicyPermissionProvider
from gate.config import GateConfig
from gate.dispatch import GateDispatcher
from gate.errors import (
    IdentityUncertainError,
    PermissionCheckFailedError,
    UnauthorizedError,
)
from gate.models import (
    ActionResolution,
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


@pytest.fixture
def authz_suite(test_config, gate_store, history_publisher, authz_provider, mock_adapters):
    runtimes = ConversationRuntimeRegistry()
    dispatcher = GateDispatcher(
        test_config, gate_store, history_publisher, authz_provider, runtimes, mock_adapters
    )
    modes = GateModeController(
        test_config, gate_store, history_publisher, authz_provider, runtimes, dispatcher
    )
    return dispatcher, modes, authz_provider, history_publisher


def _other_actor(guid: str = "unauthorized_attacker_guid") -> ActorContext:
    return ActorContext(
        actorGuid=guid,
        actorKind=ActorKind.USER,
        meetingContext={"meetingId": "mtg_other"},
        portContext={"port": "safichat-web"},
        permissionProvenance={"role": "OTHER_USER"},
    )


@pytest.mark.asyncio
async def test_default_deny_when_acl_empty(test_actor):
    """R-SEC-04 / A09: Empty LocalPolicyPermissionProvider denies all actors by default (never fail-open)."""
    provider = LocalPolicyPermissionProvider()
    with pytest.raises(UnauthorizedError):
        await provider.check_permission(test_actor, "conv_any", "gate:barge_in")


@pytest.mark.asyncio
async def test_http_provider_fails_closed_when_unconfigured(test_actor):
    """R-SEC-04: HttpPermissionProvider with empty SAFI_AUTHZ_URL and AUTHZ_MODE=http fails closed."""
    cfg = GateConfig(safi_authz_url="", authz_mode="http")
    provider = HttpPermissionProvider(cfg)
    with pytest.raises(PermissionCheckFailedError):
        await provider.check_permission(test_actor, "conv_01", "gate:barge_in")


@pytest.mark.asyncio
async def test_http_provider_responses(test_actor):
    """HttpPermissionProvider handles allow, deny, 401 uncertain, bad JSON, and network failure."""
    responses = [
        httpx.Response(200, json={"allowed": True}),
        httpx.Response(200, json={"allowed": False}),
        httpx.Response(401, text="expired token"),
        httpx.Response(200, text="not-json"),
    ]
    idx = 0

    def handler(_req: httpx.Request) -> httpx.Response:
        nonlocal idx
        resp = responses[idx]
        idx += 1
        return resp

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    cfg = GateConfig(safi_authz_url="http://authz.local", safi_authz_token="tok")
    provider = HttpPermissionProvider(cfg, http_client=client)

    assert await provider.check_permission(test_actor, "c1", "op1") is True
    with pytest.raises(UnauthorizedError):
        await provider.check_permission(test_actor, "c1", "op1")
    with pytest.raises(IdentityUncertainError):
        await provider.check_permission(test_actor, "c1", "op1")
    with pytest.raises(PermissionCheckFailedError):
        await provider.check_permission(test_actor, "c1", "op1")
    await client.aclose()


@pytest.mark.asyncio
async def test_a09_unauthorized_user_blocked_from_barge_in_gated_inject_and_replay(
    authz_suite, test_actor
):
    """A09 / R-AUTH-01: Unauthorized user cannot Barge In, inject HELD into GATED conv, or read cached replays."""
    dispatcher, modes, authz_provider, _ = authz_suite
    conv_id = "conv_restricted_01"

    await dispatcher.store.get_or_create_conversation(conv_id, "Secret Meeting")
    authz_provider.authorize_actor_for_conversation(conv_id, test_actor.actorGuid)

    attacker = _other_actor()

    # 1. Owner completes an action so an idempotency record exists
    owner_sub = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:authz:owner1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    owner_res = await dispatcher.submit_action(owner_sub)
    assert owner_res["state"] == "DONE"

    # 2. Attacker cannot read cached replay via same idempotencyKey
    attacker_replay = owner_sub.model_copy(update={"actor": attacker})
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(attacker_replay)

    # 3. Attacker cannot Barge In
    with pytest.raises(UnauthorizedError):
        await modes.barge_in(actor=attacker, conversation_id=conv_id)

    # 4. Owner barges in -> GATED
    await modes.barge_in(actor=test_actor, conversation_id=conv_id)

    # 5. Attacker CANNOT inject a HELD action into the GATED conversation
    attacker_inject = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:authz:inject_gated",
        payload={"slotId": "slot_2", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=attacker,
    )
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(attacker_inject)

    held = await modes.list_held_actions(actor=test_actor, conversation_id=conv_id)
    assert len(held["heldActions"]) == 0


@pytest.mark.asyncio
async def test_a09_revoked_permission_blocks_subsequent_and_held_release(
    authz_suite, test_actor, mock_adapters, history_publisher
):
    """A09 / R-AUTH-02 / R-GATE-09: Revoked permission blocks subsequent actions AND previously HELD releases without deadlock."""
    dispatcher, modes, authz_provider, _ = authz_suite
    conv_id = "conv_revocation_01"
    await dispatcher.store.get_or_create_conversation(conv_id, "Customer Revoke")
    await modes.barge_in(actor=test_actor, conversation_id=conv_id)

    # Hold an action while authorized
    sub_held = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:revoke:held1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    held_res = await dispatcher.submit_action(sub_held)
    assert held_res["state"] == "HELD"

    # Now revoke actor permissions
    authz_provider.revoke_actor(test_actor.actorGuid)

    # Cannot approve held action
    with pytest.raises(UnauthorizedError):
        await modes.approve_action(conv_id, held_res["actionId"], actor=test_actor)

    # Cannot ungate/release held action
    with pytest.raises(UnauthorizedError):
        await modes.return_to_ungated(
            actor=test_actor,
            conversation_id=conv_id,
            resolutions=[
                ActionResolution(
                    actionId=held_res["actionId"], decision=ReleaseDecision.RELEASE
                )
            ],
        )

    assert len(mock_adapters[EngineName.CALNODE].call_log) == 0
    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.DENIED
        for e in history_publisher.events
    )


@pytest.mark.asyncio
async def test_identity_uncertain_fails_closed_in_modes_and_dispatch(
    authz_suite, gate_store, history_publisher
):
    """R-SEC-04 / R-CTX-01: Authorized actor with missing Meeting/Port Context raises IdentityUncertainError and fails closed."""
    dispatcher, modes, authz_provider, _ = authz_suite
    conv_id = "conv_identity_uncertain"
    await gate_store.get_or_create_conversation(conv_id, "Uncertain Identity", owner_guid="user_incomplete_01")
    authz_provider.authorize_actor_for_conversation(conv_id, "user_incomplete_01")

    incomplete_actor = ActorContext(
        actorGuid="user_incomplete_01",
        actorKind=ActorKind.USER,
        # Missing meetingContext, portContext, permissionProvenance
    )

    with pytest.raises(IdentityUncertainError):
        await modes.barge_in(actor=incomplete_actor, conversation_id=conv_id)

    conv = await gate_store.get_conversation(conv_id)
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value
    assert any(
        e.type == ControlEventType.GATE_FAILED_CLOSED
        and e.conversationId == conv_id
        for e in history_publisher.events
    )


@pytest.mark.asyncio
async def test_proxy_actor_blocked_when_delegator_revoked(
    authz_suite, test_actor, proxy_actor
):
    """R-AUTH-02: Proxy acting on behalf of a revoked user is immediately blocked."""
    dispatcher, _, authz_provider, _ = authz_suite
    conv_id = "conv_proxy_revoke"
    await dispatcher.store.get_or_create_conversation(conv_id, "Proxy Revoke")
    authz_provider.authorize_actor_for_conversation(conv_id, test_actor.actorGuid)
    authz_provider.revoke_actor(test_actor.actorGuid)

    sub = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:proxy:revoked",
        payload={"messageBody": "Hi"},
        recipients=["customer-a@allowlist.test.local"],
        actor=proxy_actor,
    )
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(sub)
