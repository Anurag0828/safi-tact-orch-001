"""Tests for Authorization, Revocation, and Scope enforcement (A09, R-AUTH-01, R-AUTH-02, R-GATE-09)."""

import pytest

from gate.dispatch import GateDispatcher
from gate.errors import UnauthorizedError
from gate.models import (
    ActionSubmission,
    ActorContext,
    ActorKind,
    ControlEventType,
    EngineName,
    Outcome,
)
from gate.modes import GateModeController
from gate.state_machine import ConversationRuntimeRegistry


@pytest.fixture
def authz_suite(test_config, gate_store, history_publisher, authz_provider, mock_adapters):
    runtimes = ConversationRuntimeRegistry()
    dispatcher = GateDispatcher(test_config, gate_store, history_publisher, authz_provider, runtimes, mock_adapters)
    modes = GateModeController(test_config, gate_store, history_publisher, authz_provider, runtimes, dispatcher)
    return dispatcher, modes, authz_provider, history_publisher


@pytest.mark.asyncio
async def test_a09_unauthorized_user_blocked_from_barge_in_and_action(
    authz_suite, test_actor
):
    """A09 / R-AUTH-01: Unauthorized user cannot Gate/Ungate or access another conversation."""
    dispatcher, modes, authz_provider, _ = authz_suite
    conv_id = "conv_restricted_01"

    await dispatcher.store.get_or_create_conversation(conv_id, "Secret Meeting")
    # Explicitly permit only user_test_guid_001
    authz_provider.authorize_actor_for_conversation(conv_id, test_actor.actorGuid)

    unauthorized_actor = ActorContext(
        actorGuid="unauthorized_attacker_guid",
        actorKind=ActorKind.USER,
    )

    # Cannot Barge In
    with pytest.raises(UnauthorizedError):
        await modes.barge_in(actor=unauthorized_actor, conversation_id=conv_id)

    # Cannot submit action
    sub = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:authz:fail1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=unauthorized_actor,
    )
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(sub)


@pytest.mark.asyncio
async def test_a09_revoked_permission_blocks_subsequent_actions(
    authz_suite, test_actor, history_publisher
):
    """A09 / R-AUTH-02 / R-GATE-09: Revoked permission immediately blocks subsequent actions even if previously allowed."""
    dispatcher, _, authz_provider, _ = authz_suite
    conv_id = "conv_revocation_01"
    await dispatcher.store.get_or_create_conversation(conv_id, "Customer Revoke")

    sub1 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:revoke:1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    # First action passes
    res1 = await dispatcher.submit_action(sub1)
    assert res1["state"] == "DONE"

    # Now revoke permissions for this actor (R-AUTH-02)
    authz_provider.revoke_actor(test_actor.actorGuid)

    sub2 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:revoke:2",
        payload={"slotId": "slot_2", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    with pytest.raises(UnauthorizedError):
        await dispatcher.submit_action(sub2)

    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.DENIED
        for e in history_publisher.events
    )
