"""Tests for persistent local SQLite operational store (R-CTX-03, R-GATE-04)."""

import pytest

from gate.errors import AmbiguousSubjectLineError, ConversationNotFoundError
from gate.models import ActionState, ConversationMode, EngineName


@pytest.mark.asyncio
async def test_subject_line_resolution_exact(gate_store):
    await gate_store.get_or_create_conversation("conv_01", "Customer A - Follow up")
    res = await gate_store.resolve_conversation_by_subject("Customer A - Follow up")
    assert res["conversation_id"] == "conv_01"


@pytest.mark.asyncio
async def test_subject_line_resolution_not_found(gate_store):
    with pytest.raises(ConversationNotFoundError):
        await gate_store.resolve_conversation_by_subject("Nonexistent Subject")


@pytest.mark.asyncio
async def test_subject_line_ambiguity_forces_user_selection(gate_store):
    # R-GATE-04: Subject Line is NOT assumed unique; ambiguity requires user selection
    await gate_store.get_or_create_conversation("conv_01", "Duplicate Subject")
    await gate_store.get_or_create_conversation("conv_02", "Duplicate Subject")

    with pytest.raises(AmbiguousSubjectLineError) as exc_info:
        await gate_store.resolve_conversation_by_subject("Duplicate Subject")

    candidates = exc_info.value.details["candidates"]
    assert len(candidates) == 2
    conv_ids = {c["conversationId"] for c in candidates}
    assert conv_ids == {"conv_01", "conv_02"}


@pytest.mark.asyncio
async def test_action_persistence_and_queries(gate_store, test_actor):
    await gate_store.get_or_create_conversation("conv_01", "Subject 1")
    action = await gate_store.record_action(
        action_id="act_100",
        conversation_id="conv_01",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotency_key="key_100",
        initial_state=ActionState.HELD,
        payload={"slotId": "slot_1"},
        recipients=["customer-a@allowlist.test.local"],
        actor_guid=test_actor.actorGuid,
        actor_kind=test_actor.actorKind.value,
        context=test_actor.to_event_context(),
    )
    assert action.state == ActionState.HELD

    held = await gate_store.get_held_actions("conv_01")
    assert len(held) == 1
    assert held[0]["action_id"] == "act_100"

    await gate_store.update_action_state("act_100", ActionState.APPROVED)
    await gate_store.update_action_state("act_100", ActionState.AUTHORIZED)
    await gate_store.update_action_state("act_100", ActionState.DISPATCHING)
    await gate_store.update_action_state("act_100", ActionState.DONE, result={"bookingId": "b1"})
    held_after = await gate_store.get_held_actions("conv_01")
    assert len(held_after) == 0

    fetched = await gate_store.get_action("act_100")
    assert fetched["state"] == ActionState.DONE.value
    assert fetched["result"] == {"bookingId": "b1"}
