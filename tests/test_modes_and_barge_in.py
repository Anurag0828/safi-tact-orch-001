"""Tests for Selective Gated, Barge In, Return to Ungated, and Recovery (A06, A07, A08, A10)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from gate.dispatch import GateDispatcher
from gate.errors import (
    HeldActionsUnresolvedError,
    InvalidModeTransitionError,
)
from gate.models import (
    ActionResolution,
    ActionState,
    ActionSubmission,
    ControlEventType,
    ConversationMode,
    EngineName,
    Outcome,
    ReleaseDecision,
)
from gate.modes import GateModeController
from gate.state_machine import ConversationRuntimeRegistry
from gate.store import GateStore


@pytest.fixture
def gate_system(test_config, gate_store, history_publisher, authz_provider, mock_adapters):
    runtimes = ConversationRuntimeRegistry()
    dispatcher = GateDispatcher(
        config=test_config,
        store=gate_store,
        history=history_publisher,
        authz=authz_provider,
        runtimes=runtimes,
        adapters=mock_adapters,
    )
    modes = GateModeController(
        config=test_config,
        store=gate_store,
        history=history_publisher,
        authz=authz_provider,
        runtimes=runtimes,
        dispatcher=dispatcher,
    )
    return dispatcher, modes, runtimes


@pytest.mark.asyncio
async def test_a06_selective_gated_across_three_conversations(
    gate_system, mock_adapters, test_actor
):
    """A06 / Invariant 5: With 3 conversations active, Gate one by Subject Line; one action waits (HELD); other 2 continue."""
    dispatcher, modes, _ = gate_system

    # Set up 3 conversations
    await dispatcher.store.get_or_create_conversation("conv_A", "Customer A - Alpha")
    await dispatcher.store.get_or_create_conversation("conv_B", "Customer B - Beta")
    await dispatcher.store.get_or_create_conversation("conv_C", "Customer C - Gamma")

    # Barge In on Customer B by Subject Line (R-GATE-04)
    barge_res = await modes.barge_in(actor=test_actor, subject_line="Customer B - Beta")
    assert barge_res["conversationId"] == "conv_B"
    assert barge_res["mode"] == ConversationMode.GATED.value

    # Submit actions to all 3 conversations
    sub_a = ActionSubmission(
        conversationId="conv_A",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:conv_a:act1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    sub_b = ActionSubmission(
        conversationId="conv_B",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:conv_b:act1",
        payload={"slotId": "slot_2", "attendeeEmail": "customer-b@allowlist.test.local"},
        recipients=["customer-b@allowlist.test.local"],
        actor=test_actor,
    )
    sub_c = ActionSubmission(
        conversationId="conv_C",
        engine=EngineName.WINDMILL,
        operation="start_assignment",
        idempotencyKey="idem:conv_c:act1",
        payload={"bookingId": "bkg_c", "assignee": "ops-specialist-1@allowlist.test.local"},
        recipients=["ops-specialist-1@allowlist.test.local"],
        actor=test_actor,
    )

    res_a = await dispatcher.submit_action(sub_a)
    res_b = await dispatcher.submit_action(sub_b)
    res_c = await dispatcher.submit_action(sub_c)

    # Conversation A and C executed and finished DONE
    assert res_a["state"] == ActionState.DONE.value
    assert res_c["state"] == ActionState.DONE.value

    # Conversation B was HELD (gated)
    assert res_b["state"] == ActionState.HELD.value

    # Confirm held list for B
    held_b = await modes.list_held_actions(actor=test_actor, conversation_id="conv_B")
    assert len(held_b["heldActions"]) == 1
    assert held_b["heldActions"][0]["actionId"] == res_b["actionId"]


@pytest.mark.asyncio
async def test_a07_ungate_release_cancel_at_most_once(
    gate_system, mock_adapters, history_publisher, test_actor
):
    """A07: Return to ungated: release one, cancel another; verify at-most-once execution per approved idempotency key."""
    dispatcher, modes, _ = gate_system
    conv_id = "conv_ungate_demo"

    # Put conversation into GATED mode
    await dispatcher.store.get_or_create_conversation(conv_id, "Subject Demo")
    await modes.barge_in(actor=test_actor, conversation_id=conv_id)

    # Submit 2 actions -> both become HELD
    sub1 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:ungate:book1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    sub2 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:ungate:followup1",
        payload={"messageBody": "Hello Customer A"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )

    res1 = await dispatcher.submit_action(sub1)
    res2 = await dispatcher.submit_action(sub2)
    assert res1["state"] == ActionState.HELD.value
    assert res2["state"] == ActionState.HELD.value

    # Attempt to return to ungated without resolving all held actions -> MUST FAIL (R-GATE-06)
    with pytest.raises(HeldActionsUnresolvedError):
        await modes.return_to_ungated(
            actor=test_actor,
            conversation_id=conv_id,
            resolutions=[ActionResolution(actionId=res1["actionId"], decision=ReleaseDecision.RELEASE)],
        )

    # Return to ungated: release res1, cancel res2
    ungate_res = await modes.return_to_ungated(
        actor=test_actor,
        conversation_id=conv_id,
        resolutions=[
            ActionResolution(actionId=res1["actionId"], decision=ReleaseDecision.RELEASE),
            ActionResolution(actionId=res2["actionId"], decision=ReleaseDecision.CANCEL),
        ],
    )

    assert ungate_res["mode"] == ConversationMode.UNGATED.value
    assert len(ungate_res["released"]) == 1
    assert ungate_res["released"][0]["state"] == ActionState.DONE.value
    assert ungate_res["cancelled"] == [res2["actionId"]]

    # Calnode executed once, n8n executed zero times (cancelled)
    assert len(mock_adapters[EngineName.CALNODE].call_log) == 1
    assert len(mock_adapters[EngineName.N8N].call_log) == 0

    # Verify event stream compliance
    event_types = [e.type for e in history_publisher.events if e.conversationId == conv_id]
    assert ControlEventType.ACTION_RELEASED in event_types
    assert ControlEventType.ACTION_CANCELLED in event_types
    assert ControlEventType.MODE_UNGATED in event_types


@pytest.mark.asyncio
async def test_a08_race_and_barge_in_in_flight_reconciliation(
    gate_system, mock_adapters, test_actor
):
    """A08 / Invariant 3 & 6: Mode change during dispatch confirmed only after started effects settle; never falsely undo."""
    dispatcher, modes, runtimes = gate_system
    conv_id = "conv_race_01"

    await dispatcher.store.get_or_create_conversation(conv_id, "Race Test Subject")

    # Hook Calnode adapter to delay so it is actively in-flight
    dispatch_started = asyncio.Event()
    dispatch_can_finish = asyncio.Event()

    async def slow_execute(op, payload, idem_key):
        dispatch_started.set()
        await dispatch_can_finish.wait()
        return {"bookingId": "bkg_slow_01", "status": "CONFIRMED"}

    mock_adapters[EngineName.CALNODE].execute = slow_execute

    sub = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:race:slow_book",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )

    # Launch dispatch task
    dispatch_task = asyncio.create_task(dispatcher.submit_action(sub))
    await dispatch_started.wait()

    # While in-flight, launch Barge In in another task
    barge_task = asyncio.create_task(modes.barge_in(actor=test_actor, conversation_id=conv_id))

    # Mode is TRANSITIONING while in-flight action settles
    await asyncio.sleep(0.05)
    conv_during = await dispatcher.store.get_conversation(conv_id)
    assert conv_during["mode"] == ConversationMode.TRANSITIONING.value

    # Submit Action #2 DURING TRANSITIONING -> must be HELD, NOT dispatched!
    sub2 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:race:during_transition",
        payload={"messageBody": "Must be held during transition"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    res2 = await dispatcher.submit_action(sub2)
    assert res2["state"] == ActionState.HELD.value

    # Allow downstream execution of Action #1 to complete
    dispatch_can_finish.set()
    dispatch_res = await dispatch_task
    barge_res = await barge_task

    assert dispatch_res["state"] == ActionState.DONE.value
    assert barge_res["mode"] == ConversationMode.GATED.value
    # Reconciled in-flight indicates action finished DONE and was NOT falsely promised undone (Invariant 6)
    assert len(barge_res["reconciledInFlight"]) == 1
    assert barge_res["reconciledInFlight"][0]["finalState"] == ActionState.DONE.value
    assert barge_res["reconciledInFlight"][0]["undone"] is False

    # Submit Action #3 AFTER GATED confirmed -> nothing new starts (Invariant 3)
    sub3 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:race:after_gated",
        payload={"messageBody": "Must be held after GATED"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    res3 = await dispatcher.submit_action(sub3)
    assert res3["state"] == ActionState.HELD.value
    assert len(mock_adapters[EngineName.N8N].call_log) == 0


@pytest.mark.asyncio
async def test_a07_approve_reject_and_concurrent_approve_no_double_dispatch(
    gate_system, mock_adapters, test_actor
):
    """A07: Approve & Reject work cleanly, and concurrent approve calls NEVER double-dispatch."""
    from gate.errors import InvalidActionStateError

    dispatcher, modes, _ = gate_system
    conv_id = "conv_approve_race"
    await dispatcher.store.get_or_create_conversation(conv_id, "Approve Race")
    await modes.barge_in(actor=test_actor, conversation_id=conv_id)

    sub1 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:app:race1",
        payload={"messageBody": "Approve once"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    sub2 = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem:app:reject1",
        payload={"messageBody": "Reject me"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    h1 = await dispatcher.submit_action(sub1)
    h2 = await dispatcher.submit_action(sub2)

    # Reject h2 and verify retrying its idempotencyKey returns clean REJECTED replay (not 504 or 500)
    rej = await modes.reject_action(conv_id, h2["actionId"], actor=test_actor)
    assert rej["state"] == ActionState.REJECTED.value
    retry_rej = await dispatcher.submit_action(sub2)
    assert retry_rej["state"] == ActionState.REJECTED.value
    assert retry_rej["idempotentReplay"] is True

    # Slow n8n adapter to widen race window on concurrent approve
    orig_exec = mock_adapters[EngineName.N8N].execute

    async def delayed_exec(op, payload, idem_key):
        await asyncio.sleep(0.05)
        return await orig_exec(op, payload, idem_key)

    mock_adapters[EngineName.N8N].execute = delayed_exec

    results = await asyncio.gather(
        modes.approve_action(conv_id, h1["actionId"], actor=test_actor),
        modes.approve_action(conv_id, h1["actionId"], actor=test_actor),
        return_exceptions=True,
    )
    successes = [r for r in results if isinstance(r, dict)]
    errors = [r for r in results if isinstance(r, InvalidActionStateError)]
    assert len(successes) == 1
    assert len(errors) == 1
    assert len(mock_adapters[EngineName.N8N].call_log) == 1


@pytest.mark.asyncio
async def test_a07_return_to_ungated_downstream_error_never_bricks_in_transitioning(
    gate_system, mock_adapters, test_actor
):
    """A07: Downstream error during return_to_ungated restores GATED (never permanently deadlocked in TRANSITIONING)."""
    from gate.errors import DownstreamError

    dispatcher, modes, _ = gate_system
    conv_id = "conv_ungate_no_brick"
    await dispatcher.store.get_or_create_conversation(conv_id, "No Brick Subject")
    await modes.barge_in(actor=test_actor, conversation_id=conv_id)

    sub = ActionSubmission(
        conversationId=conv_id,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:nobrick:1",
        payload={"slotId": "INVALID", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    h = await dispatcher.submit_action(sub)

    mock_adapters[EngineName.CALNODE].simulate_downstream_error = True
    with pytest.raises(DownstreamError):
        await modes.return_to_ungated(
            actor=test_actor,
            conversation_id=conv_id,
            resolutions=[
                ActionResolution(actionId=h["actionId"], decision=ReleaseDecision.RELEASE)
            ],
        )

    # Conversation must be safely back in GATED (NOT stuck in TRANSITIONING!)
    conv = await dispatcher.store.get_conversation(conv_id)
    assert conv["mode"] == ConversationMode.GATED.value

    # Operator can cleanly return to UNGATED now that no held actions remain
    ungate_ok = await modes.return_to_ungated(
        actor=test_actor, conversation_id=conv_id, resolutions=[]
    )
    assert ungate_ok["mode"] == ConversationMode.UNGATED.value


@pytest.mark.asyncio
async def test_a10_restart_mid_dispatching_fails_closed_and_reconciles_cleanly(
    test_config, history_publisher, authz_provider, mock_adapters, test_actor
):
    """A10: Crash mid-DISPATCHING fails closed, blocks duplicate retry, and does not re-fail on second restart after reconcile."""
    from gate.errors import DownstreamOutcomeUnknownError

    store1 = GateStore(test_config.store_path)
    await store1.initialize()
    await store1.get_or_create_conversation("conv_crash_disp", "Crash Mid-Dispatch")
    await store1.record_action(
        action_id="act_crashed_1",
        conversation_id="conv_crash_disp",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotency_key="idem:crash:disp1",
        initial_state=ActionState.RECEIVED,
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor_guid=test_actor.actorGuid,
        actor_kind=test_actor.actorKind.value,
        context=test_actor.to_event_context(),
    )
    await store1.update_action_state("act_crashed_1", ActionState.AUTHORIZED)
    await store1.update_action_state("act_crashed_1", ActionState.DISPATCHING)
    await store1.close()

    # Restart 1: recovers to FAILED_CLOSED and transitions crashed action out of DISPATCHING
    store2 = GateStore(test_config.store_path)
    await store2.initialize()
    rec1 = await store2.recover_on_startup()
    assert rec1["failedClosedCount"] == 1
    assert "conv_crash_disp" in rec1["interruptedConversationsFailedClosed"]

    conv = await store2.get_conversation("conv_crash_disp")
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value

    # Reconcile conversation back to UNGATED
    runtimes2 = ConversationRuntimeRegistry()
    disp2 = GateDispatcher(test_config, store2, history_publisher, authz_provider, runtimes2, mock_adapters)
    modes2 = GateModeController(test_config, store2, history_publisher, authz_provider, runtimes2, disp2)
    await modes2.reconcile_failed_closed("conv_crash_disp", ConversationMode.UNGATED, actor=test_actor)

    # Retrying the crashed idempotency key does NOT blindly double-dispatch
    sub = ActionSubmission(
        conversationId="conv_crash_disp",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:crash:disp1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    with pytest.raises(DownstreamOutcomeUnknownError):
        await disp2.submit_action(sub)
    assert len(mock_adapters[EngineName.CALNODE].call_log) == 0
    await store2.close()

    # Restart 2: must NOT re-fail-closed the already-reconciled conversation!
    store3 = GateStore(test_config.store_path)
    await store3.initialize()
    rec2 = await store3.recover_on_startup()
    assert rec2["failedClosedCount"] == 0
    conv_final = await store3.get_conversation("conv_crash_disp")
    assert conv_final["mode"] == ConversationMode.UNGATED.value
    await store3.close()


@pytest.mark.asyncio
async def test_a10_restart_preserves_held_actions_and_recovers(
    test_config, history_publisher, authz_provider, mock_adapters, test_actor
):
    """A10 / R-REC-01: Restart during held action: mode and pending work survive safely."""
    # 1. Run initial session and leave an action HELD
    store1 = GateStore(test_config.store_path)
    await store1.initialize()
    runtimes1 = ConversationRuntimeRegistry()
    disp1 = GateDispatcher(test_config, store1, history_publisher, authz_provider, runtimes1, mock_adapters)
    modes1 = GateModeController(test_config, store1, history_publisher, authz_provider, runtimes1, disp1)

    await store1.get_or_create_conversation("conv_restart_01", "Customer Restart")
    await modes1.barge_in(actor=test_actor, conversation_id="conv_restart_01")

    sub = ActionSubmission(
        conversationId="conv_restart_01",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="idem:restart:held1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    held_res = await disp1.submit_action(sub)
    assert held_res["state"] == ActionState.HELD.value
    await store1.close()

    # 2. Simulate process restart with fresh store instance loading from the same DB file
    store2 = GateStore(test_config.store_path)
    await store2.initialize()
    recovery_report = await store2.recover_on_startup()

    assert recovery_report["heldActionsPreserved"] >= 1

    # Verify conversation mode survived intact
    conv = await store2.get_conversation("conv_restart_01")
    assert conv["mode"] == ConversationMode.GATED.value

    # Verify held action survived intact
    held_list = await store2.get_held_actions("conv_restart_01")
    assert len(held_list) == 1
    assert held_list[0]["action_id"] == held_res["actionId"]
    assert held_list[0]["state"] == ActionState.HELD.value

    await store2.close()
