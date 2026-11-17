"""Tests for GateDispatcher invariants (T-08: Invariants 1, 2, 4, 8; R-SEC-01/04; A04)."""

from __future__ import annotations

import pytest

from gate.dispatch import GateDispatcher
from gate.errors import (
    DownstreamError,
    DownstreamOutcomeUnknownError,
    GateFailedClosedError,
    IdempotencyConflictError,
    PermissionCheckFailedError,
    RecipientNotAllowlistedError,
    UnauthorizedError,
)
from gate.models import (
    ActionSubmission,
    ControlEventType,
    ConversationMode,
    EngineName,
    Outcome,
)
from gate.state_machine import ConversationRuntimeRegistry


@pytest.fixture
def dispatcher(test_config, gate_store, history_publisher, authz_provider, mock_adapters):
    runtimes = ConversationRuntimeRegistry()
    return GateDispatcher(
        config=test_config,
        store=gate_store,
        history=history_publisher,
        authz=authz_provider,
        runtimes=runtimes,
        adapters=mock_adapters,
    )


@pytest.mark.asyncio
async def test_invariant_1_idempotent_at_most_once_booking(
    dispatcher, mock_adapters, test_actor
):
    """A04 / R-CAL-03 / Invariant 1: Duplicate booking request does not double-book."""
    sub = ActionSubmission(
        conversationId="conv_sales_01",
        subjectLineDisplay="Customer A - Sales",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="book:conv_sales_01:slot_1",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )

    first = await dispatcher.submit_action(sub)
    second = await dispatcher.submit_action(sub)

    assert first["state"] == "DONE"
    assert first["idempotentReplay"] is False
    assert second["state"] == "DONE"
    assert second["idempotentReplay"] is True
    assert second["result"]["bookingId"] == first["result"]["bookingId"]
    # Calnode engine was invoked EXACTLY ONCE
    assert len(mock_adapters[EngineName.CALNODE].call_log) == 1


@pytest.mark.asyncio
async def test_idempotency_conflict_rejected(dispatcher, test_actor):
    sub1 = ActionSubmission(
        conversationId="conv_sales_02",
        subjectLineDisplay="Customer A",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="book:conflict_key",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )
    sub2 = ActionSubmission(
        conversationId="conv_sales_02",
        subjectLineDisplay="Customer A",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="book:conflict_key",
        payload={"slotId": "slot_2", "attendeeEmail": "customer-a@allowlist.test.local"},
        recipients=["customer-a@allowlist.test.local"],
        actor=test_actor,
    )

    await dispatcher.submit_action(sub1)
    with pytest.raises(IdempotencyConflictError):
        await dispatcher.submit_action(sub2)


@pytest.mark.asyncio
async def test_non_allowlisted_recipient_blocked_before_dispatch(
    dispatcher, mock_adapters, history_publisher, test_actor
):
    """R-SEC-01 / R-SEC-02: Non-allowlisted recipient is rejected and never dispatched."""
    sub = ActionSubmission(
        conversationId="conv_sec_01",
        subjectLineDisplay="Unverified Contact",
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="followup:blocked_01",
        payload={"messageBody": "Hello stranger"},
        recipients=["unlisted-stranger@external.example.com"],
        actor=test_actor,
    )

    with pytest.raises(RecipientNotAllowlistedError):
        await dispatcher.submit_action(sub)

    assert len(mock_adapters[EngineName.N8N].call_log) == 0
    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.DENIED
        for e in history_publisher.events
    )


@pytest.mark.asyncio
async def test_invariant_4_permission_uncertainty_fails_closed(
    dispatcher, gate_store, authz_provider, mock_adapters, history_publisher, test_actor
):
    """R-SEC-04 / Invariant 4: Uncertain permission check transitions conversation to FAILED_CLOSED."""
    authz_provider.simulate_check_failure = True

    sub = ActionSubmission(
        conversationId="conv_uncertain_01",
        subjectLineDisplay="Customer B",
        engine=EngineName.WINDMILL,
        operation="start_assignment",
        idempotencyKey="wm:uncertain_01",
        payload={"bookingId": "bkg_1", "assignee": "ops-specialist-1@allowlist.test.local"},
        recipients=["ops-specialist-1@allowlist.test.local"],
        actor=test_actor,
    )

    with pytest.raises(PermissionCheckFailedError):
        await dispatcher.submit_action(sub)

    assert len(mock_adapters[EngineName.WINDMILL].call_log) == 0
    conv = await gate_store.get_conversation("conv_uncertain_01")
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value
    assert any(
        e.type == ControlEventType.GATE_FAILED_CLOSED for e in history_publisher.events
    )

    # Subsequent actions are blocked while FAILED_CLOSED even after authz recovers
    authz_provider.simulate_check_failure = False
    sub2 = ActionSubmission(
        conversationId="conv_uncertain_01",
        subjectLineDisplay="Customer B",
        engine=EngineName.WINDMILL,
        operation="start_assignment",
        idempotencyKey="wm:uncertain_02",
        payload={"bookingId": "bkg_2", "assignee": "ops-specialist-1@allowlist.test.local"},
        recipients=["ops-specialist-1@allowlist.test.local"],
        actor=test_actor,
    )
    with pytest.raises(GateFailedClosedError):
        await dispatcher.submit_action(sub2)


@pytest.mark.asyncio
async def test_invariant_8_unknown_downstream_outcome_recorded_and_fails_closed(
    dispatcher, gate_store, mock_adapters, history_publisher, test_actor
):
    """Invariant 8: Timeout recorded as outcome='unknown', NOT guessed as success; retry blocked until reconciled."""
    mock_adapters[EngineName.CALNODE].simulate_network_failure = True

    sub = ActionSubmission(
        conversationId="conv_timeout_01",
        subjectLineDisplay="Customer C",
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey="book:timeout_key_01",
        payload={"slotId": "slot_1", "attendeeEmail": "customer-c@allowlist.test.local"},
        recipients=["customer-c@allowlist.test.local"],
        actor=test_actor,
    )

    with pytest.raises(DownstreamOutcomeUnknownError):
        await dispatcher.submit_action(sub)

    idem = await gate_store.get_idempotency_record("book:timeout_key_01")
    assert idem["outcome"] == Outcome.UNKNOWN.value

    conv = await gate_store.get_conversation("conv_timeout_01")
    assert conv["mode"] == ConversationMode.FAILED_CLOSED.value

    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.UNKNOWN
        for e in history_publisher.events
    )

    # Retrying with same idempotency key does NOT blindly re-dispatch
    mock_adapters[EngineName.CALNODE].simulate_network_failure = False
    with pytest.raises(DownstreamOutcomeUnknownError):
        await dispatcher.submit_action(sub)
    assert len(mock_adapters[EngineName.CALNODE].call_log) == 1


@pytest.mark.asyncio
async def test_confirmed_downstream_error_recorded(
    dispatcher, mock_adapters, history_publisher, test_actor
):
    mock_adapters[EngineName.WINDMILL].simulate_downstream_error = True
    sub = ActionSubmission(
        conversationId="conv_err_01",
        subjectLineDisplay="Customer Err",
        engine=EngineName.WINDMILL,
        operation="start_assignment",
        idempotencyKey="wm:err_01",
        payload={"bookingId": "bkg_1", "assignee": "ops-specialist-1@allowlist.test.local"},
        recipients=["ops-specialist-1@allowlist.test.local"],
        actor=test_actor,
    )
    with pytest.raises(DownstreamError):
        await dispatcher.submit_action(sub)

    assert any(
        e.type == ControlEventType.ACTION_FAILED and e.outcome == Outcome.ERROR
        for e in history_publisher.events
    )
