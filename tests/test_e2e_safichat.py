"""End-to-End tests through native SafiChat REST endpoints (A02, A03, A04, A05, A11)."""

from __future__ import annotations

import pytest

from gate.models import ActionState, ControlEventType, ConversationMode


@pytest.mark.asyncio
async def test_a02_safichat_only_control_full_connected_flow(app_client, test_actor):
    """A02, A03, A04, A05: Authorized user manages full marketing -> sales -> operations flow via SafiChat native endpoints."""
    client, app = app_client
    conv_id = "conv_e2e_customer_a"
    actor_dict = test_actor.model_dump(mode="json")

    # Step 1 (Marketing / n8n - R-MKT-01, A03): Ingest new lead
    intake_resp = await client.post(
        "/safichat/ops/marketing/ingest-lead",
        json={
            "conversationId": conv_id,
            "subjectLineDisplay": "Customer A - High Value Inquiry",
            "leadEmail": "customer-a@allowlist.test.local",
            "leadName": "Alice Customer",
            "inquiryText": "Need business consulting services",
            "idempotencyKey": "intake:conv_e2e_customer_a:01",
            "actor": actor_dict,
        },
    )
    assert intake_resp.status_code == 200
    intake_data = intake_resp.json()
    assert intake_data["state"] == "DONE"

    # Step 2 (Sales / Calnode - R-CAL-01, A04): Retrieve actual available slots
    slots_resp = await client.post(
        "/safichat/ops/sales/slots",
        json={
            "conversationId": conv_id,
            "subjectLineDisplay": "Customer A - High Value Inquiry",
            "actor": actor_dict,
        },
    )
    assert slots_resp.status_code == 200
    slots_data = slots_resp.json()
    assert len(slots_data["slots"]) >= 1
    selected_slot = slots_data["slots"][0]["slotId"]

    # Step 3 (Sales / Calnode - R-CAL-02, A04): Book appointment via Gate
    book_resp = await client.post(
        "/safichat/ops/sales/book",
        json={
            "conversationId": conv_id,
            "subjectLineDisplay": "Customer A - High Value Inquiry",
            "slotId": selected_slot,
            "attendeeEmail": "customer-a@allowlist.test.local",
            "attendeeName": "Alice Customer",
            "notes": "Discuss business automation",
            "idempotencyKey": f"book:{conv_id}:{selected_slot}",
            "actor": actor_dict,
        },
    )
    assert book_resp.status_code == 200
    book_data = book_resp.json()
    assert book_data["state"] == "DONE"
    booking_id = book_data["result"]["bookingId"]

    # Step 4 (Operations / Windmill - R-OPS-01, A05): Start operational assignment
    assign_resp = await client.post(
        "/safichat/ops/operations/assign",
        json={
            "conversationId": conv_id,
            "subjectLineDisplay": "Customer A - High Value Inquiry",
            "bookingId": booking_id,
            "assignee": "ops-specialist-1@allowlist.test.local",
            "taskSummary": f"Onboard Alice for booking {booking_id}",
            "idempotencyKey": f"assign:{conv_id}:{booking_id}",
            "actor": actor_dict,
        },
    )
    assert assign_resp.status_code == 200
    assign_data = assign_resp.json()
    assert assign_data["state"] == "DONE"
    job_id = assign_data["result"]["jobId"]
    assert job_id is not None

    # Step 5 (Operations / Windmill - R-OPS-02, A05): Check status via SafiChat
    status_resp = await client.post(
        f"/safichat/ops/operations/job-status?conversation_id={conv_id}&job_id={job_id}",
        json=actor_dict,
    )
    assert status_resp.status_code == 200
    status_data = status_resp.json()
    assert status_data["job"]["state"] == "COMPLETED"


@pytest.mark.asyncio
async def test_a11_history_attribution_written_to_jetstream(app_client, test_actor):
    """A11 / R-CTX-01, R-CTX-02: Every control event records actor GUID, Meeting/Port context, outcome, and subjectLine."""
    client, app = app_client
    conv_id = "conv_history_audit_01"
    actor_dict = test_actor.model_dump(mode="json")

    await app.state.store.get_or_create_conversation(conv_id, "Audit Subject")

    # Barge in to trigger MODE_GATED event
    resp = await client.post(
        "/safichat/ops/gate/barge-in",
        json={
            "conversationId": conv_id,
            "subjectLine": "Audit Subject",
            "actor": actor_dict,
            "correlationId": "audit_corr_100",
        },
    )
    assert resp.status_code == 200

    history = app.state.history
    events = [e for e in history.events if e.conversationId == conv_id]
    assert len(events) >= 1
    event = events[0]

    assert event.type == ControlEventType.MODE_GATED
    assert event.actorGuid == test_actor.actorGuid
    assert event.context["meetingContext"] == test_actor.meetingContext
    assert event.context["portContext"] == test_actor.portContext
    assert event.correlationId == "audit_corr_100"
    assert event.outcome.value == "ok"
    assert event.ts.endswith("Z")


@pytest.mark.asyncio
async def test_e2e_http_ambiguous_subject_line_and_reconcile(app_client, test_actor):
    """HTTP contract test: Subject Line ambiguity returns 409 candidates; FAILED_CLOSED reconciles cleanly."""
    client, app = app_client
    actor_dict = test_actor.model_dump(mode="json")

    await app.state.store.get_or_create_conversation("conv_dup_1", "Shared Subject")
    await app.state.store.get_or_create_conversation("conv_dup_2", "Shared Subject")

    # Ambiguous subject line returns 409 with both candidates (R-GATE-04)
    ambig_resp = await client.post(
        "/safichat/ops/gate/barge-in",
        json={"subjectLine": "Shared Subject", "actor": actor_dict},
    )
    assert ambig_resp.status_code == 409
    body = ambig_resp.json()
    assert body["errorCode"] == "AMBIGUOUS_SUBJECT_LINE"
    assert len(body["details"]["candidates"]) == 2

    # Force FAILED_CLOSED and verify reconcile endpoint restores target mode
    await app.state.store.update_conversation_mode("conv_dup_1", ConversationMode.FAILED_CLOSED)
    rec_resp = await client.post(
        "/safichat/ops/gate/reconcile",
        json={
            "conversationId": "conv_dup_1",
            "targetMode": "GATED",
            "actor": actor_dict,
        },
    )
    assert rec_resp.status_code == 200
    assert rec_resp.json()["mode"] == "GATED"
    assert rec_resp.json()["reconciled"] is True


@pytest.mark.asyncio
async def test_e2e_internal_engine_callback_never_bypasses_gate(app_client, proxy_actor):
    """R-GATE-01: n8n/Windmill callback to /gate/dispatch obeys conversation GATED state."""
    client, app = app_client
    conv_id = "conv_callback_gated"
    await app.state.store.get_or_create_conversation(conv_id, "Callback Gated Subject")
    await app.state.store.update_conversation_mode(conv_id, ConversationMode.GATED)

    resp = await client.post(
        "/gate/dispatch",
        json={
            "conversationId": conv_id,
            "subjectLineDisplay": "Callback Gated Subject",
            "engine": "n8n",
            "operation": "dispatch_followup",
            "idempotencyKey": "cb:n8n:01",
            "payload": {"messageBody": "Prepared by n8n"},
            "recipients": ["customer-a@allowlist.test.local"],
            "actor": proxy_actor.model_dump(mode="json"),
        },
    )
    assert resp.status_code == 200
    assert resp.json()["state"] == ActionState.HELD.value
    # Downstream n8n adapter was NOT executed while GATED
    assert len(app.state.adapters[app.state.adapters.__iter__().__next__()].call_log) == 0
