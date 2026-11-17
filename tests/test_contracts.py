"""Contract validation tests ensuring strict compliance with Part D schemas."""

from __future__ import annotations

import pytest

from gate.errors import ContractValidationError
from gate.models import (
    ActionResolution,
    ActionState,
    ActorKind,
    ControlEventType,
    EngineName,
    GateActionRecord,
    GateOperationRequest,
    Outcome,
    ReleaseDecision,
    utc_now_iso,
    validate_against_schema,
)


def test_gate_action_schema_valid():
    record = GateActionRecord(
        actionId="act_0123456789ab",
        conversationId="conv_alpha_01",
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey="idem_key_001",
        state=ActionState.HELD,
        createdAt=utc_now_iso(),
    )
    data = record.to_schema_dict()
    assert data["actionId"] == "act_0123456789ab"
    assert data["state"] == "HELD"


def test_gate_action_schema_rejects_extra_fields():
    data = {
        "actionId": "act_01",
        "conversationId": "conv_01",
        "engine": "n8n",
        "operation": "test",
        "idempotencyKey": "k1",
        "state": "HELD",
        "createdAt": "2026-11-17T18:00:00Z",
        "forbiddenExtraField": "illegal",
    }
    with pytest.raises(ContractValidationError):
        validate_against_schema("gate-action.schema.json", data)


def test_control_event_schema_valid(test_actor):
    from gate.models import ControlEvent

    event = ControlEvent(
        eventId="evt_0123456789ab",
        conversationId="conv_alpha_01",
        subjectLineDisplay="Customer A - Inquiry",
        actorGuid=test_actor.actorGuid,
        actorKind=test_actor.actorKind,
        type=ControlEventType.MODE_GATED,
        operationId="barge_in:01",
        idempotencyKey="idem_barge_01",
        outcome=Outcome.OK,
        context=test_actor.to_event_context(),
        correlationId="corr_999",
        ts=utc_now_iso(),
    )
    data = event.to_schema_dict()
    assert data["type"] == "MODE_GATED"
    assert data["outcome"] == "ok"


def test_control_event_schema_rejects_invalid_outcome():
    data = {
        "eventId": "evt_01",
        "conversationId": "conv_01",
        "actorGuid": "user_01",
        "actorKind": "USER",
        "type": "MODE_GATED",
        "operationId": "op_01",
        "idempotencyKey": "k1",
        "outcome": "partially_successful",  # ILLEGAL: must be ok, denied, error, unknown
        "context": {},
        "ts": "2026-11-17T18:00:00Z",
    }
    with pytest.raises(ContractValidationError):
        validate_against_schema("control-event.schema.json", data)


def test_gate_operation_schema_valid():
    op = GateOperationRequest(
        operationType="RETURN_TO_UNGATED",
        conversationId="conv_01",
        actorGuid="user_01",
        resolutions=[
            ActionResolution(actionId="act_1", decision=ReleaseDecision.RELEASE),
            ActionResolution(actionId="act_2", decision=ReleaseDecision.CANCEL),
        ],
    )
    data = op.to_schema_dict()
    assert data["operationType"] == "RETURN_TO_UNGATED"
    assert len(data["resolutions"]) == 2

    # BARGE_IN by subjectLine alone (without conversationId) is valid per R-GATE-04
    op_subject = GateOperationRequest(
        operationType="BARGE_IN",
        subjectLine="Customer B - Selective Gate",
        actorGuid="user_01",
    )
    data_sub = op_subject.to_schema_dict()
    assert data_sub["subjectLine"] == "Customer B - Selective Gate"


def test_captured_contract_example_files_validate_against_schemas():
    import json
    from gate.models import CONTRACTS_DIR

    pairs = [
        ("control-event.schema.json", "control-event.example.json"),
        ("gate-action.schema.json", "gate-action.example.json"),
        ("gate-operation.schema.json", "gate-operation.example.json"),
    ]
    for schema_name, example_name in pairs:
        payload = json.loads((CONTRACTS_DIR / example_name).read_text(encoding="utf-8"))
        validate_against_schema(schema_name, payload)


def test_gate_operation_schema_requires_action_id_and_target_mode():
    # APPROVE_ACTION without actionId must fail schema validation
    with pytest.raises(ContractValidationError):
        validate_against_schema(
            "gate-operation.schema.json",
            {
                "operationType": "APPROVE_ACTION",
                "conversationId": "conv_01",
                "actorGuid": "user_01",
            },
        )

    # REJECT_ACTION without actionId must fail schema validation
    with pytest.raises(ContractValidationError):
        validate_against_schema(
            "gate-operation.schema.json",
            {
                "operationType": "REJECT_ACTION",
                "conversationId": "conv_01",
                "actorGuid": "user_01",
            },
        )

    # RECONCILE without targetMode must fail schema validation
    with pytest.raises(ContractValidationError):
        validate_against_schema(
            "gate-operation.schema.json",
            {
                "operationType": "RECONCILE",
                "conversationId": "conv_01",
                "actorGuid": "user_01",
            },
        )

    # RECONCILE with valid targetMode passes
    validate_against_schema(
        "gate-operation.schema.json",
        {
            "operationType": "RECONCILE",
            "conversationId": "conv_01",
            "actorGuid": "user_01",
            "targetMode": "GATED",
        },
    )

    # LIST_HELD by conversationId or subjectLine passes
    validate_against_schema(
        "gate-operation.schema.json",
        {
            "operationType": "LIST_HELD",
            "conversationId": "conv_01",
            "actorGuid": "user_01",
        },
    )
