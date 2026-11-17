"""Strict domain models & JSON Schema validators matching Part D contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, ConfigDict, Field

from gate.errors import ContractValidationError


CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


class ConversationMode(str, Enum):
    UNGATED = "UNGATED"
    TRANSITIONING = "TRANSITIONING"
    GATED = "GATED"
    FAILED_CLOSED = "FAILED_CLOSED"


class EngineName(str, Enum):
    N8N = "n8n"
    CALNODE = "calnode"
    WINDMILL = "windmill"


class ActionState(str, Enum):
    RECEIVED = "RECEIVED"
    HELD = "HELD"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    AUTHORIZED = "AUTHORIZED"
    DISPATCHING = "DISPATCHING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ActorKind(str, Enum):
    USER = "USER"
    PROXY = "PROXY"


class ControlEventType(str, Enum):
    MODE_GATED = "MODE_GATED"
    MODE_UNGATED = "MODE_UNGATED"
    ACTION_HELD = "ACTION_HELD"
    ACTION_APPROVED = "ACTION_APPROVED"
    ACTION_REJECTED = "ACTION_REJECTED"
    ACTION_RELEASED = "ACTION_RELEASED"
    ACTION_CANCELLED = "ACTION_CANCELLED"
    ACTION_DISPATCHED = "ACTION_DISPATCHED"
    ACTION_FAILED = "ACTION_FAILED"
    GATE_FAILED_CLOSED = "GATE_FAILED_CLOSED"


class Outcome(str, Enum):
    OK = "ok"
    DENIED = "denied"
    ERROR = "error"
    UNKNOWN = "unknown"


class ReleaseDecision(str, Enum):
    RELEASE = "RELEASE"
    CANCEL = "CANCEL"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class ActorContext(BaseModel):
    """Identity and full historical Meeting/Port Context preserved on every event (R-CTX-01)."""

    model_config = ConfigDict(extra="forbid")

    actorGuid: str = Field(..., min_length=1)
    actorKind: ActorKind = ActorKind.USER
    meetingContext: dict[str, Any] = Field(default_factory=dict)
    portContext: dict[str, Any] = Field(default_factory=dict)
    permissionProvenance: dict[str, Any] = Field(default_factory=dict)

    def to_event_context(self) -> dict[str, Any]:
        return {
            "meetingContext": self.meetingContext,
            "portContext": self.portContext,
            "permissionProvenance": self.permissionProvenance,
        }


class GateActionRecord(BaseModel):
    """Strict representation matching contracts/gate-action.schema.json (Part D1)."""

    model_config = ConfigDict(extra="forbid")

    actionId: str
    conversationId: str
    engine: EngineName
    operation: str
    idempotencyKey: str
    state: ActionState
    payloadRef: str | None = None
    createdAt: str

    def to_schema_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json", exclude_none=True)
        validate_against_schema("gate-action.schema.json", data)
        return data


class ControlEvent(BaseModel):
    """Strict representation matching contracts/control-event.schema.json (Part D2)."""

    model_config = ConfigDict(extra="forbid")

    eventId: str
    conversationId: str
    subjectLineDisplay: str | None = None
    actorGuid: str
    actorKind: ActorKind = ActorKind.USER
    type: ControlEventType
    operationId: str
    idempotencyKey: str
    outcome: Outcome
    context: dict[str, Any]
    correlationId: str | None = None
    ts: str

    def to_schema_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json", exclude_none=True)
        validate_against_schema("control-event.schema.json", data)
        return data


class ActionResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    actionId: str
    decision: ReleaseDecision


class GateOperationRequest(BaseModel):
    """Strict representation matching contracts/gate-operation.schema.json."""

    model_config = ConfigDict(extra="forbid")

    operationType: str
    conversationId: str
    subjectLine: str | None = None
    actorGuid: str
    actorKind: ActorKind = ActorKind.USER
    actionId: str | None = None
    resolutions: list[ActionResolution] | None = None
    idempotencyKey: str | None = None
    correlationId: str | None = None

    def to_schema_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json", exclude_none=True)
        validate_against_schema("gate-operation.schema.json", data)
        return data


class ActionSubmission(BaseModel):
    """Incoming controlled-effect request submitted to the Gate Adapter."""

    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    engine: EngineName
    operation: str = Field(..., min_length=1)
    idempotencyKey: str = Field(..., min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    payloadRef: str | None = None
    recipients: list[str] = Field(default_factory=list)
    actor: ActorContext
    correlationId: str | None = None


_VALIDATOR_CACHE: dict[str, Draft202012Validator] = {}


def load_schema_validator(schema_filename: str) -> Draft202012Validator:
    if schema_filename not in _VALIDATOR_CACHE:
        schema_path = CONTRACTS_DIR / schema_filename
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        _VALIDATOR_CACHE[schema_filename] = Draft202012Validator(
            schema, format_checker=FormatChecker()
        )
    return _VALIDATOR_CACHE[schema_filename]


def validate_against_schema(schema_filename: str, payload: dict[str, Any]) -> None:
    validator = load_schema_validator(schema_filename)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        first = errors[0]
        path = ".".join(str(p) for p in first.path) or "<root>"
        raise ContractValidationError(schema_filename, f"{path}: {first.message}")
