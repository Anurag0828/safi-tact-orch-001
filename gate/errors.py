"""Typed error hierarchy matching contracts/errors.md (Part D3)."""

from __future__ import annotations

from typing import Any


class GateError(Exception):
    """Base error for all Gate Adapter failures."""

    def __init__(
        self,
        code: str,
        message: str,
        http_status: int = 500,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "errorCode": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


class UnauthorizedError(GateError):
    def __init__(self, message: str = "Actor is not authorized for this operation", details: dict[str, Any] | None = None) -> None:
        super().__init__("UNAUTHORIZED", message, 403, details)


class IdentityUncertainError(GateError):
    def __init__(self, message: str = "Actor identity or context is missing or uncertain", details: dict[str, Any] | None = None) -> None:
        super().__init__("IDENTITY_UNCERTAIN", message, 401, details)


class PermissionCheckFailedError(GateError):
    def __init__(self, message: str = "Permission recheck failed or returned uncertain result", details: dict[str, Any] | None = None) -> None:
        super().__init__("PERMISSION_CHECK_FAILED", message, 503, details)


class GateFailedClosedError(GateError):
    def __init__(self, conversation_id: str, reason: str = "Conversation gate is FAILED_CLOSED until reconciled") -> None:
        super().__init__(
            "GATE_FAILED_CLOSED",
            reason,
            503,
            {"conversationId": conversation_id},
        )


class ConversationNotFoundError(GateError):
    def __init__(self, identifier: str) -> None:
        super().__init__(
            "CONVERSATION_NOT_FOUND",
            f"No conversation found matching '{identifier}'",
            404,
            {"identifier": identifier},
        )


class AmbiguousSubjectLineError(GateError):
    def __init__(self, subject_line: str, candidates: list[dict[str, Any]]) -> None:
        super().__init__(
            "AMBIGUOUS_SUBJECT_LINE",
            f"Subject Line '{subject_line}' matches multiple conversations; select a specific ConversationID",
            409,
            {"subjectLine": subject_line, "candidates": candidates},
        )


class InvalidModeTransitionError(GateError):
    def __init__(self, current_mode: str, requested: str) -> None:
        super().__init__(
            "INVALID_MODE_TRANSITION",
            f"Cannot execute '{requested}' while conversation is in mode '{current_mode}'",
            409,
            {"currentMode": current_mode, "requested": requested},
        )


class HeldActionsUnresolvedError(GateError):
    def __init__(self, conversation_id: str, held_actions: list[dict[str, Any]]) -> None:
        super().__init__(
            "HELD_ACTIONS_UNRESOLVED",
            "All held actions must be explicitly marked RELEASE or CANCEL before returning to UNGATED",
            409,
            {"conversationId": conversation_id, "heldActions": held_actions},
        )


class ActionNotFoundError(GateError):
    def __init__(self, action_id: str) -> None:
        super().__init__(
            "ACTION_NOT_FOUND",
            f"Action '{action_id}' not found",
            404,
            {"actionId": action_id},
        )


class InvalidActionStateError(GateError):
    def __init__(self, action_id: str, current_state: str, expected: str) -> None:
        super().__init__(
            "INVALID_ACTION_STATE",
            f"Action '{action_id}' is in state '{current_state}', expected '{expected}'",
            409,
            {"actionId": action_id, "currentState": current_state, "expected": expected},
        )


class RecipientNotAllowlistedError(GateError):
    def __init__(self, recipient: str) -> None:
        super().__init__(
            "RECIPIENT_NOT_ALLOWLISTED",
            f"Recipient '{recipient}' is not in the consenting allowlist (R-SEC-01)",
            403,
            {"recipient": recipient},
        )


class IdempotencyConflictError(GateError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            "IDEMPOTENCY_CONFLICT",
            f"Idempotency key '{idempotency_key}' was already used with a different payload",
            409,
            {"idempotencyKey": idempotency_key},
        )


class DownstreamOutcomeUnknownError(GateError):
    def __init__(self, action_id: str, idempotency_key: str, reason: str) -> None:
        super().__init__(
            "DOWNSTREAM_OUTCOME_UNKNOWN",
            f"Downstream outcome for action '{action_id}' is unknown: {reason}",
            504,
            {"actionId": action_id, "idempotencyKey": idempotency_key},
        )


class DownstreamError(GateError):
    def __init__(self, engine: str, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(
            "DOWNSTREAM_ERROR",
            f"Downstream engine '{engine}' failed: {message}",
            502,
            {"engine": engine, **(details or {})},
        )


class StoreError(GateError):
    def __init__(self, message: str) -> None:
        super().__init__("STORE_ERROR", message, 500)


class ContractValidationError(GateError):
    def __init__(self, schema_name: str, message: str) -> None:
        super().__init__(
            "CONTRACT_VALIDATION_ERROR",
            f"Schema validation failed for '{schema_name}': {message}",
            422,
            {"schema": schema_name},
        )
