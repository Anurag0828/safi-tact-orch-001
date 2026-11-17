"""Single trusted per-conversation Gate Dispatcher (R-GATE-01, R-GATE-09, E3, E5)."""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import uuid4

from adapters.base import EngineAdapter
from gate.authz import PermissionProvider
from gate.config import GateConfig
from gate.errors import (
    DownstreamError,
    DownstreamOutcomeUnknownError,
    GateFailedClosedError,
    IdempotencyConflictError,
    IdentityUncertainError,
    PermissionCheckFailedError,
    RecipientNotAllowlistedError,
    UnauthorizedError,
)
from gate.history import HistoryPublisher
from gate.models import (
    ActionState,
    ActionSubmission,
    ActorContext,
    ControlEventType,
    ConversationMode,
    EngineName,
    Outcome,
)
from gate.state_machine import (
    ConversationRuntimeRegistry,
    assert_valid_mode_transition,
)
from gate.store import GateStore, compute_payload_hash

logger = logging.getLogger("gate.dispatch")


class GateDispatcher:
    """The ONE trusted per-conversation dispatcher across n8n, Calnode, and Windmill."""

    def __init__(
        self,
        config: GateConfig,
        store: GateStore,
        history: HistoryPublisher,
        authz: PermissionProvider,
        runtimes: ConversationRuntimeRegistry,
        adapters: dict[EngineName, EngineAdapter],
    ) -> None:
        self.config = config
        self.store = store
        self.history = history
        self.authz = authz
        self.runtimes = runtimes
        self.adapters = adapters

    async def fail_closed(
        self,
        conversation_id: str,
        actor: ActorContext,
        operation_id: str,
        idempotency_key: str,
        subject_line_display: str = "",
        correlation_id: str | None = None,
    ) -> None:
        """Transitions conversation to FAILED_CLOSED and emits GATE_FAILED_CLOSED (E4, R-SEC-04)."""
        await self.store.get_or_create_conversation(conversation_id, subject_line_display)
        await self.store.update_conversation_mode(
            conversation_id, ConversationMode.FAILED_CLOSED
        )
        await self.history.record_control_event(
            conversation_id=conversation_id,
            actor=actor,
            event_type=ControlEventType.GATE_FAILED_CLOSED,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
            outcome=Outcome.UNKNOWN,
            subject_line_display=subject_line_display,
            correlation_id=correlation_id,
        )

    async def submit_action(self, submission: ActionSubmission) -> dict[str, Any]:
        """Entry point for ALL controlled effects across n8n, Calnode, and Windmill (R-GATE-01)."""
        conv_id = submission.conversationId
        runtime = self.runtimes.get(conv_id)

        # Ensure conversation exists in operational store
        await self.store.get_or_create_conversation(
            conv_id, submission.subjectLineDisplay
        )

        # 1. Recipient allowlist validation (R-SEC-01, R-SEC-02)
        for recipient in submission.recipients:
            if not self.config.is_recipient_allowed(recipient):
                await self.history.record_control_event(
                    conversation_id=conv_id,
                    actor=submission.actor,
                    event_type=ControlEventType.ACTION_FAILED,
                    operation_id=f"{submission.engine.value}:{submission.operation}",
                    idempotency_key=submission.idempotencyKey,
                    outcome=Outcome.DENIED,
                    subject_line_display=submission.subjectLineDisplay,
                    correlation_id=submission.correlationId,
                )
                raise RecipientNotAllowlistedError(recipient)

        # 2. Idempotency pre-check (E5 Invariant 1: at most one external effect per key)
        existing = await self.store.get_idempotency_record(submission.idempotencyKey)
        if existing is not None:
            payload_hash = compute_payload_hash(submission.payload)
            if existing["payload_hash"] != payload_hash:
                raise IdempotencyConflictError(submission.idempotencyKey)

            existing_action = await self.store.get_action(existing["action_id"])
            if existing_action is not None:
                if existing_action["state"] == ActionState.DONE.value:
                    return {
                        "actionId": existing_action["action_id"],
                        "conversationId": conv_id,
                        "state": ActionState.DONE.value,
                        "idempotencyKey": submission.idempotencyKey,
                        "idempotentReplay": True,
                        "result": existing_action.get("result", {}),
                    }
                if existing_action["state"] == ActionState.HELD.value:
                    return {
                        "actionId": existing_action["action_id"],
                        "conversationId": conv_id,
                        "state": ActionState.HELD.value,
                        "idempotencyKey": submission.idempotencyKey,
                        "idempotentReplay": True,
                    }
                if existing["outcome"] == Outcome.UNKNOWN.value:
                    # Unknown prior outcome: do not blindly double-dispatch
                    raise DownstreamOutcomeUnknownError(
                        existing_action["action_id"],
                        submission.idempotencyKey,
                        "Previous dispatch had unknown outcome; reconcile before retry",
                    )

        # 3. Fast admission check under state_lock before waiting on dispatch_lock
        #    If GATED or TRANSITIONING, hold immediately (R-GATE-05, Invariant 3)
        action_id = f"act_{uuid4().hex[:12]}"
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            mode = ConversationMode(conv["mode"])

            if mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            if mode in (ConversationMode.GATED, ConversationMode.TRANSITIONING):
                await self.store.record_action(
                    action_id=action_id,
                    conversation_id=conv_id,
                    engine=submission.engine,
                    operation=submission.operation,
                    idempotency_key=submission.idempotencyKey,
                    initial_state=ActionState.HELD,
                    payload=submission.payload,
                    recipients=submission.recipients,
                    actor_guid=submission.actor.actorGuid,
                    actor_kind=submission.actor.actorKind.value,
                    context=submission.actor.to_event_context(),
                    payload_ref=submission.payloadRef,
                )
                await self.history.record_control_event(
                    conversation_id=conv_id,
                    actor=submission.actor,
                    event_type=ControlEventType.ACTION_HELD,
                    operation_id=f"{submission.engine.value}:{submission.operation}",
                    idempotency_key=submission.idempotencyKey,
                    outcome=Outcome.OK,
                    subject_line_display=submission.subjectLineDisplay,
                    correlation_id=submission.correlationId,
                )
                return {
                    "actionId": action_id,
                    "conversationId": conv_id,
                    "state": ActionState.HELD.value,
                    "mode": mode.value,
                    "idempotencyKey": submission.idempotencyKey,
                }

        # 4. UNGATED path: serialize execution per conversation (R-GATE-07)
        async with runtime.dispatch_lock:
            # Re-check mode & idempotency inside dispatch_lock (prevents TOCTOU race with barge_in)
            async with runtime.state_lock:
                conv = await self.store.get_conversation(conv_id)
                mode = ConversationMode(conv["mode"])

                if mode == ConversationMode.FAILED_CLOSED:
                    raise GateFailedClosedError(conv_id)

                if mode in (ConversationMode.GATED, ConversationMode.TRANSITIONING):
                    await self.store.record_action(
                        action_id=action_id,
                        conversation_id=conv_id,
                        engine=submission.engine,
                        operation=submission.operation,
                        idempotency_key=submission.idempotencyKey,
                        initial_state=ActionState.HELD,
                        payload=submission.payload,
                        recipients=submission.recipients,
                        actor_guid=submission.actor.actorGuid,
                        actor_kind=submission.actor.actorKind.value,
                        context=submission.actor.to_event_context(),
                        payload_ref=submission.payloadRef,
                    )
                    await self.history.record_control_event(
                        conversation_id=conv_id,
                        actor=submission.actor,
                        event_type=ControlEventType.ACTION_HELD,
                        operation_id=f"{submission.engine.value}:{submission.operation}",
                        idempotency_key=submission.idempotencyKey,
                        outcome=Outcome.OK,
                        subject_line_display=submission.subjectLineDisplay,
                        correlation_id=submission.correlationId,
                    )
                    return {
                        "actionId": action_id,
                        "conversationId": conv_id,
                        "state": ActionState.HELD.value,
                        "mode": mode.value,
                        "idempotencyKey": submission.idempotencyKey,
                    }

                # Record action RECEIVED
                await self.store.record_action(
                    action_id=action_id,
                    conversation_id=conv_id,
                    engine=submission.engine,
                    operation=submission.operation,
                    idempotency_key=submission.idempotencyKey,
                    initial_state=ActionState.RECEIVED,
                    payload=submission.payload,
                    recipients=submission.recipients,
                    actor_guid=submission.actor.actorGuid,
                    actor_kind=submission.actor.actorKind.value,
                    context=submission.actor.to_event_context(),
                    payload_ref=submission.payloadRef,
                )

                # Immediately recheck SAFI permission before dispatch (R-GATE-09)
                await self._recheck_permission_or_fail(
                    action_id=action_id,
                    conversation_id=conv_id,
                    actor=submission.actor,
                    operation_id=f"{submission.engine.value}:{submission.operation}",
                    idempotency_key=submission.idempotencyKey,
                    subject_line_display=submission.subjectLineDisplay,
                    correlation_id=submission.correlationId,
                )

                # Mark AUTHORIZED -> DISPATCHING & register in-flight
                await self.store.update_action_state(action_id, ActionState.AUTHORIZED)
                await self.store.update_action_state(action_id, ActionState.DISPATCHING)
                runtime.mark_inflight(action_id)

            # Execute downstream outside state_lock (so barge_in can enter TRANSITIONING)
            return await self._execute_downstream(
                action_id=action_id,
                conversation_id=conv_id,
                engine=submission.engine,
                operation=submission.operation,
                payload=submission.payload,
                idempotency_key=submission.idempotencyKey,
                actor=submission.actor,
                subject_line_display=submission.subjectLineDisplay,
                correlation_id=submission.correlationId,
            )

    async def dispatch_approved_action(
        self,
        action_id: str,
        actor: ActorContext,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Dispatches an already APPROVED/RELEASED action with mandatory fresh permission recheck (R-GATE-09)."""
        action = await self.store.get_action(action_id)
        if action is None:
            raise ValueError(f"Action '{action_id}' not found")

        conv_id = action["conversation_id"]
        engine = EngineName(action["engine"])
        operation = action["operation"]
        idempotency_key = action["idempotency_key"]
        payload = action["payload"]
        recipients = action["recipients"]

        conv = await self.store.get_conversation(conv_id)
        subject_line_display = conv["subject_line"] if conv else ""

        # Idempotency guard (at-most-once even if approve/release invoked twice)
        if action["state"] == ActionState.DONE.value:
            return {
                "actionId": action_id,
                "conversationId": conv_id,
                "state": ActionState.DONE.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": True,
                "result": action.get("result", {}),
            }

        # Re-verify recipient allowlist
        for recipient in recipients:
            if not self.config.is_recipient_allowed(recipient):
                await self.store.update_action_state(
                    action_id,
                    ActionState.FAILED,
                    error={"code": "RECIPIENT_NOT_ALLOWLISTED", "recipient": recipient},
                )
                raise RecipientNotAllowlistedError(recipient)

        runtime = self.runtimes.get(conv_id)
        async with runtime.dispatch_lock:
            async with runtime.state_lock:
                conv = await self.store.get_conversation(conv_id)
                if ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
                    raise GateFailedClosedError(conv_id)

                # Mandatory fresh permission recheck right before dispatch (R-GATE-09)
                await self._recheck_permission_or_fail(
                    action_id=action_id,
                    conversation_id=conv_id,
                    actor=actor,
                    operation_id=f"{engine.value}:{operation}",
                    idempotency_key=idempotency_key,
                    subject_line_display=subject_line_display,
                    correlation_id=correlation_id,
                )

                await self.store.update_action_state(action_id, ActionState.AUTHORIZED)
                await self.store.update_action_state(action_id, ActionState.DISPATCHING)
                runtime.mark_inflight(action_id)

            return await self._execute_downstream(
                action_id=action_id,
                conversation_id=conv_id,
                engine=engine,
                operation=operation,
                payload=payload,
                idempotency_key=idempotency_key,
                actor=actor,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )

    async def _recheck_permission_or_fail(
        self,
        action_id: str,
        conversation_id: str,
        actor: ActorContext,
        operation_id: str,
        idempotency_key: str,
        subject_line_display: str,
        correlation_id: str | None,
    ) -> None:
        try:
            await self.authz.check_permission(actor, conversation_id, operation_id)
        except UnauthorizedError:
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error={"code": "UNAUTHORIZED"},
            )
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.ACTION_FAILED,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                outcome=Outcome.DENIED,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise
        except (PermissionCheckFailedError, IdentityUncertainError):
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error={"code": "PERMISSION_CHECK_FAILED"},
            )
            await self.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise

    async def _execute_downstream(
        self,
        action_id: str,
        conversation_id: str,
        engine: EngineName,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
        actor: ActorContext,
        subject_line_display: str,
        correlation_id: str | None,
    ) -> dict[str, Any]:
        runtime = self.runtimes.get(conversation_id)
        adapter = self.adapters.get(engine)
        operation_id = f"{engine.value}:{operation}"

        try:
            if adapter is None:
                raise DownstreamError(engine.value, "No adapter registered for engine")

            result = await adapter.execute(operation, payload, idempotency_key)

            await self.store.update_action_state(
                action_id, ActionState.DONE, result=result
            )
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.ACTION_DISPATCHED,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                outcome=Outcome.OK,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            return {
                "actionId": action_id,
                "conversationId": conversation_id,
                "state": ActionState.DONE.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": False,
                "result": result,
            }

        except DownstreamOutcomeUnknownError as exc:
            # Invariant 8: record as 'unknown', NOT guessed as success; fail closed (R-SEC-04)
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error={"code": exc.code, "message": exc.message, "outcome": "unknown"},
            )
            await self.store.update_idempotency_outcome(
                idempotency_key,
                Outcome.UNKNOWN,
                {"code": exc.code, "message": exc.message},
            )
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.ACTION_FAILED,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                outcome=Outcome.UNKNOWN,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            await self.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise

        except DownstreamError as exc:
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error=exc.to_dict(),
            )
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.ACTION_FAILED,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                outcome=Outcome.ERROR,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise

        finally:
            async with runtime.state_lock:
                runtime.mark_settled(action_id)
