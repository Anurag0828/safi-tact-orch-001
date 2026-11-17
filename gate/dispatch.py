"""Single trusted per-conversation Gate Dispatcher (R-GATE-01, R-GATE-09, E3, E5)."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from adapters.base import EngineAdapter
from gate.authz import PermissionProvider
from gate.config import GateConfig
from gate.errors import (
    ActionNotFoundError,
    DownstreamError,
    DownstreamOutcomeUnknownError,
    GateFailedClosedError,
    IdempotencyConflictError,
    IdentityUncertainError,
    InvalidActionStateError,
    PermissionCheckFailedError,
    RecipientNotAllowlistedError,
    StoreError,
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
from gate.state_machine import ConversationRuntimeRegistry
from gate.store import GateStore, compute_payload_hash

logger = logging.getLogger("gate.dispatch")

_RECIPIENT_PAYLOAD_KEYS = frozenset(
    {
        "recipientemail",
        "attendeeemail",
        "leademail",
        "assignee",
        "email",
        "to",
        "recipients",
        "recipient",
    }
)

_OPERATIONS_REQUIRING_RECIPIENT = frozenset(
    {
        "dispatch_followup",
        "ingest_lead",
        "create_booking",
        "start_assignment",
    }
)


def extract_all_recipients(
    explicit_recipients: list[str], payload: dict[str, Any]
) -> list[str]:
    """Extracts ALL recipients from both submission.recipients AND payload fields (prevents /gate/dispatch allowlist bypass)."""
    collected: list[str] = []
    seen: set[str] = set()

    def _add(val: Any) -> None:
        if isinstance(val, str):
            cleaned = val.strip()
            if cleaned and cleaned.lower() not in seen:
                seen.add(cleaned.lower())
                collected.append(cleaned)
        elif isinstance(val, list):
            for item in val:
                _add(item)

    for r in explicit_recipients:
        _add(r)

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                if str(k).lower() in _RECIPIENT_PAYLOAD_KEYS:
                    _add(v)
                elif isinstance(v, (dict, list)):
                    _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    _walk(item)

    _walk(payload)
    return collected


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
        allow_create: bool = False,
    ) -> None:
        """Transitions conversation to FAILED_CLOSED and emits GATE_FAILED_CLOSED (E4, R-SEC-04)."""
        conv = await self.store.get_conversation(conversation_id)
        if conv is None:
            if not allow_create:
                return
            conv = await self.store.get_or_create_conversation(
                conversation_id, subject_line_display
            )
        effective_subject = subject_line_display or conv["subject_line"] or ""
        await self.store.update_conversation_mode(
            conversation_id, ConversationMode.FAILED_CLOSED
        )
        try:
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.GATE_FAILED_CLOSED,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                outcome=Outcome.UNKNOWN,
                subject_line_display=effective_subject,
                correlation_id=correlation_id,
            )
        except StoreError as exc:
            logger.error(
                "Failed to emit GATE_FAILED_CLOSED to JetStream for '%s' (mode persisted FAILED_CLOSED): %s",
                conversation_id,
                exc,
            )

    async def _check_actor_authz(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation_id: str,
        idempotency_key: str,
        subject_line_display: str,
        correlation_id: str | None,
    ) -> None:
        """Checks permission before admission/replay/hold; fails closed on uncertainty (A09, R-SEC-04)."""
        try:
            await self.authz.check_permission(actor, conversation_id, operation_id)
        except UnauthorizedError:
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
            await self.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
                allow_create=False,
            )
            raise

    async def _validate_recipients_or_deny(
        self,
        conversation_id: str,
        operation: str,
        operation_id: str,
        idempotency_key: str,
        explicit_recipients: list[str],
        payload: dict[str, Any],
        actor: ActorContext,
        subject_line_display: str,
        correlation_id: str | None,
    ) -> list[str]:
        """Validates ALL explicit and payload-embedded recipients against the allowlist (R-SEC-01, R-SEC-02)."""
        all_recipients = extract_all_recipients(explicit_recipients, payload)
        if operation in _OPERATIONS_REQUIRING_RECIPIENT and not all_recipients:
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
            raise RecipientNotAllowlistedError("<missing_recipient>")

        for recipient in all_recipients:
            if not self.config.is_recipient_allowed(recipient):
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
                raise RecipientNotAllowlistedError(recipient)
        return all_recipients

    async def _check_existing_idempotency(
        self,
        conversation_id: str,
        idempotency_key: str,
        engine: EngineName,
        operation: str,
        payload: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Evaluates per-conversation idempotency records safely under lock (E5 Invariant 1 & 8, A06/A07)."""
        existing = await self.store.get_idempotency_record(conversation_id, idempotency_key)
        if existing is None:
            return None

        if existing["conversation_id"] != conversation_id or existing["operation"] != operation:
            raise IdempotencyConflictError(idempotency_key)

        payload_hash = compute_payload_hash(payload)
        if existing["payload_hash"] != payload_hash:
            raise IdempotencyConflictError(idempotency_key)

        existing_action = await self.store.get_action(existing["action_id"])
        if existing_action is None or existing_action["conversation_id"] != conversation_id:
            raise DownstreamOutcomeUnknownError(
                existing["action_id"],
                idempotency_key,
                "Idempotency record exists without matching action row; reconcile before retry",
            )

        # Verify engine matches original action (prevents cross-engine idempotency collisions)
        if existing_action["engine"] != engine.value:
            raise IdempotencyConflictError(idempotency_key)

        state = existing_action["state"]
        if state == ActionState.DONE.value:
            return {
                "actionId": existing_action["action_id"],
                "conversationId": conversation_id,
                "state": ActionState.DONE.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": True,
                "result": existing_action.get("result", {}),
            }
        if state == ActionState.HELD.value:
            return {
                "actionId": existing_action["action_id"],
                "conversationId": conversation_id,
                "state": ActionState.HELD.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": True,
            }
        if existing["outcome"] == Outcome.UNKNOWN.value or state == ActionState.DISPATCHING.value:
            raise DownstreamOutcomeUnknownError(
                existing_action["action_id"],
                idempotency_key,
                "Previous dispatch had unknown outcome or is still in-flight; reconcile before retry",
            )
        if state in (ActionState.REJECTED.value, ActionState.CANCELLED.value):
            return {
                "actionId": existing_action["action_id"],
                "conversationId": conversation_id,
                "state": state,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": True,
            }
        if state == ActionState.FAILED.value:
            return {
                "actionId": existing_action["action_id"],
                "conversationId": conversation_id,
                "state": ActionState.FAILED.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": True,
                "error": existing_action.get("error", {}),
            }

        raise DownstreamOutcomeUnknownError(
            existing_action["action_id"],
            idempotency_key,
            f"Previous action is in transitional state '{state}'",
        )

    async def _record_held_and_emit(
        self,
        action_id: str,
        submission: ActionSubmission,
        all_recipients: list[str],
        operation_id: str,
        effective_subject: str,
        mode: ConversationMode,
    ) -> dict[str, Any]:
        await self.store.record_action(
            action_id=action_id,
            conversation_id=submission.conversationId,
            engine=submission.engine,
            operation=submission.operation,
            idempotency_key=submission.idempotencyKey,
            initial_state=ActionState.HELD,
            payload=submission.payload,
            recipients=all_recipients,
            actor_guid=submission.actor.actorGuid,
            actor_kind=submission.actor.actorKind.value,
            context=submission.actor.to_event_context(),
            payload_ref=submission.payloadRef,
        )
        try:
            await self.history.record_control_event(
                conversation_id=submission.conversationId,
                actor=submission.actor,
                event_type=ControlEventType.ACTION_HELD,
                operation_id=operation_id,
                idempotency_key=submission.idempotencyKey,
                outcome=Outcome.OK,
                subject_line_display=effective_subject,
                correlation_id=submission.correlationId,
            )
        except StoreError:
            await self.store.update_conversation_mode(
                submission.conversationId, ConversationMode.FAILED_CLOSED
            )
            raise
        return {
            "actionId": action_id,
            "conversationId": submission.conversationId,
            "state": ActionState.HELD.value,
            "mode": mode.value,
            "idempotencyKey": submission.idempotencyKey,
        }

    async def submit_action(self, submission: ActionSubmission) -> dict[str, Any]:
        """Entry point for ALL controlled effects across n8n, Calnode, and Windmill (R-GATE-01)."""
        conv_id = submission.conversationId
        operation_id = f"{submission.engine.value}:{submission.operation}"
        runtime = self.runtimes.get(conv_id)

        # 1. Mandatory authorization check FIRST
        existing_conv = await self.store.get_conversation(conv_id)
        effective_subject = (
            submission.subjectLineDisplay
            or (existing_conv["subject_line"] if existing_conv else "")
            or ""
        )
        await self._check_actor_authz(
            actor=submission.actor,
            conversation_id=conv_id,
            operation_id=operation_id,
            idempotency_key=submission.idempotencyKey,
            subject_line_display=effective_subject,
            correlation_id=submission.correlationId,
        )

        # 2. Check existing FAILED_CLOSED BEFORE mutating SQLite (never overwrite subject_line on FAILED_CLOSED!)
        if existing_conv is not None and ConversationMode(existing_conv["mode"]) == ConversationMode.FAILED_CLOSED:
            raise GateFailedClosedError(conv_id)

        # 3. Validate ALL explicit + payload-embedded recipients BEFORE creating conversation rows (R-SEC-01)
        all_recipients = await self._validate_recipients_or_deny(
            conversation_id=conv_id,
            operation=submission.operation,
            operation_id=operation_id,
            idempotency_key=submission.idempotencyKey,
            explicit_recipients=submission.recipients,
            payload=submission.payload,
            actor=submission.actor,
            subject_line_display=effective_subject,
            correlation_id=submission.correlationId,
        )

        # 4. Now persist/update conversation row safely AFTER authz, FAILED_CLOSED, and allowlist pass
        owner_to_pin: str | None = None
        if submission.actor.actorKind.value == "USER":
            owner_to_pin = submission.actor.actorGuid
        else:
            delegated = (submission.actor.permissionProvenance or {}).get("delegatedBy")
            if isinstance(delegated, str) and delegated.strip():
                owner_to_pin = delegated.strip()

        conv = await self.store.get_or_create_conversation(
            conv_id, submission.subjectLineDisplay, owner_guid=owner_to_pin
        )
        effective_subject = submission.subjectLineDisplay or conv["subject_line"] or ""

        # 5. Fast admission check under state_lock
        action_id = f"act_{uuid4().hex[:12]}"
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            mode = ConversationMode(conv["mode"])

            if mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            replay = await self._check_existing_idempotency(
                conv_id,
                submission.idempotencyKey,
                submission.engine,
                submission.operation,
                submission.payload,
            )
            if replay is not None:
                return replay

            if mode in (ConversationMode.GATED, ConversationMode.TRANSITIONING):
                return await self._record_held_and_emit(
                    action_id=action_id,
                    submission=submission,
                    all_recipients=all_recipients,
                    operation_id=operation_id,
                    effective_subject=effective_subject,
                    mode=mode,
                )

        # 6. UNGATED path: serialize execution per conversation (R-GATE-07)
        async with runtime.dispatch_lock:
            async with runtime.state_lock:
                conv = await self.store.get_conversation(conv_id)
                mode = ConversationMode(conv["mode"])

                if mode == ConversationMode.FAILED_CLOSED:
                    raise GateFailedClosedError(conv_id)

                replay = await self._check_existing_idempotency(
                    conv_id,
                    submission.idempotencyKey,
                    submission.engine,
                    submission.operation,
                    submission.payload,
                )
                if replay is not None:
                    return replay

                if mode in (ConversationMode.GATED, ConversationMode.TRANSITIONING):
                    return await self._record_held_and_emit(
                        action_id=action_id,
                        submission=submission,
                        all_recipients=all_recipients,
                        operation_id=operation_id,
                        effective_subject=effective_subject,
                        mode=mode,
                    )

                await self.store.record_action(
                    action_id=action_id,
                    conversation_id=conv_id,
                    engine=submission.engine,
                    operation=submission.operation,
                    idempotency_key=submission.idempotencyKey,
                    initial_state=ActionState.RECEIVED,
                    payload=submission.payload,
                    recipients=all_recipients,
                    actor_guid=submission.actor.actorGuid,
                    actor_kind=submission.actor.actorKind.value,
                    context=submission.actor.to_event_context(),
                    payload_ref=submission.payloadRef,
                )

                await self._recheck_permission_or_fail(
                    action_id=action_id,
                    conversation_id=conv_id,
                    actor=submission.actor,
                    operation_id=operation_id,
                    idempotency_key=submission.idempotencyKey,
                    subject_line_display=effective_subject,
                    correlation_id=submission.correlationId,
                )

                await self.store.update_action_state(action_id, ActionState.AUTHORIZED)
                await self.store.update_action_state(action_id, ActionState.DISPATCHING)
                runtime.mark_inflight(action_id)

            return await self._execute_downstream(
                action_id=action_id,
                conversation_id=conv_id,
                engine=submission.engine,
                operation=submission.operation,
                payload=submission.payload,
                idempotency_key=submission.idempotencyKey,
                actor=submission.actor,
                subject_line_display=effective_subject,
                correlation_id=submission.correlationId,
            )

    async def dispatch_approved_action(
        self,
        action_id: str,
        actor: ActorContext,
        correlation_id: str | None = None,
        already_inflight: bool = False,
    ) -> dict[str, Any]:
        """Dispatches an already APPROVED/RELEASED action with mandatory fresh permission recheck (R-GATE-09).

        When `already_inflight=True` (reserved atomically by approve_action under state_lock), guarantees
        `runtime.mark_settled(action_id)` even if pre-dispatch checks raise before `_execute_downstream`.
        """
        initial = await self.store.get_action(action_id)
        if initial is None:
            raise ActionNotFoundError(action_id)

        conv_id = initial["conversation_id"]
        runtime = self.runtimes.get(conv_id)
        handed_to_executor = False

        try:
            async with runtime.dispatch_lock:
                async with runtime.state_lock:
                    conv = await self.store.get_conversation(conv_id)
                    if ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
                        raise GateFailedClosedError(conv_id)

                    action = await self.store.get_action(action_id)
                    if action is None:
                        raise ActionNotFoundError(action_id)

                    engine = EngineName(action["engine"])
                    operation = action["operation"]
                    operation_id = f"{engine.value}:{operation}"
                    idempotency_key = action["idempotency_key"]
                    payload = action["payload"]
                    recipients = action["recipients"]
                    subject_line_display = (conv["subject_line"] if conv else "") or ""

                    if action["state"] == ActionState.DONE.value:
                        return {
                            "actionId": action_id,
                            "conversationId": conv_id,
                            "state": ActionState.DONE.value,
                            "idempotencyKey": idempotency_key,
                            "idempotentReplay": True,
                            "result": action.get("result", {}),
                        }

                    if action["state"] != ActionState.APPROVED.value:
                        raise InvalidActionStateError(
                            action_id, action["state"], ActionState.APPROVED.value
                        )

                    # Re-verify ALL explicit + payload-embedded recipients before dispatch
                    all_recipients = extract_all_recipients(recipients, payload)
                    if operation in _OPERATIONS_REQUIRING_RECIPIENT and not all_recipients:
                        await self.store.update_action_state(
                            action_id,
                            ActionState.FAILED,
                            error={"code": "RECIPIENT_NOT_ALLOWLISTED", "recipient": "<missing_recipient>"},
                            outcome_override=Outcome.DENIED,
                        )
                        raise RecipientNotAllowlistedError("<missing_recipient>")

                    for recipient in all_recipients:
                        if not self.config.is_recipient_allowed(recipient):
                            await self.store.update_action_state(
                                action_id,
                                ActionState.FAILED,
                                error={"code": "RECIPIENT_NOT_ALLOWLISTED", "recipient": recipient},
                                outcome_override=Outcome.DENIED,
                            )
                            await self.history.record_control_event(
                                conversation_id=conv_id,
                                actor=actor,
                                event_type=ControlEventType.ACTION_FAILED,
                                operation_id=operation_id,
                                idempotency_key=idempotency_key,
                                outcome=Outcome.DENIED,
                                subject_line_display=subject_line_display,
                                correlation_id=correlation_id,
                            )
                            raise RecipientNotAllowlistedError(recipient)

                    await self._recheck_permission_or_fail(
                        action_id=action_id,
                        conversation_id=conv_id,
                        actor=actor,
                        operation_id=operation_id,
                        idempotency_key=idempotency_key,
                        subject_line_display=subject_line_display,
                        correlation_id=correlation_id,
                    )

                    await self.store.update_action_state(action_id, ActionState.AUTHORIZED)
                    await self.store.update_action_state(action_id, ActionState.DISPATCHING)
                    if not already_inflight:
                        runtime.mark_inflight(action_id)
                    handed_to_executor = True

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
        finally:
            if already_inflight and not handed_to_executor:
                async with runtime.state_lock:
                    runtime.mark_settled(action_id)

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
                outcome_override=Outcome.DENIED,
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
                outcome_override=Outcome.UNKNOWN,
            )
            await self.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
                allow_create=False,
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
            try:
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
            except StoreError as exc:
                logger.error(
                    "Audit publish failed after downstream success for action '%s': %s",
                    action_id,
                    exc,
                )
                await self.store.update_conversation_mode(
                    conversation_id, ConversationMode.FAILED_CLOSED
                )
                raise

            return {
                "actionId": action_id,
                "conversationId": conversation_id,
                "state": ActionState.DONE.value,
                "idempotencyKey": idempotency_key,
                "idempotentReplay": False,
                "result": result,
            }

        except UnauthorizedError as exc:
            # Adapter-level scope guard (e.g. arbitrary Windmill scriptPath or out-of-bounds Calnode calendar)
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error=exc.to_dict(),
                outcome_override=Outcome.DENIED,
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

        except DownstreamOutcomeUnknownError as exc:
            # Always emit the real Gate action_id (act_...) + engine in structured details
            extra = {k: v for k, v in exc.details.items() if k not in ("actionId", "idempotencyKey")}
            extra["engine"] = engine.value
            normalized_err = DownstreamOutcomeUnknownError(
                action_id=action_id,
                idempotency_key=idempotency_key,
                reason=exc.message,
                extra_details=extra,
            )
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error={
                    "code": normalized_err.code,
                    "message": normalized_err.message,
                    "outcome": "unknown",
                    "details": normalized_err.details,
                },
                outcome_override=Outcome.UNKNOWN,
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
                allow_create=False,
            )
            raise normalized_err from exc

        except DownstreamError as exc:
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error=exc.to_dict(),
                outcome_override=Outcome.ERROR,
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

        except Exception as exc:
            logger.exception("Unexpected error during downstream execution for action '%s'", action_id)
            unknown_err = DownstreamOutcomeUnknownError(
                action_id,
                idempotency_key,
                f"Unexpected adapter error: {type(exc).__name__}: {exc}",
                extra_details={"engine": engine.value},
            )
            await self.store.update_action_state(
                action_id,
                ActionState.FAILED,
                error={"code": unknown_err.code, "message": unknown_err.message, "outcome": "unknown"},
                outcome_override=Outcome.UNKNOWN,
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
                allow_create=False,
            )
            raise unknown_err from exc

        finally:
            async with runtime.state_lock:
                runtime.mark_settled(action_id)
