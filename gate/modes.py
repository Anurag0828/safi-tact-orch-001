"""Barge In, Gated/Ungated mode transitions, and held-action review (T-09, A06, A07, A08)."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

from gate.authz import PermissionProvider
from gate.config import GateConfig
from gate.dispatch import GateDispatcher
from gate.errors import (
    ActionNotFoundError,
    ConversationNotFoundError,
    GateFailedClosedError,
    HeldActionsUnresolvedError,
    InvalidActionStateError,
    InvalidModeTransitionError,
    PermissionCheckFailedError,
    UnauthorizedError,
)
from gate.history import HistoryPublisher
from gate.models import (
    ActionResolution,
    ActionState,
    ActorContext,
    ControlEventType,
    ConversationMode,
    Outcome,
    ReleaseDecision,
)
from gate.state_machine import (
    ConversationRuntimeRegistry,
    assert_valid_mode_transition,
)
from gate.store import GateStore


class GateModeController:
    """Manages Subject Line resolution, Barge In, Held Action review, and Return to Ungated."""

    def __init__(
        self,
        config: GateConfig,
        store: GateStore,
        history: HistoryPublisher,
        authz: PermissionProvider,
        runtimes: ConversationRuntimeRegistry,
        dispatcher: GateDispatcher,
    ) -> None:
        self.config = config
        self.store = store
        self.history = history
        self.authz = authz
        self.runtimes = runtimes
        self.dispatcher = dispatcher

    async def resolve_conversation(
        self,
        conversation_id: str | None = None,
        subject_line: str | None = None,
    ) -> dict[str, Any]:
        """Resolves a conversation by ConversationID or Subject Line (R-GATE-04)."""
        if conversation_id:
            conv = await self.store.get_conversation(conversation_id)
            if not conv:
                raise ConversationNotFoundError(conversation_id)
            return conv
        if subject_line:
            return await self.store.resolve_conversation_by_subject(subject_line)
        raise ConversationNotFoundError("<empty>")

    async def barge_in(
        self,
        actor: ActorContext,
        conversation_id: str | None = None,
        subject_line: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Switches ONE selected conversation to GATED, waiting for in-flight actions (R-GATE-03/04/08, A06/A08)."""
        conv = await self.resolve_conversation(conversation_id, subject_line)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        op_id = f"barge_in:{uuid4().hex[:8]}"
        idem_key = f"mode:gated:{conv_id}:{op_id}"

        await self._check_authz(actor, conv_id, "gate:barge_in", idem_key, display_subject, correlation_id)

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            current_mode = ConversationMode(conv["mode"])

            if current_mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)
            if current_mode == ConversationMode.GATED:
                return {
                    "conversationId": conv_id,
                    "subjectLineDisplay": display_subject,
                    "mode": ConversationMode.GATED.value,
                    "reconciledInFlight": [],
                    "undone": False,
                }

            assert_valid_mode_transition(current_mode, ConversationMode.TRANSITIONING)
            await self.store.update_conversation_mode(
                conv_id,
                ConversationMode.TRANSITIONING,
                target_mode=ConversationMode.GATED,
            )
            inflight_snapshot = list(runtime.inflight_action_ids)

        # Wait outside state_lock for already-started in-flight actions to finish (A08)
        try:
            await asyncio.wait_for(
                runtime.inflight_zero.wait(),
                timeout=self.config.inflight_reconcile_timeout_seconds,
            )
        except asyncio.TimeoutError:
            await self.dispatcher.fail_closed(
                conversation_id=conv_id,
                actor=actor,
                operation_id=op_id,
                idempotency_key=idem_key,
                subject_line_display=display_subject,
                correlation_id=correlation_id,
            )
            raise GateFailedClosedError(
                conv_id,
                "Timed out reconciling in-flight actions during Barge In; transitioned to FAILED_CLOSED",
            )

        # Confirm GATED only after zero in-flight actions remain
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            if ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            assert_valid_mode_transition(
                ConversationMode(conv["mode"]), ConversationMode.GATED
            )
            await self.store.update_conversation_mode(conv_id, ConversationMode.GATED)

            await self.history.record_control_event(
                conversation_id=conv_id,
                actor=actor,
                event_type=ControlEventType.MODE_GATED,
                operation_id=op_id,
                idempotency_key=idem_key,
                outcome=Outcome.OK,
                subject_line_display=display_subject,
                correlation_id=correlation_id,
            )

        # Gather final reconciled states of actions that were already in-flight (Invariant 6: never claim undo)
        reconciled = []
        for aid in inflight_snapshot:
            rec = await self.store.get_action(aid)
            if rec:
                reconciled.append(
                    {
                        "actionId": aid,
                        "engine": rec["engine"],
                        "operation": rec["operation"],
                        "finalState": rec["state"],
                        "undone": False,
                    }
                )

        return {
            "conversationId": conv_id,
            "subjectLineDisplay": display_subject,
            "mode": ConversationMode.GATED.value,
            "reconciledInFlight": reconciled,
            "undone": False,
        }

    async def list_held_actions(
        self,
        actor: ActorContext,
        conversation_id: str | None = None,
        subject_line: str | None = None,
    ) -> dict[str, Any]:
        conv = await self.resolve_conversation(conversation_id, subject_line)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        await self._check_authz(
            actor, conv_id, "gate:list_held", f"list:{conv_id}", display_subject, None
        )
        held = await self.store.get_held_actions(conv_id)
        return {
            "conversationId": conv_id,
            "subjectLineDisplay": display_subject,
            "mode": conv["mode"],
            "heldActions": [
                {
                    "actionId": a["action_id"],
                    "engine": a["engine"],
                    "operation": a["operation"],
                    "idempotencyKey": a["idempotency_key"],
                    "state": a["state"],
                    "createdAt": a["created_at"],
                }
                for a in held
            ],
        }

    async def approve_action(
        self,
        conversation_id: str,
        action_id: str,
        actor: ActorContext,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Approves a HELD action, records ACTION_APPROVED, rechecks authz, and dispatches (E4, R-GATE-05/09)."""
        conv = await self.resolve_conversation(conversation_id=conversation_id)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""

        action = await self.store.get_action(action_id)
        if not action or action["conversation_id"] != conv_id:
            raise ActionNotFoundError(action_id)

        await self._check_authz(
            actor,
            conv_id,
            "gate:approve_action",
            action["idempotency_key"],
            display_subject,
            correlation_id,
        )

        if action["state"] != ActionState.HELD.value:
            raise InvalidActionStateError(action_id, action["state"], ActionState.HELD.value)

        await self.store.update_action_state(action_id, ActionState.APPROVED)
        await self.history.record_control_event(
            conversation_id=conv_id,
            actor=actor,
            event_type=ControlEventType.ACTION_APPROVED,
            operation_id=f"{action['engine']}:{action['operation']}",
            idempotency_key=action["idempotency_key"],
            outcome=Outcome.OK,
            subject_line_display=display_subject,
            correlation_id=correlation_id,
        )

        # Immediately recheck permission & dispatch through single Gate Dispatcher
        return await self.dispatcher.dispatch_approved_action(
            action_id=action_id, actor=actor, correlation_id=correlation_id
        )

    async def reject_action(
        self,
        conversation_id: str,
        action_id: str,
        actor: ActorContext,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Rejects a HELD action and records ACTION_REJECTED (E4, R-GATE-05)."""
        conv = await self.resolve_conversation(conversation_id=conversation_id)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""

        action = await self.store.get_action(action_id)
        if not action or action["conversation_id"] != conv_id:
            raise ActionNotFoundError(action_id)

        await self._check_authz(
            actor,
            conv_id,
            "gate:reject_action",
            action["idempotency_key"],
            display_subject,
            correlation_id,
        )

        if action["state"] != ActionState.HELD.value:
            raise InvalidActionStateError(action_id, action["state"], ActionState.HELD.value)

        await self.store.update_action_state(action_id, ActionState.REJECTED)
        await self.history.record_control_event(
            conversation_id=conv_id,
            actor=actor,
            event_type=ControlEventType.ACTION_REJECTED,
            operation_id=f"{action['engine']}:{action['operation']}",
            idempotency_key=action["idempotency_key"],
            outcome=Outcome.OK,
            subject_line_display=display_subject,
            correlation_id=correlation_id,
        )

        return {
            "actionId": action_id,
            "conversationId": conv_id,
            "state": ActionState.REJECTED.value,
        }

    async def return_to_ungated(
        self,
        actor: ActorContext,
        conversation_id: str | None = None,
        subject_line: str | None = None,
        resolutions: list[ActionResolution] | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Returns conversation from GATED to UNGATED after explicit release/cancel of every held action (R-GATE-06, A07)."""
        conv = await self.resolve_conversation(conversation_id, subject_line)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        op_id = f"ungate:{uuid4().hex[:8]}"
        idem_key = f"mode:ungated:{conv_id}:{op_id}"

        await self._check_authz(
            actor, conv_id, "gate:return_to_ungated", idem_key, display_subject, correlation_id
        )

        runtime = self.runtimes.get(conv_id)
        resolution_map = {r.actionId: r.decision for r in (resolutions or [])}

        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            current_mode = ConversationMode(conv["mode"])
            if current_mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)
            if current_mode != ConversationMode.GATED:
                raise InvalidModeTransitionError(current_mode.value, "RETURN_TO_UNGATED")

            held_actions = await self.store.get_held_actions(conv_id)
            missing = [
                {
                    "actionId": a["action_id"],
                    "engine": a["engine"],
                    "operation": a["operation"],
                    "idempotencyKey": a["idempotency_key"],
                }
                for a in held_actions
                if a["action_id"] not in resolution_map
            ]
            if missing:
                raise HeldActionsUnresolvedError(conv_id, missing)

            assert_valid_mode_transition(current_mode, ConversationMode.TRANSITIONING)
            await self.store.update_conversation_mode(
                conv_id,
                ConversationMode.TRANSITIONING,
                target_mode=ConversationMode.UNGATED,
            )

        released_results: list[dict[str, Any]] = []
        cancelled_ids: list[str] = []

        for action in held_actions:
            aid = action["action_id"]
            decision = resolution_map[aid]
            if decision == ReleaseDecision.CANCEL:
                await self.store.update_action_state(aid, ActionState.CANCELLED)
                await self.history.record_control_event(
                    conversation_id=conv_id,
                    actor=actor,
                    event_type=ControlEventType.ACTION_CANCELLED,
                    operation_id=f"{action['engine']}:{action['operation']}",
                    idempotency_key=action["idempotency_key"],
                    outcome=Outcome.OK,
                    subject_line_display=display_subject,
                    correlation_id=correlation_id,
                )
                cancelled_ids.append(aid)
            elif decision == ReleaseDecision.RELEASE:
                await self.store.update_action_state(aid, ActionState.APPROVED)
                await self.history.record_control_event(
                    conversation_id=conv_id,
                    actor=actor,
                    event_type=ControlEventType.ACTION_RELEASED,
                    operation_id=f"{action['engine']}:{action['operation']}",
                    idempotency_key=action["idempotency_key"],
                    outcome=Outcome.OK,
                    subject_line_display=display_subject,
                    correlation_id=correlation_id,
                )
                res = await self.dispatcher.dispatch_approved_action(
                    action_id=aid, actor=actor, correlation_id=correlation_id
                )
                released_results.append(res)

        await asyncio.wait_for(
            runtime.inflight_zero.wait(),
            timeout=self.config.inflight_reconcile_timeout_seconds,
        )

        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            if ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            await self.store.update_conversation_mode(conv_id, ConversationMode.UNGATED)
            await self.history.record_control_event(
                conversation_id=conv_id,
                actor=actor,
                event_type=ControlEventType.MODE_UNGATED,
                operation_id=op_id,
                idempotency_key=idem_key,
                outcome=Outcome.OK,
                subject_line_display=display_subject,
                correlation_id=correlation_id,
            )

        return {
            "conversationId": conv_id,
            "subjectLineDisplay": display_subject,
            "mode": ConversationMode.UNGATED.value,
            "released": released_results,
            "cancelled": cancelled_ids,
        }

    async def reconcile_failed_closed(
        self,
        conversation_id: str,
        target_mode: ConversationMode,
        actor: ActorContext,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Reconciles a FAILED_CLOSED conversation after operator verification (E4, E6)."""
        if target_mode not in (ConversationMode.UNGATED, ConversationMode.GATED):
            raise InvalidModeTransitionError(ConversationMode.FAILED_CLOSED.value, target_mode.value)

        conv = await self.resolve_conversation(conversation_id=conversation_id)
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        op_id = f"reconcile:{uuid4().hex[:8]}"
        idem_key = f"reconcile:{conv_id}:{op_id}"

        await self._check_authz(
            actor, conv_id, "gate:reconcile", idem_key, display_subject, correlation_id
        )

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            if ConversationMode(conv["mode"]) != ConversationMode.FAILED_CLOSED:
                raise InvalidModeTransitionError(conv["mode"], "RECONCILE")

            await self.store.update_conversation_mode(conv_id, target_mode)
            event_type = (
                ControlEventType.MODE_GATED
                if target_mode == ConversationMode.GATED
                else ControlEventType.MODE_UNGATED
            )
            await self.history.record_control_event(
                conversation_id=conv_id,
                actor=actor,
                event_type=event_type,
                operation_id=op_id,
                idempotency_key=idem_key,
                outcome=Outcome.OK,
                subject_line_display=display_subject,
                correlation_id=correlation_id,
            )

        return {
            "conversationId": conv_id,
            "mode": target_mode.value,
            "reconciled": True,
        }

    async def _check_authz(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
        idempotency_key: str,
        subject_line_display: str,
        correlation_id: str | None,
    ) -> None:
        try:
            await self.authz.check_permission(actor, conversation_id, operation)
        except UnauthorizedError:
            await self.history.record_control_event(
                conversation_id=conversation_id,
                actor=actor,
                event_type=ControlEventType.ACTION_FAILED,
                operation_id=operation,
                idempotency_key=idempotency_key,
                outcome=Outcome.DENIED,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise
        except PermissionCheckFailedError:
            await self.dispatcher.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
            )
            raise
