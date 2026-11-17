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
    AmbiguousSubjectLineError,
    ConversationNotFoundError,
    DownstreamError,
    DownstreamOutcomeUnknownError,
    GateError,
    GateFailedClosedError,
    HeldActionsUnresolvedError,
    IdentityUncertainError,
    InvalidActionStateError,
    InvalidModeTransitionError,
    PermissionCheckFailedError,
    RecipientNotAllowlistedError,
    StoreError,
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
    """Manages caller-scoped Subject Line resolution, Barge In, Held Action review, and Return to Ungated."""

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

    async def resolve_conversation_for_actor(
        self,
        actor: ActorContext,
        operation: str,
        conversation_id: str | None = None,
        subject_line: str | None = None,
        idempotency_key: str = "",
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Resolves a conversation safely WITH authorization enforced BEFORE leaking metadata or squatting (R-GATE-04, A09)."""
        if conversation_id:
            conv = await self.store.get_conversation(conversation_id)
            if not conv:
                # Non-mutating check: never pin ownership in memory for a nonexistent conversation (prevents squatting!)
                if not await self.authz.can_access_conversation(actor, conversation_id, owner_guid=None):
                    raise UnauthorizedError(
                        f"Actor '{actor.actorGuid}' is not authorized for conversation '{conversation_id}'"
                    )
                raise ConversationNotFoundError(conversation_id)

            display = conv["subject_line"] or ""
            await self._check_authz(
                actor,
                conversation_id,
                operation,
                idempotency_key or f"{operation}:{conversation_id}",
                display,
                correlation_id,
            )
            return conv

        if subject_line:
            all_matches = await self.store.list_conversations_by_subject(subject_line)
            authorized_matches: list[dict[str, Any]] = []
            for row in all_matches:
                if await self.authz.can_access_conversation(
                    actor, row["conversation_id"], owner_guid=row.get("owner_guid")
                ):
                    authorized_matches.append(row)

            if not authorized_matches:
                if all_matches:
                    raise UnauthorizedError(
                        f"Actor '{actor.actorGuid}' is not authorized to access conversations matching '{subject_line}'"
                    )
                raise ConversationNotFoundError(subject_line)

            if len(authorized_matches) > 1:
                candidates = [
                    {
                        "conversationId": r["conversation_id"],
                        "subjectLine": r["subject_line"],
                        "mode": r["mode"],
                    }
                    for r in authorized_matches
                ]
                raise AmbiguousSubjectLineError(subject_line, candidates)

            chosen = authorized_matches[0]
            await self._check_authz(
                actor,
                chosen["conversation_id"],
                operation,
                idempotency_key or f"{operation}:{chosen['conversation_id']}",
                chosen["subject_line"] or "",
                correlation_id,
            )
            return chosen

        raise ConversationNotFoundError("<empty>")

    async def authorize_read_operation(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
        subject_line_display: str = "",
    ) -> None:
        """Enforces fail-closed authz, persists owner_guid on new conversations, and guards FAILED_CLOSED."""
        conv = await self.store.get_conversation(conversation_id)
        effective_subject = subject_line_display or (conv["subject_line"] if conv else "") or ""
        await self._check_authz(
            actor,
            conversation_id,
            operation,
            f"read:{operation}:{conversation_id}",
            effective_subject,
            None,
        )
        if conv is None and actor.actorKind.value == "USER":
            await self.store.get_or_create_conversation(
                conversation_id, subject_line_display, owner_guid=actor.actorGuid
            )
            conv = await self.store.get_conversation(conversation_id)
        if conv and ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
            raise GateFailedClosedError(conversation_id)

    async def barge_in(
        self,
        actor: ActorContext,
        conversation_id: str | None = None,
        subject_line: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Switches ONE selected conversation to GATED, waiting for in-flight actions (R-GATE-03/04/08, A06/A08)."""
        op_id = f"barge_in:{uuid4().hex[:8]}"
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:barge_in",
            conversation_id=conversation_id,
            subject_line=subject_line,
            idempotency_key=f"mode:gated:{conversation_id or 'subj'}:{op_id}",
            correlation_id=correlation_id,
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        idem_key = f"mode:gated:{conv_id}:{op_id}"

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            current_mode = ConversationMode(conv["mode"])

            if current_mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            if current_mode == ConversationMode.GATED:
                inflight_snapshot = list(runtime.inflight_action_ids)
            elif current_mode == ConversationMode.TRANSITIONING:
                runtime.barge_in_requested = True
                await self.store.update_conversation_mode(
                    conv_id,
                    ConversationMode.TRANSITIONING,
                    target_mode=ConversationMode.GATED,
                )
                inflight_snapshot = list(runtime.inflight_action_ids)
            else:
                assert_valid_mode_transition(current_mode, ConversationMode.TRANSITIONING)
                runtime.barge_in_requested = True
                await self.store.update_conversation_mode(
                    conv_id,
                    ConversationMode.TRANSITIONING,
                    target_mode=ConversationMode.GATED,
                )
                inflight_snapshot = list(runtime.inflight_action_ids)

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
                allow_create=False,
            )
            raise GateFailedClosedError(
                conv_id,
                "Timed out reconciling in-flight actions during Barge In; transitioned to FAILED_CLOSED",
            )

        # Atomically finalize GATED; only the single caller that transitions non-GATED -> GATED emits MODE_GATED
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            now_mode = ConversationMode(conv["mode"])
            if now_mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)

            if now_mode != ConversationMode.GATED:
                assert_valid_mode_transition(now_mode, ConversationMode.GATED)
                await self.store.update_conversation_mode(conv_id, ConversationMode.GATED)
                try:
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
                except StoreError:
                    await self.store.update_conversation_mode(
                        conv_id, ConversationMode.FAILED_CLOSED
                    )
                    raise

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
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:list_held",
            conversation_id=conversation_id,
            subject_line=subject_line,
            idempotency_key=f"list:{conversation_id or 'subj'}",
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
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
        """Approves a HELD action atomically under state_lock AND reserves in-flight slot (zero TOCTOU race with barge_in)."""
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:approve_action",
            conversation_id=conversation_id,
            idempotency_key=f"approve:{action_id}",
            correlation_id=correlation_id,
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            mode = ConversationMode(conv["mode"])
            if mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)
            if mode != ConversationMode.GATED:
                raise InvalidModeTransitionError(mode.value, "APPROVE_ACTION")

            action = await self.store.get_action(action_id)
            if not action or action["conversation_id"] != conv_id:
                raise ActionNotFoundError(action_id)

            if action["state"] != ActionState.HELD.value:
                raise InvalidActionStateError(
                    action_id, action["state"], ActionState.HELD.value
                )

            await self.store.update_action_state(action_id, ActionState.APPROVED)
            try:
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
            except StoreError:
                await self.store.update_action_state(
                    action_id,
                    ActionState.FAILED,
                    error={"code": "STORE_ERROR", "message": "Audit publish failed on approve"},
                    outcome_override=Outcome.UNKNOWN,
                )
                await self.store.update_conversation_mode(
                    conv_id, ConversationMode.FAILED_CLOSED
                )
                raise

            # Atomically reserve in-flight BEFORE releasing state_lock so barge_in / return_to_ungated sees it!
            runtime.mark_inflight(action_id)

        return await self.dispatcher.dispatch_approved_action(
            action_id=action_id,
            actor=actor,
            correlation_id=correlation_id,
            already_inflight=True,
        )

    async def reject_action(
        self,
        conversation_id: str,
        action_id: str,
        actor: ActorContext,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        """Rejects a HELD action atomically under state_lock (ONLY while mode is GATED) and records ACTION_REJECTED."""
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:reject_action",
            conversation_id=conversation_id,
            idempotency_key=f"reject:{action_id}",
            correlation_id=correlation_id,
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            mode = ConversationMode(conv["mode"])
            if mode == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)
            if mode != ConversationMode.GATED:
                raise InvalidModeTransitionError(mode.value, "REJECT_ACTION")

            action = await self.store.get_action(action_id)
            if not action or action["conversation_id"] != conv_id:
                raise ActionNotFoundError(action_id)

            if action["state"] != ActionState.HELD.value:
                raise InvalidActionStateError(
                    action_id, action["state"], ActionState.HELD.value
                )

            await self.store.update_action_state(action_id, ActionState.REJECTED)
            try:
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
            except StoreError:
                await self.store.update_conversation_mode(
                    conv_id, ConversationMode.FAILED_CLOSED
                )
                raise

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
        op_id = f"ungate:{uuid4().hex[:8]}"
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:return_to_ungated",
            conversation_id=conversation_id,
            subject_line=subject_line,
            idempotency_key=f"mode:ungated:{conversation_id or 'subj'}:{op_id}",
            correlation_id=correlation_id,
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        idem_key = f"mode:ungated:{conv_id}:{op_id}"

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
            held_ids = {a["action_id"] for a in held_actions}

            for provided_aid in resolution_map:
                if provided_aid not in held_ids:
                    raise ActionNotFoundError(provided_aid)

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
            runtime.barge_in_requested = False
            await self.store.update_conversation_mode(
                conv_id,
                ConversationMode.TRANSITIONING,
                target_mode=ConversationMode.UNGATED,
            )

        released_results: list[dict[str, Any]] = []
        cancelled_ids: list[str] = []

        try:
            for action in held_actions:
                async with runtime.state_lock:
                    if runtime.barge_in_requested:
                        return {
                            "conversationId": conv_id,
                            "subjectLineDisplay": display_subject,
                            "mode": ConversationMode.GATED.value,
                            "interruptedByBargeIn": True,
                            "released": released_results,
                            "cancelled": cancelled_ids,
                        }

                aid = action["action_id"]
                decision = resolution_map[aid]
                if decision == ReleaseDecision.CANCEL:
                    async with runtime.state_lock:
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
                    async with runtime.state_lock:
                        await self.store.update_action_state(aid, ActionState.APPROVED)
                        try:
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
                        except StoreError:
                            await self.store.update_action_state(
                                aid,
                                ActionState.FAILED,
                                error={"code": "STORE_ERROR", "message": "Audit publish failed on release"},
                                outcome_override=Outcome.UNKNOWN,
                            )
                            raise
                        runtime.mark_inflight(aid)
                    res = await self.dispatcher.dispatch_approved_action(
                        action_id=aid,
                        actor=actor,
                        correlation_id=correlation_id,
                        already_inflight=True,
                    )
                    released_results.append(res)

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
                allow_create=False,
            )
            raise GateFailedClosedError(
                conv_id,
                "Timed out reconciling in-flight actions during Return to Ungated; transitioned to FAILED_CLOSED",
            )
        except (
            StoreError,
            PermissionCheckFailedError,
            IdentityUncertainError,
            DownstreamOutcomeUnknownError,
        ):
            # Store/JetStream/authz/unknown failures MUST fail closed (NEVER revert to GATED!)
            async with runtime.state_lock:
                conv_now = await self.store.get_conversation(conv_id)
                if conv_now and ConversationMode(conv_now["mode"]) != ConversationMode.FAILED_CLOSED:
                    await self.store.update_conversation_mode(
                        conv_id, ConversationMode.FAILED_CLOSED
                    )
            raise
        except (
            UnauthorizedError,
            RecipientNotAllowlistedError,
            DownstreamError,
            GateError,
        ):
            async with runtime.state_lock:
                conv_now = await self.store.get_conversation(conv_id)
                if conv_now and ConversationMode(conv_now["mode"]) == ConversationMode.TRANSITIONING:
                    await self.store.update_conversation_mode(
                        conv_id, ConversationMode.GATED
                    )
            raise
        except Exception:
            await self.dispatcher.fail_closed(
                conversation_id=conv_id,
                actor=actor,
                operation_id=op_id,
                idempotency_key=idem_key,
                subject_line_display=display_subject,
                correlation_id=correlation_id,
                allow_create=False,
            )
            raise

        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            if ConversationMode(conv["mode"]) == ConversationMode.FAILED_CLOSED:
                raise GateFailedClosedError(conv_id)
            if runtime.barge_in_requested or ConversationMode(conv["mode"]) == ConversationMode.GATED:
                return {
                    "conversationId": conv_id,
                    "subjectLineDisplay": display_subject,
                    "mode": ConversationMode.GATED.value,
                    "interruptedByBargeIn": True,
                    "released": released_results,
                    "cancelled": cancelled_ids,
                }

            newly_held = await self.store.get_held_actions(conv_id)
            if newly_held:
                await self.store.update_conversation_mode(conv_id, ConversationMode.GATED)
                raise HeldActionsUnresolvedError(
                    conv_id,
                    [
                        {
                            "actionId": a["action_id"],
                            "engine": a["engine"],
                            "operation": a["operation"],
                            "idempotencyKey": a["idempotency_key"],
                        }
                        for a in newly_held
                    ],
                )

            assert_valid_mode_transition(
                ConversationMode(conv["mode"]), ConversationMode.UNGATED
            )
            await self.store.update_conversation_mode(conv_id, ConversationMode.UNGATED)
            try:
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
            except StoreError:
                await self.store.update_conversation_mode(
                    conv_id, ConversationMode.FAILED_CLOSED
                )
                raise

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
        """Reconciles a FAILED_CLOSED conversation after operator verification (E4, E6).

        Refuses reconciling directly to UNGATED if HELD actions still exist (prevents orphaning HELD items).
        """
        if target_mode not in (ConversationMode.UNGATED, ConversationMode.GATED):
            raise InvalidModeTransitionError(
                ConversationMode.FAILED_CLOSED.value, target_mode.value
            )

        op_id = f"reconcile:{uuid4().hex[:8]}"
        conv = await self.resolve_conversation_for_actor(
            actor=actor,
            operation="gate:reconcile",
            conversation_id=conversation_id,
            idempotency_key=f"reconcile:{conversation_id}:{op_id}",
            correlation_id=correlation_id,
        )
        conv_id = conv["conversation_id"]
        display_subject = conv["subject_line"] or ""
        idem_key = f"reconcile:{conv_id}:{op_id}"

        runtime = self.runtimes.get(conv_id)
        async with runtime.state_lock:
            conv = await self.store.get_conversation(conv_id)
            if ConversationMode(conv["mode"]) != ConversationMode.FAILED_CLOSED:
                raise InvalidModeTransitionError(conv["mode"], "RECONCILE")

            if target_mode == ConversationMode.UNGATED:
                held = await self.store.get_held_actions(conv_id)
                if held:
                    raise HeldActionsUnresolvedError(
                        conv_id,
                        [
                            {
                                "actionId": a["action_id"],
                                "engine": a["engine"],
                                "operation": a["operation"],
                                "idempotencyKey": a["idempotency_key"],
                            }
                            for a in held
                        ],
                    )

            await self.store.update_conversation_mode(conv_id, target_mode)
            event_type = (
                ControlEventType.MODE_GATED
                if target_mode == ConversationMode.GATED
                else ControlEventType.MODE_UNGATED
            )
            try:
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
            except StoreError:
                await self.store.update_conversation_mode(
                    conv_id, ConversationMode.FAILED_CLOSED
                )
                raise

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
        except (PermissionCheckFailedError, IdentityUncertainError):
            await self.dispatcher.fail_closed(
                conversation_id=conversation_id,
                actor=actor,
                operation_id=operation,
                idempotency_key=idempotency_key,
                subject_line_display=subject_line_display,
                correlation_id=correlation_id,
                allow_create=False,
            )
            raise
