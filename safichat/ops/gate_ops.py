"""SafiChat Gate control endpoints (T-09: Barge In, Held Actions, Approve/Reject, Return to Ungated)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict

from gate.models import (
    ActionResolution,
    ActorContext,
    ConversationMode,
    GateOperationRequest,
)

router = APIRouter(prefix="/safichat/ops/gate", tags=["safichat-gate-controls"])


class BargeInRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str | None = None
    subjectLine: str | None = None
    actor: ActorContext
    correlationId: str | None = None


class ListHeldRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str | None = None
    subjectLine: str | None = None
    actor: ActorContext


class ActionDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str
    actionId: str
    actor: ActorContext
    correlationId: str | None = None


class ReturnToUngatedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str | None = None
    subjectLine: str | None = None
    resolutions: list[ActionResolution] = []
    actor: ActorContext
    correlationId: str | None = None


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str
    targetMode: ConversationMode
    actor: ActorContext
    correlationId: str | None = None


@router.post("/barge-in")
async def barge_in(req: BargeInRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="BARGE_IN",
        conversationId=req.conversationId,
        subjectLine=req.subjectLine,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
        correlationId=req.correlationId,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.barge_in(
        actor=req.actor,
        conversation_id=req.conversationId,
        subject_line=req.subjectLine,
        correlation_id=req.correlationId,
    )


@router.post("/held")
async def list_held(req: ListHeldRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="LIST_HELD",
        conversationId=req.conversationId,
        subjectLine=req.subjectLine,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.list_held_actions(
        actor=req.actor,
        conversation_id=req.conversationId,
        subject_line=req.subjectLine,
    )


@router.post("/approve")
async def approve_action(req: ActionDecisionRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="APPROVE_ACTION",
        conversationId=req.conversationId,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
        actionId=req.actionId,
        correlationId=req.correlationId,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.approve_action(
        conversation_id=req.conversationId,
        action_id=req.actionId,
        actor=req.actor,
        correlation_id=req.correlationId,
    )


@router.post("/reject")
async def reject_action(req: ActionDecisionRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="REJECT_ACTION",
        conversationId=req.conversationId,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
        actionId=req.actionId,
        correlationId=req.correlationId,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.reject_action(
        conversation_id=req.conversationId,
        action_id=req.actionId,
        actor=req.actor,
        correlation_id=req.correlationId,
    )


@router.post("/ungate")
async def return_to_ungated(req: ReturnToUngatedRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="RETURN_TO_UNGATED",
        conversationId=req.conversationId,
        subjectLine=req.subjectLine,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
        resolutions=req.resolutions,
        correlationId=req.correlationId,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.return_to_ungated(
        actor=req.actor,
        conversation_id=req.conversationId,
        subject_line=req.subjectLine,
        resolutions=req.resolutions,
        correlation_id=req.correlationId,
    )


@router.post("/reconcile")
async def reconcile(req: ReconcileRequest, request: Request) -> dict[str, Any]:
    GateOperationRequest(
        operationType="RECONCILE",
        conversationId=req.conversationId,
        actorGuid=req.actor.actorGuid,
        actorKind=req.actor.actorKind,
        targetMode=req.targetMode,
        correlationId=req.correlationId,
    ).to_schema_dict()
    modes = request.app.state.modes
    return await modes.reconcile_failed_closed(
        conversation_id=req.conversationId,
        target_mode=req.targetMode,
        actor=req.actor,
        correlation_id=req.correlationId,
    )
