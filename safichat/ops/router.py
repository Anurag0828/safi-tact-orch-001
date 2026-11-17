"""SafiChat business operations router (T-06: R-CHAT-01, R-CHAT-02, A02, A03, A04, A05)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field

from gate.models import ActionSubmission, ActorContext, EngineName

router = APIRouter(prefix="/safichat/ops", tags=["safichat-business-ops"])


class LeadIngestRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    leadEmail: str = Field(..., min_length=3)
    leadName: str = ""
    inquiryText: str = ""
    idempotencyKey: str = Field(..., min_length=1)
    actor: ActorContext
    correlationId: str | None = None


class FollowupDispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    recipientEmail: str = Field(..., min_length=3)
    messageBody: str = Field(..., min_length=1)
    resumeUrl: str | None = None
    idempotencyKey: str = Field(..., min_length=1)
    actor: ActorContext
    correlationId: str | None = None


class SlotsQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    calendarId: str | None = None
    dateFrom: str | None = None
    dateTo: str | None = None
    actor: ActorContext


class BookingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    calendarId: str | None = None
    slotId: str = Field(..., min_length=1)
    attendeeEmail: str = Field(..., min_length=3)
    attendeeName: str = ""
    notes: str = ""
    idempotencyKey: str = Field(..., min_length=1)
    actor: ActorContext
    correlationId: str | None = None


class AssignmentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    bookingId: str = Field(..., min_length=1)
    assignee: str = Field(..., min_length=3)
    taskSummary: str = ""
    idempotencyKey: str = Field(..., min_length=1)
    actor: ActorContext
    correlationId: str | None = None


class JobStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1)
    jobId: str = Field(..., min_length=1)
    subjectLineDisplay: str = ""
    actor: ActorContext


@router.post("/marketing/ingest-lead")
async def ingest_lead(req: LeadIngestRequest, request: Request) -> dict[str, Any]:
    """R-MKT-01: Ingests a consented sample lead via n8n through the Gate."""
    dispatcher = request.app.state.dispatcher
    submission = ActionSubmission(
        conversationId=req.conversationId,
        subjectLineDisplay=req.subjectLineDisplay,
        engine=EngineName.N8N,
        operation="ingest_lead",
        idempotencyKey=req.idempotencyKey,
        payload={
            "conversationId": req.conversationId,
            "subjectLineDisplay": req.subjectLineDisplay,
            "leadEmail": req.leadEmail,
            "leadName": req.leadName,
            "inquiryText": req.inquiryText,
            "correlationId": req.correlationId,
            "originActor": {
                "actorGuid": req.actor.actorGuid,
                "actorKind": req.actor.actorKind.value,
                "meetingContext": req.actor.meetingContext,
                "portContext": req.actor.portContext,
                "permissionProvenance": req.actor.permissionProvenance,
            },
        },
        recipients=[req.leadEmail],
        actor=req.actor,
        correlationId=req.correlationId,
    )
    return await dispatcher.submit_action(submission)


@router.post("/marketing/followup")
async def dispatch_followup(req: FollowupDispatchRequest, request: Request) -> dict[str, Any]:
    """R-MKT-02: Dispatches an approved marketing follow-up ONLY through the Gate."""
    dispatcher = request.app.state.dispatcher
    payload: dict[str, Any] = {
        "conversationId": req.conversationId,
        "recipientEmail": req.recipientEmail,
        "messageBody": req.messageBody,
    }
    if req.resumeUrl:
        payload["resumeUrl"] = req.resumeUrl

    submission = ActionSubmission(
        conversationId=req.conversationId,
        subjectLineDisplay=req.subjectLineDisplay,
        engine=EngineName.N8N,
        operation="dispatch_followup",
        idempotencyKey=req.idempotencyKey,
        payload=payload,
        recipients=[req.recipientEmail],
        actor=req.actor,
        correlationId=req.correlationId,
    )
    return await dispatcher.submit_action(submission)


@router.post("/sales/slots")
async def get_available_slots(req: SlotsQueryRequest, request: Request) -> dict[str, Any]:
    """R-CAL-01: Retrieves actual configured available slots from Calnode (authz + fail-closed enforced)."""
    modes = request.app.state.modes
    await modes.authorize_read_operation(
        req.actor,
        req.conversationId,
        "calnode:list_slots",
        subject_line_display=req.subjectLineDisplay,
    )
    calnode = request.app.state.adapters[EngineName.CALNODE]
    result = await calnode.list_slots(
        calendar_id=req.calendarId,
        date_from=req.dateFrom,
        date_to=req.dateTo,
    )
    return {
        "conversationId": req.conversationId,
        "engine": "calnode",
        "slots": result.get("slots", []),
        "raw": result,
    }


@router.post("/sales/book")
async def create_booking(req: BookingRequest, request: Request) -> dict[str, Any]:
    """R-CAL-02, R-CAL-03: Books one real test appointment via Gate -> Calnode (idempotent)."""
    dispatcher = request.app.state.dispatcher
    submission = ActionSubmission(
        conversationId=req.conversationId,
        subjectLineDisplay=req.subjectLineDisplay,
        engine=EngineName.CALNODE,
        operation="create_booking",
        idempotencyKey=req.idempotencyKey,
        payload={
            "calendarId": req.calendarId or request.app.state.config.calnode_calendar_id,
            "slotId": req.slotId,
            "attendeeEmail": req.attendeeEmail,
            "attendeeName": req.attendeeName,
            "notes": req.notes,
        },
        recipients=[req.attendeeEmail],
        actor=req.actor,
        correlationId=req.correlationId,
    )
    return await dispatcher.submit_action(submission)


@router.post("/operations/assign")
async def start_assignment(req: AssignmentRequest, request: Request) -> dict[str, Any]:
    """R-OPS-01, R-OPS-02: Starts real Windmill operational assignment through the Gate."""
    dispatcher = request.app.state.dispatcher
    submission = ActionSubmission(
        conversationId=req.conversationId,
        subjectLineDisplay=req.subjectLineDisplay,
        engine=EngineName.WINDMILL,
        operation="start_assignment",
        idempotencyKey=req.idempotencyKey,
        payload={
            "conversationId": req.conversationId,
            "bookingId": req.bookingId,
            "assignee": req.assignee,
            "taskSummary": req.taskSummary,
        },
        recipients=[req.assignee],
        actor=req.actor,
        correlationId=req.correlationId,
    )
    return await dispatcher.submit_action(submission)


@router.post("/operations/job-status")
async def check_job_status(req: JobStatusRequest, request: Request) -> dict[str, Any]:
    """R-OPS-02: Reports Windmill job ID, state, assignee, completion result, and retry/error status."""
    modes = request.app.state.modes
    await modes.authorize_read_operation(
        req.actor,
        req.conversationId,
        "windmill:get_job_status",
        subject_line_display=req.subjectLineDisplay,
    )
    windmill = request.app.state.adapters[EngineName.WINDMILL]
    status = await windmill.get_job_status(req.jobId)
    return {
        "conversationId": req.conversationId,
        "engine": "windmill",
        "job": status,
    }
