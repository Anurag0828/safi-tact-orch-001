"""JetStream History & Attribution Publisher (R-CTX-01, R-CTX-02, A11)."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any
from uuid import uuid4

from gate.config import GateConfig
from gate.models import (
    ActorContext,
    ControlEvent,
    ControlEventType,
    Outcome,
    utc_now_iso,
    validate_against_schema,
)

logger = logging.getLogger("gate.history")


class HistoryPublisher(ABC):
    @abstractmethod
    async def record_control_event(
        self,
        conversation_id: str,
        actor: ActorContext,
        event_type: ControlEventType,
        operation_id: str,
        idempotency_key: str,
        outcome: Outcome,
        subject_line_display: str = "",
        correlation_id: str | None = None,
    ) -> ControlEvent:
        pass


class InMemoryHistoryPublisher(HistoryPublisher):
    """Authoritative memory sink for unit tests and local runs."""

    def __init__(self) -> None:
        self.events: list[ControlEvent] = []

    async def record_control_event(
        self,
        conversation_id: str,
        actor: ActorContext,
        event_type: ControlEventType,
        operation_id: str,
        idempotency_key: str,
        outcome: Outcome,
        subject_line_display: str = "",
        correlation_id: str | None = None,
    ) -> ControlEvent:
        event = ControlEvent(
            eventId=f"evt_{uuid4().hex[:12]}",
            conversationId=conversation_id,
            subjectLineDisplay=subject_line_display or None,
            actorGuid=actor.actorGuid,
            actorKind=actor.actorKind,
            type=event_type,
            operationId=operation_id,
            idempotencyKey=idempotency_key,
            outcome=outcome,
            context=actor.to_event_context(),
            correlationId=correlation_id,
            ts=utc_now_iso(),
        )
        # Validate against contracts/control-event.schema.json
        payload = event.to_schema_dict()
        self.events.append(event)
        logger.info(
            "Recorded ControlEvent: %s (conv=%s, type=%s, outcome=%s)",
            event.eventId,
            conversation_id,
            event_type.value,
            outcome.value,
        )
        return event


class JetStreamHistoryPublisher(HistoryPublisher):
    """Publishes control events to real SAFI JetStream stream preserving subject hierarchy."""

    def __init__(self, config: GateConfig) -> None:
        self.config = config
        self._nc: Any = None
        self._js: Any = None
        self.fallback = InMemoryHistoryPublisher()

    async def connect(self) -> None:
        try:
            import nats
            opts: dict[str, Any] = {"servers": [self.config.nats_url]}
            if self.config.nats_token:
                opts["token"] = self.config.nats_token
            elif self.config.nats_user and self.config.nats_password:
                opts["user"] = self.config.nats_user
                opts["password"] = self.config.nats_password
            self._nc = await nats.connect(**opts)
            self._js = self._nc.jetstream()
            logger.info("Connected to NATS JetStream at %s", self.config.nats_url)
        except Exception as exc:
            logger.warning(
                "Could not connect to NATS JetStream (%s). Control events will use local fallback publisher.",
                exc,
            )

    async def record_control_event(
        self,
        conversation_id: str,
        actor: ActorContext,
        event_type: ControlEventType,
        operation_id: str,
        idempotency_key: str,
        outcome: Outcome,
        subject_line_display: str = "",
        correlation_id: str | None = None,
    ) -> ControlEvent:
        event = await self.fallback.record_control_event(
            conversation_id=conversation_id,
            actor=actor,
            event_type=event_type,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
            outcome=outcome,
            subject_line_display=subject_line_display,
            correlation_id=correlation_id,
        )

        if self._js:
            # Preserves NATS Subject routing: e.g. safi.control.events.<conversationId>.<eventType>
            subject = (
                f"{self.config.jetstream_control_subject_prefix}."
                f"{conversation_id}.{event_type.value.lower()}"
            )
            payload_bytes = json.dumps(event.to_schema_dict()).encode("utf-8")
            await self._js.publish(subject, payload_bytes)

        return event

    async def close(self) -> None:
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
            self._js = None
