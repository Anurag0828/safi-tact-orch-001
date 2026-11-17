"""JetStream History & Attribution Publisher (R-CTX-01, R-CTX-02, A11)."""

from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Any
from uuid import uuid4

from gate.config import GateConfig
from gate.errors import StoreError
from gate.models import (
    ActorContext,
    ControlEvent,
    ControlEventType,
    Outcome,
    utc_now_iso,
)

logger = logging.getLogger("gate.history")

_SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9_-]")


def sanitize_subject_token(token: str) -> str:
    """Ensures a NATS subject token contains no wildcards, dots, or whitespace."""
    cleaned = _SAFE_TOKEN_RE.sub("_", token.strip())
    if not cleaned:
        raise StoreError("Invalid empty NATS subject token")
    return cleaned


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
    """In-memory sink for isolated unit tests."""

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
        # Strict validation against contracts/control-event.schema.json
        event.to_schema_dict()
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
    """Publishes schema-validated ControlEvents to SAFI JetStream preserving subject hierarchy.

    When `config.jetstream_required` is True (container/production default), connection or
    publish failure fails closed (raises StoreError) rather than silently falling back to memory.
    """

    def __init__(self, config: GateConfig) -> None:
        self.config = config
        self._nc: Any = None
        self._js: Any = None
        self.connected: bool = False
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

            # Verify target JetStream stream exists (create only if using local standalone profile stream)
            try:
                await self._js.stream_info(self.config.jetstream_stream)
            except Exception:
                subject_wildcard = f"{self.config.jetstream_control_subject_prefix}.>"
                await self._js.add_stream(
                    name=self.config.jetstream_stream,
                    subjects=[subject_wildcard],
                )
                logger.info(
                    "Created local JetStream stream '%s' for '%s'",
                    self.config.jetstream_stream,
                    subject_wildcard,
                )

            self.connected = True
            logger.info(
                "Connected to NATS JetStream at %s (stream=%s)",
                self.config.nats_url,
                self.config.jetstream_stream,
            )
        except Exception as exc:
            self.connected = False
            self._js = None
            if self.config.jetstream_required:
                raise StoreError(
                    f"JetStream is required (JETSTREAM_REQUIRED=true) but unavailable at {self.config.nats_url}: {exc}"
                ) from exc
            logger.warning(
                "Could not connect to NATS JetStream (%s). Falling back to in-memory publisher (non-production mode).",
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

        if self._js is None:
            if self.config.jetstream_required:
                raise StoreError(
                    "JetStream publisher is disconnected; refusing to proceed without authoritative audit event"
                )
            return event

        conv_token = sanitize_subject_token(conversation_id)
        type_token = sanitize_subject_token(event_type.value.lower())
        subject = f"{self.config.jetstream_control_subject_prefix}.{conv_token}.{type_token}"
        payload_bytes = json.dumps(event.to_schema_dict()).encode("utf-8")
        try:
            await self._js.publish(subject, payload_bytes)
        except Exception as exc:
            if self.config.jetstream_required:
                raise StoreError(
                    f"Failed to publish ControlEvent to JetStream subject '{subject}': {exc}"
                ) from exc
            logger.error("JetStream publish failed for '%s': %s", subject, exc)

        return event

    async def close(self) -> None:
        if self._nc:
            await self._nc.drain()
            await self._nc.close()
            self._nc = None
            self._js = None
            self.connected = False
