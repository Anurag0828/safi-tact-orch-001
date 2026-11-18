"""JetStream History & Attribution Publisher (R-CTX-01, R-CTX-02, R-CTX-03, A11)."""

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


def build_validated_control_event(
    conversation_id: str,
    actor: ActorContext,
    event_type: ControlEventType,
    operation_id: str,
    idempotency_key: str,
    outcome: Outcome,
    subject_line_display: str = "",
    correlation_id: str | None = None,
) -> tuple[ControlEvent, dict[str, Any]]:
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
    payload = event.to_schema_dict()
    return event, payload


class HistoryPublisher(ABC):
    async def ensure_available(self) -> None:
        """Verifies audit history backend is reachable before executing a downstream side effect."""
        return None

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
    """In-memory sink for isolated unit tests only."""

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
        event, _ = build_validated_control_event(
            conversation_id=conversation_id,
            actor=actor,
            event_type=event_type,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
            outcome=outcome,
            subject_line_display=subject_line_display,
            correlation_id=correlation_id,
        )
        self.events.append(event)
        logger.info(
            "Recorded ControlEvent (in-memory test sink): %s (conv=%s, type=%s, outcome=%s)",
            event.eventId,
            conversation_id,
            event_type.value,
            outcome.value,
        )
        return event


class JetStreamHistoryPublisher(HistoryPublisher):
    """Publishes schema-validated ControlEvents directly to SAFI JetStream.

    - Never auto-mutates NATS streams unless `JETSTREAM_AUTO_CREATE_STREAM=true` is explicitly set.
    - Never retains an unbounded in-memory duplicate copy of history when JetStream is connected (R-CTX-03).
    - Fails closed (raises StoreError) when `JETSTREAM_REQUIRED=true` and JetStream is unavailable.
    """

    def __init__(self, config: GateConfig) -> None:
        self.config = config
        self._nc: Any = None
        self._js: Any = None
        self.connected: bool = False
        # Used ONLY in non-production local mode when JETSTREAM_REQUIRED=false and JetStream is offline
        self.fallback: InMemoryHistoryPublisher | None = (
            None if config.jetstream_required else InMemoryHistoryPublisher()
        )

    async def connect(self, max_retries: int = 3, retry_delay_s: float = 0.5) -> None:
        """Connects to NATS JetStream with strict fail-closed timeout & retry budget (D1).

        Uses allow_reconnect=False and max_reconnect_attempts=0 on initial connect wrapped in
        asyncio.wait_for so unreachable/blackholed NATS fails fast within <= 15s total budget.
        """
        import asyncio
        import time

        per_attempt_cap = max(0.5, float(self.config.nats_connect_timeout_seconds))
        total_budget = min(max(per_attempt_cap, float(self.config.nats_connect_total_budget_seconds)), 15.0)
        attempts = max(1, max_retries) if self.config.jetstream_required else 1
        deadline = time.monotonic() + total_budget

        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                last_exc = TimeoutError(f"Exceeded total NATS connect budget ({total_budget:.1f}s)")
                break

            attempt_timeout = min(per_attempt_cap, remaining)
            try:
                import nats

                opts: dict[str, Any] = {
                    "servers": [self.config.nats_url],
                    "allow_reconnect": False,
                    "max_reconnect_attempts": 0,
                    "connect_timeout": attempt_timeout,
                }
                if self.config.nats_token:
                    opts["token"] = self.config.nats_token
                elif self.config.nats_user and self.config.nats_password:
                    opts["user"] = self.config.nats_user
                    opts["password"] = self.config.nats_password

                self._nc = await asyncio.wait_for(
                    nats.connect(**opts),
                    timeout=attempt_timeout + 0.5,
                )
                self._js = self._nc.jetstream()

                try:
                    await asyncio.wait_for(
                        self._js.stream_info(self.config.jetstream_stream),
                        timeout=attempt_timeout,
                    )
                except Exception as stream_exc:
                    if not self.config.jetstream_auto_create_stream:
                        raise StoreError(
                            f"JetStream stream '{self.config.jetstream_stream}' not found or unreachable "
                            f"(JETSTREAM_AUTO_CREATE_STREAM=false; refusing to mutate NATS): {stream_exc}"
                        ) from stream_exc
                    subject_wildcard = f"{self.config.jetstream_control_subject_prefix}.>"
                    await asyncio.wait_for(
                        self._js.add_stream(
                            name=self.config.jetstream_stream,
                            subjects=[subject_wildcard],
                        ),
                        timeout=attempt_timeout,
                    )
                    logger.info(
                        "Created local JetStream stream '%s' for '%s' (JETSTREAM_AUTO_CREATE_STREAM=true)",
                        self.config.jetstream_stream,
                        subject_wildcard,
                    )

                self.connected = True
                logger.info(
                    "Connected to NATS JetStream at %s (stream=%s)",
                    self.config.nats_url,
                    self.config.jetstream_stream,
                )
                return
            except Exception as exc:
                last_exc = exc
                self.connected = False
                self._js = None
                if self._nc is not None:
                    try:
                        await asyncio.wait_for(self._nc.close(), timeout=1.0)
                    except Exception:
                        pass
                    self._nc = None
                if attempt < attempts and self.config.jetstream_required:
                    sleep_for = min(retry_delay_s, max(0.0, deadline - time.monotonic()))
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)

        if self.config.jetstream_required:
            raise StoreError(
                f"JetStream is required (JETSTREAM_REQUIRED=true) but unavailable at {self.config.nats_url}: {last_exc}"
            ) from last_exc
        logger.warning(
            "WARNING: Could not connect to NATS JetStream (%s). Falling back to local in-memory publisher (JETSTREAM_REQUIRED=false).",
            last_exc,
        )

    async def _drop_connection(self) -> None:
        import asyncio

        self.connected = False
        self._js = None
        if self._nc is not None:
            try:
                await asyncio.wait_for(self._nc.close(), timeout=1.0)
            except Exception:
                pass
            self._nc = None

    async def ensure_available(self) -> None:
        """Fail-closed liveness check before dispatching external side effects (D1 / A10.3)."""
        import asyncio

        if not self.config.jetstream_required:
            return

        ping_timeout = min(max(0.5, float(self.config.nats_connect_timeout_seconds)), 2.5)
        if self._nc is not None and self._js is not None and self._nc.is_connected:
            try:
                await asyncio.wait_for(self._nc.flush(timeout=ping_timeout), timeout=ping_timeout + 0.5)
                return
            except Exception as exc:
                logger.warning("NATS liveness check failed mid-run (%s); attempting single bounded reconnect", exc)
                await self._drop_connection()

        # Single bounded reconnect attempt so recovery works after NATS returns (A10.3), while failing fast if down
        await self.connect(max_retries=1, retry_delay_s=0.0)

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
        import asyncio

        event, payload = build_validated_control_event(
            conversation_id=conversation_id,
            actor=actor,
            event_type=event_type,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
            outcome=outcome,
            subject_line_display=subject_line_display,
            correlation_id=correlation_id,
        )

        if self._js is None or self._nc is None or not self._nc.is_connected:
            if self.config.jetstream_required:
                # Attempt single fast reconnect in case NATS just recovered (A10.3)
                await self.connect(max_retries=1, retry_delay_s=0.0)
            else:
                if self.fallback is not None:
                    self.fallback.events.append(event)
                return event

        conv_token = sanitize_subject_token(conversation_id)
        type_token = sanitize_subject_token(event_type.value.lower())
        subject = f"{self.config.jetstream_control_subject_prefix}.{conv_token}.{type_token}"
        payload_bytes = json.dumps(payload).encode("utf-8")
        pub_timeout = max(0.5, float(self.config.nats_connect_timeout_seconds))
        try:
            await asyncio.wait_for(
                self._js.publish(subject, payload_bytes, timeout=pub_timeout),
                timeout=pub_timeout + 1.0,
            )
        except Exception as exc:
            await self._drop_connection()
            if self.config.jetstream_required:
                raise StoreError(
                    f"Failed to publish ControlEvent to JetStream subject '{subject}': {exc}"
                ) from exc
            logger.error("JetStream publish failed for '%s': %s", subject, exc)

        logger.info(
            "Published ControlEvent to JetStream: %s (subject=%s, outcome=%s)",
            event.eventId,
            subject,
            outcome.value,
        )
        return event

    async def close(self) -> None:
        import asyncio

        if self._nc:
            try:
                if self._nc.is_connected:
                    await asyncio.wait_for(self._nc.drain(), timeout=2.0)
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._nc.close(), timeout=1.0)
            except Exception:
                pass
            self._nc = None
            self._js = None
            self.connected = False
