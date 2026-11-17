"""Calnode Sales Scheduling Adapter (R-CAL-01, R-CAL-02, R-CAL-03, R-GATE-02, A04).

Verified against live Calnode v0.14.0 (ghcr.io/calnode/calnode@sha256:120f1434d02c409b25f981a163cd4408db3e3f3ea571abc4c1e3e922f292af80):
- V-01: GET  /v1/event-types/{slug}/slots?from=YYYY-MM-DD&to=YYYY-MM-DD&tz=UTC
- V-02: POST /v1/bookings (Authorization: Bearer <key>, Idempotency-Key: <key>)
- V-03: Duplicate POST /v1/bookings with same Idempotency-Key returns original booking ID without double-booking.
- R-GATE-02: Event type configured with invite_delivery='none', reminders=[], SMTP unconfigured.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from adapters.base import EngineAdapter
from gate.config import GateConfig
from gate.errors import DownstreamError, DownstreamOutcomeUnknownError
from gate.models import EngineName


class CalnodeAdapter(EngineAdapter):
    engine_name = EngineName.CALNODE

    def __init__(
        self,
        config: GateConfig,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._client = http_client

    async def list_slots(
        self,
        calendar_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        """Read-only availability query against live Calnode /v1/event-types/{slug}/slots (R-CAL-01)."""
        slug = calendar_id or self.config.calnode_calendar_id
        today = datetime.now(timezone.utc).date()
        params: dict[str, Any] = {
            "from": date_from or today.isoformat(),
            "to": date_to or (today + timedelta(days=5)).isoformat(),
            "tz": "UTC",
        }
        data = await self._request(
            "GET", f"/v1/event-types/{slug}/slots", params=params
        )
        # Normalize slotId to slot start timestamp for uniform SafiChat UX
        raw_slots = data.get("slots", [])
        normalized = []
        for s in raw_slots:
            if isinstance(s, dict):
                normalized.append({**s, "slotId": s.get("slotId") or s.get("start")})
        return {**data, "eventTypeSlug": slug, "slots": normalized}

    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        if operation == "list_slots":
            return await self.list_slots(
                calendar_id=payload.get("calendarId"),
                date_from=payload.get("from"),
                date_to=payload.get("to"),
            )
        if operation == "create_booking":
            slot_id = payload.get("slotId")
            attendee_email = payload.get("attendeeEmail")
            if not slot_id or not attendee_email:
                raise DownstreamError(
                    "calnode",
                    "Missing required booking fields: 'slotId' and 'attendeeEmail' are required",
                    {"missingFields": [f for f in ("slotId", "attendeeEmail") if not payload.get(f)]},
                )
            slug = payload.get("calendarId") or self.config.calnode_calendar_id
            booking_body = {
                "event_type_slug": slug,
                "start_at": slot_id,
                "name": payload.get("attendeeName") or "SAFI Pilot Guest",
                "email": attendee_email,
                "timezone": payload.get("timezone", "UTC"),
                "notes": payload.get("notes", ""),
            }
            data = await self._request(
                "POST",
                "/v1/bookings",
                json_body=booking_body,
                idempotency_key=idempotency_key,
            )
            return {
                "bookingId": data.get("id") or data.get("bookingId"),
                "status": data.get("status", "confirmed"),
                "startAt": data.get("start_at"),
                "endAt": data.get("end_at"),
                "inviteDelivery": data.get("invite_delivery", "none"),
                "raw": data,
            }
        raise DownstreamError("calnode", f"Unsupported Calnode operation '{operation}'")

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        url = f"{self.config.calnode_base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if self.config.calnode_api_key:
            headers["Authorization"] = f"Bearer {self.config.calnode_api_key}"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key

        try:
            if self._client is not None:
                resp = await self._client.request(
                    method, url, params=params, json=json_body, headers=headers
                )
            else:
                async with httpx.AsyncClient(timeout=self.config.http_timeout_seconds) as client:
                    resp = await client.request(
                        method, url, params=params, json=json_body, headers=headers
                    )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise DownstreamOutcomeUnknownError("calnode", idempotency_key, str(exc)) from exc

        if resp.status_code >= 400:
            raise DownstreamError(
                "calnode",
                f"HTTP {resp.status_code}: {resp.text}",
                {"statusCode": resp.status_code},
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise DownstreamError(
                "calnode",
                f"Invalid JSON response from Calnode (HTTP {resp.status_code}): {exc}",
                {"statusCode": resp.status_code},
            ) from exc
