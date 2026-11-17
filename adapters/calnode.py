"""Calnode Sales Scheduling Adapter (R-CAL-01, R-CAL-02, R-CAL-03, R-GATE-02, A04).

UNVERIFIED API PATHS: Verify V-01/V-02/V-03 against live Calnode container in T-02.
Auto-notifications that cannot be routed through the Gate MUST be disabled (R-GATE-02).
"""

from __future__ import annotations

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

    async def list_slots(self, calendar_id: str | None = None, date_from: str | None = None, date_to: str | None = None) -> dict[str, Any]:
        """Read-only availability query (R-CAL-01)."""
        cal_id = calendar_id or self.config.calnode_calendar_id
        params: dict[str, Any] = {"calendarId": cal_id}
        if date_from:
            params["from"] = date_from
        if date_to:
            params["to"] = date_to

        return await self._request("GET", "/api/v1/slots", params=params)

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
            booking_body = {
                "calendarId": payload.get("calendarId", self.config.calnode_calendar_id),
                "slotId": payload["slotId"],
                "attendeeEmail": payload["attendeeEmail"],
                "attendeeName": payload.get("attendeeName", ""),
                "notes": payload.get("notes", ""),
                "idempotencyKey": idempotency_key,
                # R-GATE-02: suppress ungated Calnode auto-notifications
                "suppressNotifications": True,
            }
            return await self._request(
                "POST",
                "/api/v1/bookings",
                json_body=booking_body,
                idempotency_key=idempotency_key,
            )
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
        return resp.json()
