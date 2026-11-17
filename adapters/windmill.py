"""Windmill Operations Assignment Adapter (R-OPS-01, R-OPS-02, A05).

UNVERIFIED API PATHS: Verify V-04 against live Windmill container in T-02.
"""

from __future__ import annotations

from typing import Any

import httpx

from adapters.base import EngineAdapter
from gate.config import GateConfig
from gate.errors import DownstreamError, DownstreamOutcomeUnknownError
from gate.models import EngineName


class WindmillAdapter(EngineAdapter):
    engine_name = EngineName.WINDMILL

    def __init__(
        self,
        config: GateConfig,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._client = http_client

    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        ws = self.config.windmill_workspace
        if operation == "start_assignment":
            script_path = payload.get("scriptPath", "f/safi/ops_task_assignment")
            job_payload = {
                "bookingId": payload["bookingId"],
                "conversationId": payload["conversationId"],
                "assignee": payload["assignee"],
                "taskSummary": payload.get("taskSummary", ""),
                "idempotencyKey": idempotency_key,
            }
            return await self._request(
                "POST",
                f"/api/w/{ws}/jobs/run_wait_result/p/{script_path}",
                json_body=job_payload,
                idempotency_key=idempotency_key,
            )
        if operation == "get_job_status":
            job_id = payload["jobId"]
            return await self._request(
                "GET",
                f"/api/w/{ws}/jobs_u/get/{job_id}",
                idempotency_key=idempotency_key,
            )
        raise DownstreamError("windmill", f"Unsupported Windmill operation '{operation}'")

    async def get_job_status(self, job_id: str) -> dict[str, Any]:
        ws = self.config.windmill_workspace
        return await self._request("GET", f"/api/w/{ws}/jobs_u/get/{job_id}")

    async def _request(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        url = f"{self.config.windmill_base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if self.config.windmill_token:
            headers["Authorization"] = f"Bearer {self.config.windmill_token}"
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        try:
            if self._client is not None:
                resp = await self._client.request(
                    method, url, json=json_body, headers=headers
                )
            else:
                async with httpx.AsyncClient(timeout=self.config.http_timeout_seconds) as client:
                    resp = await client.request(
                        method, url, json=json_body, headers=headers
                    )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise DownstreamOutcomeUnknownError("windmill", idempotency_key, str(exc)) from exc

        if resp.status_code >= 400:
            raise DownstreamError(
                "windmill",
                f"HTTP {resp.status_code}: {resp.text}",
                {"statusCode": resp.status_code},
            )
        return resp.json()
