"""Windmill Operations Assignment Adapter (R-OPS-01, R-OPS-02, A05).

Verified against live Windmill CE v1.425.0 (ghcr.io/windmill-labs/windmill:1.425.0):
- V-04 Trigger: POST /api/w/{workspace}/jobs/run/p/{script_path} -> returns job UUID
- V-04 Status:  GET  /api/w/{workspace}/jobs_u/get/{job_id} -> returns CompletedJob / QueuedJob JSON
"""

from __future__ import annotations

import asyncio
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
            raw_job_id = await self._request_text(
                "POST",
                f"/api/w/{ws}/jobs/run/p/{script_path}",
                json_body=job_payload,
                idempotency_key=idempotency_key,
            )
            job_id = raw_job_id.strip().strip('"')

            # Poll briefly until worker completes the job so SafiChat receives jobId + state + assignee + result
            job_data: dict[str, Any] = {"id": job_id, "type": "QueuedJob"}
            for _ in range(30):
                job_data = await self.get_job_status(job_id)
                if job_data.get("type") == "CompletedJob":
                    break
                await asyncio.sleep(0.25)

            result_obj = job_data.get("result") or {}
            success = bool(job_data.get("success", False))
            if job_data.get("type") == "CompletedJob" and not success:
                raise DownstreamError(
                    "windmill",
                    f"Windmill job {job_id} failed: {result_obj}",
                    {"jobId": job_id, "result": result_obj},
                )

            return {
                "jobId": job_id,
                "state": "COMPLETED" if success else job_data.get("type", "RUNNING"),
                "assignee": (
                    result_obj.get("assignee")
                    if isinstance(result_obj, dict)
                    else payload["assignee"]
                ),
                "result": result_obj,
                "durationMs": job_data.get("duration_ms"),
                "retryCount": result_obj.get("retryCount", 0) if isinstance(result_obj, dict) else 0,
                "error": None if success else str(result_obj),
            }

        if operation == "get_job_status":
            return await self.get_job_status(payload["jobId"])

        raise DownstreamError("windmill", f"Unsupported Windmill operation '{operation}'")

    async def get_job_status(self, job_id: str) -> dict[str, Any]:
        ws = self.config.windmill_workspace
        data = await self._request_json("GET", f"/api/w/{ws}/jobs_u/get/{job_id}")
        result_obj = data.get("result") or {}
        return {
            **data,
            "jobId": data.get("id", job_id),
            "state": (
                "COMPLETED"
                if data.get("type") == "CompletedJob" and data.get("success")
                else ("FAILED" if data.get("type") == "CompletedJob" else data.get("type", "QUEUED"))
            ),
            "assignee": (
                result_obj.get("assignee") if isinstance(result_obj, dict) else None
            ),
        }

    async def _request_text(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> str:
        resp = await self._send(method, path, json_body, idempotency_key)
        return resp.text

    async def _request_json(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        resp = await self._send(method, path, json_body, idempotency_key)
        return resp.json()

    async def _send(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        idempotency_key: str = "",
    ) -> httpx.Response:
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
        return resp
