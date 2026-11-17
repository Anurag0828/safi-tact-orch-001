"""n8n Marketing Engine Adapter (R-MKT-01, R-MKT-02, A03).

NOTE: All controlled outbound effects (e.g. follow-up dispatch) execute ONLY
when invoked through the Gate Adapter. n8n workflows MUST NOT use direct
outbound send nodes; instead they prepare payloads and submit to the Gate.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import httpx

from adapters.base import EngineAdapter
from gate.config import GateConfig
from gate.errors import DownstreamError, DownstreamOutcomeUnknownError
from gate.models import EngineName


class N8nAdapter(EngineAdapter):
    engine_name = EngineName.N8N

    def __init__(
        self,
        config: GateConfig,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._client = http_client

    def _validate_resume_url(self, resume_url: str) -> str:
        """Prevents SSRF & credential leakage by requiring resumeUrl to match n8n_base_url origin."""
        base = urlparse(self.config.n8n_base_url)
        target = urlparse(resume_url)
        if target.scheme not in ("http", "https") or target.netloc != base.netloc or target.scheme != base.scheme:
            raise DownstreamError(
                "n8n",
                "Rejected resumeUrl: origin does not match configured n8n_base_url",
                {"resumeUrl": resume_url},
            )
        return resume_url

    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        if operation == "ingest_lead":
            return await self._post(
                path="/webhook/safi-lead-intake",
                body={**payload, "idempotencyKey": idempotency_key},
                idempotency_key=idempotency_key,
            )
        if operation == "dispatch_followup":
            # Resumes a paused n8n Wait node OR executes the approved follow-up webhook
            resume_url = payload.get("resumeUrl")
            if resume_url:
                safe_url = self._validate_resume_url(resume_url)
                return await self._post(
                    path=safe_url,
                    body={**payload, "idempotencyKey": idempotency_key, "gateApproved": True},
                    idempotency_key=idempotency_key,
                    absolute_url=True,
                )
            return await self._post(
                path="/webhook/safi-approved-followup",
                body={**payload, "idempotencyKey": idempotency_key, "gateApproved": True},
                idempotency_key=idempotency_key,
            )
        raise DownstreamError("n8n", f"Unsupported n8n operation '{operation}'")

    async def _post(
        self,
        path: str,
        body: dict[str, Any],
        idempotency_key: str,
        absolute_url: bool = False,
    ) -> dict[str, Any]:
        url = path if absolute_url else f"{self.config.n8n_base_url}{path}"
        headers = {
            "X-Idempotency-Key": idempotency_key,
            "Content-Type": "application/json",
        }
        # Least-privilege (SOW §9): authenticate webhooks ONLY with X-SAFI-Webhook-Secret, never admin X-N8N-API-KEY
        if self.config.n8n_webhook_secret:
            headers["X-SAFI-Webhook-Secret"] = self.config.n8n_webhook_secret

        try:
            if self._client is not None:
                resp = await self._client.post(url, json=body, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=self.config.http_timeout_seconds) as client:
                    resp = await client.post(url, json=body, headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise DownstreamOutcomeUnknownError("n8n", idempotency_key, str(exc)) from exc

        if resp.status_code >= 400:
            raise DownstreamError(
                "n8n",
                f"HTTP {resp.status_code}: {resp.text}",
                {"statusCode": resp.status_code},
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise DownstreamError(
                "n8n",
                f"Invalid JSON response from n8n (HTTP {resp.status_code}): {exc}",
                {"statusCode": resp.status_code},
            ) from exc
