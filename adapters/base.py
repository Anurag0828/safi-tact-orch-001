"""Base engine adapter contract for Gate Adapter dispatch."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from gate.models import EngineName


class EngineAdapter(ABC):
    engine_name: EngineName

    @abstractmethod
    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Executes an authorized, gated operation against the downstream engine.

        Raises:
            DownstreamOutcomeUnknownError: On timeout or ambiguous network drop (Invariant 8).
            DownstreamError: On confirmed downstream error (4xx/5xx).
        """
        pass
