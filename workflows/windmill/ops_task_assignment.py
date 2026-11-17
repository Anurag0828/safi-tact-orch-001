"""Windmill Operations Task Assignment Script (R-OPS-01, R-OPS-02, A05).

Deployed to Windmill path: f/safi/ops_task_assignment
NOTE (R-GATE-01): This script does NOT send direct external notifications.
Any customer/assignee outbound notification must be submitted back through
the Local Gate Adapter (/gate/dispatch).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def main(
    bookingId: str,
    conversationId: str,
    assignee: str,
    taskSummary: str = "",
    idempotencyKey: str = "",
) -> dict[str, Any]:
    if not bookingId or not conversationId or not assignee:
        raise ValueError("bookingId, conversationId, and assignee are required")

    # Deterministic assignment ID from idempotencyKey/bookingId so retries never duplicate
    seed = idempotencyKey or f"{conversationId}:{bookingId}:{assignee}"
    assignment_id = "asgn_" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:12]

    return {
        "assignmentId": assignment_id,
        "bookingId": bookingId,
        "conversationId": conversationId,
        "assignee": assignee,
        "taskSummary": taskSummary or f"Deliver service for booking {bookingId}",
        "state": "COMPLETED",
        "retryCount": 0,
        "error": None,
        "completedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
