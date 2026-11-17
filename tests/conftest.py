"""Pytest fixtures and test harnesses for SAFI Local Gate Adapter."""

from __future__ import annotations

import tempfile
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from adapters.base import EngineAdapter
from gate.app import create_app
from gate.authz import LocalPolicyPermissionProvider
from gate.config import GateConfig
from gate.history import InMemoryHistoryPublisher
from gate.models import (
    ActionState,
    ActorContext,
    ActorKind,
    EngineName,
)
from gate.state_machine import ConversationRuntimeRegistry
from gate.store import GateStore


class MockEngineAdapter(EngineAdapter):
    """Configurable mock adapter for deterministic test execution."""

    def __init__(self, engine_name: EngineName) -> None:
        self.engine_name = engine_name
        self.call_log: list[dict[str, Any]] = []
        self.simulate_network_failure: bool = False
        self.simulate_downstream_error: bool = False
        self.custom_response: dict[str, Any] | None = None

    async def list_slots(
        self,
        calendar_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> dict[str, Any]:
        return await self.execute(
            "list_slots",
            {"calendarId": calendar_id, "from": date_from, "to": date_to},
            idempotency_key="read:list_slots",
        )

    async def get_job_status(self, job_id: str) -> dict[str, Any]:
        return await self.execute(
            "get_job_status",
            {"jobId": job_id},
            idempotency_key=f"read:job:{job_id}",
        )

    async def execute(
        self,
        operation: str,
        payload: dict[str, Any],
        idempotency_key: str,
    ) -> dict[str, Any]:
        self.call_log.append(
            {
                "operation": operation,
                "payload": payload,
                "idempotencyKey": idempotency_key,
            }
        )
        if self.simulate_network_failure:
            from gate.errors import DownstreamOutcomeUnknownError
            raise DownstreamOutcomeUnknownError(
                self.engine_name.value,
                idempotency_key,
                "Simulated downstream network timeout",
            )

        if self.simulate_downstream_error:
            from gate.errors import DownstreamError
            raise DownstreamError(self.engine_name.value, "Simulated downstream 500 error")

        if self.custom_response is not None:
            return self.custom_response

        # Default realistic responses
        if self.engine_name == EngineName.N8N:
            return {
                "executionId": f"n8n_exec_{uuid4().hex[:8]}",
                "status": "success",
                "dispatchedFollowup": payload.get("messageBody", ""),
            }
        if self.engine_name == EngineName.CALNODE:
            if operation == "list_slots":
                return {
                    "calendarId": payload.get("calendarId", "cal_test_bounded_01"),
                    "slots": [
                        {"slotId": "slot_1", "start": "2026-11-18T14:00:00Z", "available": True},
                        {"slotId": "slot_2", "start": "2026-11-18T15:00:00Z", "available": True},
                    ],
                }
            return {
                "bookingId": f"bkg_{uuid4().hex[:8]}",
                "slotId": payload.get("slotId", "slot_1"),
                "status": "CONFIRMED",
                "attendeeEmail": payload.get("attendeeEmail", ""),
            }
        if self.engine_name == EngineName.WINDMILL:
            return {
                "jobId": f"wm_job_{uuid4().hex[:8]}",
                "state": "COMPLETED",
                "assignee": payload.get("assignee", ""),
                "result": {"assignmentId": f"asgn_{uuid4().hex[:8]}", "status": "ASSIGNED"},
            }
        return {"status": "ok"}


@pytest.fixture
def temp_db_path(tmp_path: Any) -> str:
    return str(tmp_path / "test_gate_state.db")


@pytest.fixture
def test_config(temp_db_path: str) -> GateConfig:
    return GateConfig(
        store_path=temp_db_path,
        allowed_recipients=frozenset(
            [
                "customer-a@allowlist.test.local",
                "customer-b@allowlist.test.local",
                "customer-c@allowlist.test.local",
                "ops-specialist-1@allowlist.test.local",
                "lead@allowlist.test.local",
            ]
        ),
        calnode_calendar_id="cal_test_bounded_01",
        gate_require_auth=False,
        jetstream_required=False,
        inflight_reconcile_timeout_seconds=2.0,
    )


@pytest_asyncio.fixture
async def gate_store(test_config: GateConfig):
    store = GateStore(test_config.store_path)
    await store.initialize()
    yield store
    await store.close()


@pytest.fixture
def history_publisher() -> InMemoryHistoryPublisher:
    return InMemoryHistoryPublisher()


@pytest.fixture
def authz_provider() -> LocalPolicyPermissionProvider:
    return LocalPolicyPermissionProvider(
        bootstrap_actors={"user_test_guid_001"},
        proxy_actors={"proxy_n8n_intake"},
    )


@pytest.fixture
def mock_adapters() -> dict[EngineName, MockEngineAdapter]:
    return {
        EngineName.N8N: MockEngineAdapter(EngineName.N8N),
        EngineName.CALNODE: MockEngineAdapter(EngineName.CALNODE),
        EngineName.WINDMILL: MockEngineAdapter(EngineName.WINDMILL),
    }


@pytest.fixture
def test_actor() -> ActorContext:
    return ActorContext(
        actorGuid="user_test_guid_001",
        actorKind=ActorKind.USER,
        meetingContext={"meetingId": "mtg_99182", "room": "sales-room-a"},
        portContext={"port": "safichat-desktop"},
        permissionProvenance={"role": "BUSINESS_OWNER", "scope": "conversation:admin"},
    )


@pytest.fixture
def proxy_actor() -> ActorContext:
    return ActorContext(
        actorGuid="proxy_n8n_intake",
        actorKind=ActorKind.PROXY,
        meetingContext={"meetingId": "mtg_99182"},
        portContext={"port": "n8n-webhook"},
        permissionProvenance={
            "role": "SYSTEM_PROXY",
            "scope": "intake:write",
            "delegatedBy": "user_test_guid_001",
        },
    )


@pytest_asyncio.fixture
async def app_client(
    test_config: GateConfig,
    gate_store: GateStore,
    history_publisher: InMemoryHistoryPublisher,
    authz_provider: LocalPolicyPermissionProvider,
    mock_adapters: dict[EngineName, MockEngineAdapter],
):
    app = create_app(
        config=test_config,
        store=gate_store,
        history=history_publisher,
        authz=authz_provider,
        adapters=mock_adapters,
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app
