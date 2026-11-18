"""FastAPI application wiring the ONE Local Gate Adapter and SafiChat operations."""

from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from adapters.base import EngineAdapter
from adapters.calnode import CalnodeAdapter
from adapters.n8n import N8nAdapter
from adapters.windmill import WindmillAdapter
from gate.authz import HttpPermissionProvider, PermissionProvider
from gate.config import GateConfig
from gate.dispatch import GateDispatcher
from gate.errors import GateError, IdentityUncertainError, UnauthorizedError
from gate.history import HistoryPublisher, JetStreamHistoryPublisher
from gate.models import (
    ActionSubmission,
    ActorContext,
    ActorKind,
    ControlEventType,
    EngineName,
    Outcome,
)
from gate.modes import GateModeController
from gate.state_machine import ConversationRuntimeRegistry
from gate.store import GateStore
from safichat.ops.gate_ops import router as gate_ops_router
from safichat.ops.router import router as business_ops_router


def _verify_safichat_caller(request: Request) -> None:
    """Authenticates SafiChat callers via Bearer token when GATE_REQUIRE_AUTH or GATE_API_TOKEN is set (R-SEC-02/03)."""
    cfg: GateConfig = request.app.state.config
    if not cfg.gate_require_auth and not cfg.gate_api_token:
        return
    if not cfg.gate_api_token:
        raise IdentityUncertainError(
            "GATE_REQUIRE_AUTH is enabled but GATE_API_TOKEN is not configured; failing closed"
        )
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise UnauthorizedError("Missing Bearer token for SafiChat Gate API")
    provided = auth_header[7:].strip()
    if not hmac.compare_digest(provided, cfg.gate_api_token):
        raise UnauthorizedError("Invalid Bearer token for SafiChat Gate API")


def _verify_internal_dispatch_caller(request: Request) -> None:
    """Authenticates internal engine callbacks (/gate/dispatch) via shared secret (R-SEC-02/03)."""
    cfg: GateConfig = request.app.state.config
    if not cfg.gate_require_auth and not cfg.n8n_webhook_secret:
        return
    if not cfg.n8n_webhook_secret:
        raise IdentityUncertainError(
            "GATE_REQUIRE_AUTH is enabled but N8N_WEBHOOK_SECRET is not configured; failing closed"
        )
    provided = request.headers.get("X-SAFI-Gate-Secret", "").strip()
    if not provided or not hmac.compare_digest(provided, cfg.n8n_webhook_secret):
        raise UnauthorizedError("Missing or invalid X-SAFI-Gate-Secret for /gate/dispatch")


def create_app(
    config: GateConfig | None = None,
    store: GateStore | None = None,
    history: HistoryPublisher | None = None,
    authz: PermissionProvider | None = None,
    adapters: dict[EngineName, EngineAdapter] | None = None,
) -> FastAPI:
    cfg = config or GateConfig.from_env()
    gate_store = store or GateStore(cfg.store_path)
    gate_history = history or JetStreamHistoryPublisher(cfg)
    gate_authz = authz or HttpPermissionProvider(cfg)
    runtimes = ConversationRuntimeRegistry()

    engine_adapters: dict[EngineName, EngineAdapter] = adapters or {
        EngineName.N8N: N8nAdapter(cfg),
        EngineName.CALNODE: CalnodeAdapter(cfg),
        EngineName.WINDMILL: WindmillAdapter(cfg),
    }

    dispatcher = GateDispatcher(
        config=cfg,
        store=gate_store,
        history=gate_history,
        authz=gate_authz,
        runtimes=runtimes,
        adapters=engine_adapters,
    )
    modes = GateModeController(
        config=cfg,
        store=gate_store,
        history=gate_history,
        authz=gate_authz,
        runtimes=runtimes,
        dispatcher=dispatcher,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await gate_store.initialize()
        try:
            # Restore persisted per-conversation ownership into local ACL provider across restarts (A09/A10)
            persisted_owners = await gate_store.list_conversation_owners()
            local_acl = (
                gate_authz.local_policy
                if isinstance(gate_authz, HttpPermissionProvider)
                else gate_authz
            )
            if hasattr(local_acl, "authorize_actor_for_conversation"):
                for cid, owner_guid in persisted_owners.items():
                    local_acl.authorize_actor_for_conversation(cid, owner_guid)

            if isinstance(gate_history, JetStreamHistoryPublisher):
                await gate_history.connect()
            recovery = await gate_store.recover_on_startup()
            app.state.recovery_summary = recovery

            # Emit authoritative GATE_FAILED_CLOSED events to JetStream for any crash-interrupted conversations (A10/A11)
            system_actor = ActorContext(
                actorGuid="system-gate-recovery",
                actorKind=ActorKind.PROXY,
                meetingContext={"source": "startup-recovery"},
                portContext={"port": "local-gate-adapter"},
                permissionProvenance={"role": "SYSTEM_RECOVERY"},
            )
            for item in recovery.get("failedClosedDetails", []):
                await gate_history.record_control_event(
                    conversation_id=item["conversationId"],
                    actor=system_actor,
                    event_type=ControlEventType.GATE_FAILED_CLOSED,
                    operation_id="startup:recover_on_startup",
                    idempotency_key=f"recovery:{item['conversationId']}",
                    outcome=Outcome.UNKNOWN,
                    subject_line_display=item.get("subjectLine", ""),
                )

            yield
        finally:
            if isinstance(gate_history, JetStreamHistoryPublisher):
                await gate_history.close()
            await gate_store.close()

    app = FastAPI(
        title="SAFI Local Gate Adapter",
        version="1.1.0",
        lifespan=lifespan,
    )

    app.state.config = cfg
    app.state.store = gate_store
    app.state.history = gate_history
    app.state.authz = gate_authz
    app.state.runtimes = runtimes
    app.state.adapters = engine_adapters
    app.state.dispatcher = dispatcher
    app.state.modes = modes
    app.state.recovery_summary = {}

    @app.exception_handler(GateError)
    async def gate_error_handler(_request: Request, exc: GateError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        hist = request.app.state.history
        js_connected = hist.connected if isinstance(hist, JetStreamHistoryPublisher) else True
        return {
            "status": "ok",
            "service": "safi-local-gate-adapter",
            "jetstreamConnected": js_connected,
            "recovery": request.app.state.recovery_summary,
        }

    @app.post("/gate/dispatch", dependencies=[Depends(_verify_internal_dispatch_caller)])
    async def internal_gate_dispatch(
        submission: ActionSubmission, request: Request
    ) -> dict[str, Any]:
        """Single authenticated callback endpoint for n8n/Windmill/Calnode workflows (R-GATE-01)."""
        return await request.app.state.dispatcher.submit_action(submission)

    app.include_router(
        business_ops_router, dependencies=[Depends(_verify_safichat_caller)]
    )
    app.include_router(
        gate_ops_router, dependencies=[Depends(_verify_safichat_caller)]
    )
    return app


app = create_app()
