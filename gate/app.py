"""FastAPI application wiring the ONE Local Gate Adapter and SafiChat operations."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from adapters.base import EngineAdapter
from adapters.calnode import CalnodeAdapter
from adapters.n8n import N8nAdapter
from adapters.windmill import WindmillAdapter
from gate.authz import HttpPermissionProvider, PermissionProvider
from gate.config import GateConfig
from gate.dispatch import GateDispatcher
from gate.errors import GateError
from gate.history import HistoryPublisher, JetStreamHistoryPublisher
from gate.models import ActionSubmission, EngineName
from gate.modes import GateModeController
from gate.state_machine import ConversationRuntimeRegistry
from gate.store import GateStore
from safichat.ops.gate_ops import router as gate_ops_router
from safichat.ops.router import router as business_ops_router


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
        recovery = await gate_store.recover_on_startup()
        app.state.recovery_summary = recovery
        if isinstance(gate_history, JetStreamHistoryPublisher):
            await gate_history.connect()
        yield
        if isinstance(gate_history, JetStreamHistoryPublisher):
            await gate_history.close()
        await gate_store.close()

    app = FastAPI(
        title="SAFI Local Gate Adapter",
        version="1.0.0",
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
        return {
            "status": "ok",
            "service": "safi-local-gate-adapter",
            "recovery": request.app.state.recovery_summary,
        }

    @app.post("/gate/dispatch")
    async def internal_gate_dispatch(
        submission: ActionSubmission, request: Request
    ) -> dict[str, Any]:
        """Single callback endpoint for n8n/Windmill/Calnode workflows to submit controlled effects (R-GATE-01)."""
        return await request.app.state.dispatcher.submit_action(submission)

    app.include_router(business_ops_router)
    app.include_router(gate_ops_router)
    return app


app = create_app()
