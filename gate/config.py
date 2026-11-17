"""Configuration for the SAFI Local Gate Adapter (R-SEC-01, R-SEC-03, R-SEC-04)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes")


@dataclass(frozen=True)
class GateConfig:
    # Local Gate Adapter operational store (SQLite WAL - R-CTX-03: NOT authoritative chat history)
    store_path: str = "data/gate_state.db"

    # Recipient allowlist (R-SEC-01, R-SEC-02: only explicit consenting test contacts)
    allowed_recipients: frozenset[str] = field(default_factory=frozenset)

    # Local engine base URLs (bound to localhost / internal compose network)
    n8n_base_url: str = "http://127.0.0.1:5678"
    n8n_api_key: str = ""
    n8n_webhook_secret: str = ""

    calnode_base_url: str = "http://127.0.0.1:3000"
    calnode_api_key: str = ""
    calnode_calendar_id: str = "consultation-30m"

    windmill_base_url: str = "http://127.0.0.1:8001"
    windmill_token: str = ""
    windmill_workspace: str = "safi-ops"
    windmill_max_poll_attempts: int = 60
    windmill_poll_interval_seconds: float = 0.25

    # Gate Adapter endpoint authentication — FAIL-CLOSED by default (R-SEC-02, R-SEC-03)
    gate_api_token: str = ""
    gate_require_auth: bool = True

    # Authorization mode: 'http' (requires SAFI_AUTHZ_URL, fails closed if missing)
    # or 'local_acl' (explicit default-deny ACL from LOCAL_ACL_JSON for local pilot)
    authz_mode: str = "http"
    local_acl_json: str = ""

    # SAFI backend endpoints (UNVERIFIED until Phase 0 V-06/V-07/V-08)
    safi_authz_url: str = ""
    safi_authz_token: str = ""
    nats_url: str = "nats://127.0.0.1:4222"
    nats_user: str = ""
    nats_password: str = ""
    nats_token: str = ""
    jetstream_stream: str = "SAFI_HISTORY"
    jetstream_control_subject_prefix: str = "safi.control.events"
    # Fail-closed by default; never auto-mutate production NATS streams unless explicitly opted in
    jetstream_required: bool = True
    jetstream_auto_create_stream: bool = False

    # Timeouts (seconds)
    http_timeout_seconds: float = 10.0
    authz_timeout_seconds: float = 3.0
    inflight_reconcile_timeout_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "GateConfig":
        raw_allowlist = os.getenv("SAFI_ALLOWED_RECIPIENTS", "")
        allowed = frozenset(
            item.strip().lower()
            for item in raw_allowlist.split(",")
            if item.strip()
        )
        return cls(
            store_path=os.getenv("GATE_STORE_PATH", "data/gate_state.db"),
            allowed_recipients=allowed,
            n8n_base_url=os.getenv("N8N_BASE_URL", "http://127.0.0.1:5678").rstrip("/"),
            n8n_api_key=os.getenv("N8N_API_KEY", ""),
            n8n_webhook_secret=os.getenv("N8N_WEBHOOK_SECRET", ""),
            calnode_base_url=os.getenv("CALNODE_BASE_URL", "http://127.0.0.1:3000").rstrip("/"),
            calnode_api_key=os.getenv("CALNODE_API_KEY", ""),
            calnode_calendar_id=os.getenv("CALNODE_CALENDAR_ID", "consultation-30m"),
            windmill_base_url=os.getenv("WINDMILL_BASE_URL", "http://127.0.0.1:8001").rstrip("/"),
            windmill_token=os.getenv("WINDMILL_TOKEN", ""),
            windmill_workspace=os.getenv("WINDMILL_WORKSPACE", "safi-ops"),
            windmill_max_poll_attempts=int(os.getenv("WINDMILL_MAX_POLL_ATTEMPTS", "60")),
            windmill_poll_interval_seconds=float(os.getenv("WINDMILL_POLL_INTERVAL_SECONDS", "0.25")),
            gate_api_token=os.getenv("GATE_API_TOKEN", ""),
            gate_require_auth=_parse_bool_env("GATE_REQUIRE_AUTH", True),
            authz_mode=os.getenv("AUTHZ_MODE", "http").strip().lower(),
            local_acl_json=os.getenv("LOCAL_ACL_JSON", ""),
            safi_authz_url=os.getenv("SAFI_AUTHZ_URL", "").rstrip("/"),
            safi_authz_token=os.getenv("SAFI_AUTHZ_TOKEN", ""),
            nats_url=os.getenv("NATS_URL", "nats://127.0.0.1:4222"),
            nats_user=os.getenv("NATS_USER", ""),
            nats_password=os.getenv("NATS_PASSWORD", ""),
            nats_token=os.getenv("NATS_TOKEN", ""),
            jetstream_stream=os.getenv("JETSTREAM_STREAM", "SAFI_HISTORY"),
            jetstream_control_subject_prefix=os.getenv(
                "JETSTREAM_CONTROL_SUBJECT_PREFIX", "safi.control.events"
            ),
            jetstream_required=_parse_bool_env("JETSTREAM_REQUIRED", True),
            jetstream_auto_create_stream=_parse_bool_env("JETSTREAM_AUTO_CREATE_STREAM", False),
            http_timeout_seconds=float(os.getenv("GATE_HTTP_TIMEOUT_SECONDS", "10.0")),
            authz_timeout_seconds=float(os.getenv("GATE_AUTHZ_TIMEOUT_SECONDS", "3.0")),
            inflight_reconcile_timeout_seconds=float(
                os.getenv("GATE_INFLIGHT_TIMEOUT_SECONDS", "30.0")
            ),
        )

    def is_recipient_allowed(self, recipient: str) -> bool:
        """Fail-closed allowlist check (R-SEC-01, R-SEC-04)."""
        if not recipient or not self.allowed_recipients:
            return False
        return recipient.strip().lower() in self.allowed_recipients
