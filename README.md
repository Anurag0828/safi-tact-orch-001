# SAFI-TACT-ORCH-001: Local Business Orchestration (Gated / Ungated)

Hardened implementation of the **Local Gate Adapter**, **SafiChat operation wiring**, **JSON Schema contracts & captured event examples**, **engine adapters & exported workflows** (n8n, Calnode, Windmill), **digest-pinned Docker Compose stack**, **automated unit/integration tests**, and **live verification harness** for SOW `SAFI-TACT-ORCH-001`.

## Architecture Overview

- **SafiChat Operations (`safichat/ops/`)**: Authenticated (`Authorization: Bearer <GATE_API_TOKEN>`) operation handlers exposing Marketing (n8n), Sales (Calnode), Operations (Windmill), and Gate Controls (Barge In, Held Actions, Approve/Reject, Return to Ungated, Reconcile), validated against `contracts/gate-operation.schema.json`.
- **Local Gate Adapter (`gate/`)**: Single trusted per-conversation dispatcher with:
  - Per-conversation serialization (`asyncio.Lock` per `ConversationID`, no global lock).
  - Strict conversation state machine (`UNGATED` ↔ `TRANSITIONING` ↔ `GATED` | `FAILED_CLOSED`) with deadlock-free failure recovery.
  - Strict action lifecycle enforcement (`assert_valid_action_transition` inside `gate/store.py`: `RECEIVED` → `HELD` → `APPROVED` → `AUTHORIZED` → `DISPATCHING` → `DONE` | `FAILED` | `REJECTED` | `CANCELLED`).
  - Fail-closed authorization (`gate/authz.py`): default-deny ACL with persisted conversation ownership across restarts, mandatory fresh SAFI permission recheck (`R-GATE-09`) before every dispatch, and `IdentityUncertainError` fail-closed handling.
  - At-most-once execution per idempotency key (lock-guarded recheck on both `submit_action` and `dispatch_approved_action`, clean terminal replays, and `outcome="unknown"` fail-closed handling).
  - SQLite WAL operational state store (`gate/store.py`) that purges terminal message payloads and never acts as authoritative chat history (`R-CTX-03`).
  - Strict JSON Schema validated `ControlEvent` emission to NATS JetStream (`gate/history.py`, fail-closed when `JETSTREAM_REQUIRED=true`).
  - Consenting recipient allowlist enforcement (`R-SEC-01`).
- **Contracts (`contracts/`)**: Strict Draft 2020-12 JSON Schemas (`additionalProperties: false`), error code specification (`contracts/errors.md`), and real captured examples (`control-event`, `gate-action`, `gate-operation`, `calnode-slots`, `calnode-book`, `windmill-job`).
- **Engine Workflows & Config (`workflows/`, `calnode/`)**:
  - `workflows/n8n/lead_intake_gated_followup.json`: Enforces `X-SAFI-Webhook-Secret`, preserves full actor context, and posts controlled effects strictly to `POST /gate/dispatch` with `X-SAFI-Gate-Secret`.
  - `workflows/n8n/approved_followup_dispatch.json`: Enforces `X-SAFI-Webhook-Secret` AND `gateApproved === true` (returns `403 GATE_BYPASS_REJECTED` on direct bypass attempts).
  - `workflows/windmill/ops_task_assignment.py`: Deterministic idempotent operational assignment script.
  - `calnode/test-config.json`: Bounded test calendar (`consultation-30m`) with ungateable auto-notifications disabled (`invite_delivery: "none"`, `reminders: []`, `SMTP_HOST=""`).

## Running Automated & Live Verification

```bash
# 1. Automated unit, contract, concurrency, authz & crash-recovery test suite
python -m pytest -v

# 2. Live verification against the running local Docker Compose stack
python scripts/verify_sow_live.py
```

## Deploying & Operating on a UNIX/Linux Host

1. Run preflight validation:
   ```bash
   ./scripts/preflight.sh
   ```
2. Follow [deploy/README.md](deploy/README.md) to configure `deploy/.env` from `deploy/.env.template`, start, stop, restart/recover, or remove the Docker Compose stack.
3. Review [evidence/README.md](evidence/README.md), [docs/safichat_user_guide.md](docs/safichat_user_guide.md), and [docs/known_limitations.md](docs/known_limitations.md).
