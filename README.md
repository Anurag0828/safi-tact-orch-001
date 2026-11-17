# SAFI-TACT-ORCH-001: Local Business Orchestration (Gated / Ungated)

Complete implementation of the **Local Gate Adapter**, **SafiChat operation wiring**, **JSON Schema contracts**, **engine adapters & exported workflows** (n8n, Calnode, Windmill), **Docker Compose stack**, and **automated verification test suite** for SOW `SAFI-TACT-ORCH-001`.

## Architecture Overview

- **SafiChat Operations (`safichat/ops/`)**: Native operation handlers exposing Marketing (n8n), Sales (Calnode), Operations (Windmill), and Gate Controls (Barge In, Held Actions, Approve/Reject, Return to Ungated, Reconcile).
- **Local Gate Adapter (`gate/`)**: Single trusted per-conversation dispatcher with:
  - Per-conversation serialization (`asyncio.Lock` per `ConversationID`, no global lock).
  - Strict conversation state machine (`UNGATED` ↔ `TRANSITIONING` ↔ `GATED` | `FAILED_CLOSED`).
  - Action lifecycle enforcement (`RECEIVED` → `HELD` → `APPROVED` → `AUTHORIZED` → `DISPATCHING` → `DONE` | `FAILED` | `REJECTED` | `CANCELLED`).
  - Mandatory fresh SAFI permission recheck immediately before every dispatch (`R-GATE-09`).
  - At-most-once execution per idempotency key and unknown-outcome fail-closed handling (`E5 Invariants 1, 4, 8`).
  - SQLite WAL operational state store (`gate/store.py`) that never acts as authoritative chat history (`R-CTX-03`).
  - Strict JSON Schema validated `ControlEvent` emission to JetStream (`gate/history.py`, `R-CTX-01`, `R-CTX-02`).
  - Consenting recipient allowlist enforcement (`R-SEC-01`).
- **Contracts (`contracts/`)**: Strict Draft 2020-12 JSON Schemas (`additionalProperties: false`) and error code specification (`contracts/errors.md`).
- **Engine Workflows & Config (`workflows/`, `calnode/`)**:
  - `workflows/n8n/lead_intake_gated_followup.json`: Routes prepared follow-ups strictly to `POST /gate/dispatch` (no direct send nodes).
  - `workflows/windmill/ops_task_assignment.py`: Deterministic idempotent operational assignment script.
  - `calnode/test-config.json`: Bounded test calendar configuration with ungateable auto-notifications disabled (`R-GATE-02`).

## Running Automated Verification Tests

```bash
python -m pytest
```

## Deploying & Operating on a UNIX/Linux Host

1. Run preflight validation:
   ```bash
   ./scripts/preflight.sh
   ```
2. Follow [deploy/README.md](deploy/README.md) to configure `deploy/.env` from `deploy/.env.template`, start, stop, restart/recover, or remove the Docker Compose stack.
3. Review [docs/safichat_user_guide.md](docs/safichat_user_guide.md) and [docs/known_limitations.md](docs/known_limitations.md).
