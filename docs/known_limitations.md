# Known Limitations & Excluded Notification Paths (SOW §4, §9, §10 / R-GATE-02)

## 1. Excluded Automatic Notification Paths (R-GATE-02)
Per SOW §4 and Requirement R-GATE-02, automatic notification paths that cannot be routed through the Local Gate Adapter are **disabled and explicitly excluded** in this pilot:

| Engine | Notification Path | Status | Reason & Enforcement |
|---|---|---|---|
| **Calnode** | Built-in booking confirmation emails (`calnode.builtin.email_confirmation`) | **DISABLED / EXCLUDED** | Would bypass the Gate Adapter on booking creation. Event type `consultation-30m` is configured with `invite_delivery: "none"` (`invite_sender_ready: false`) and `SMTP_HOST=""` in `deploy/docker-compose.yml` (`calnode/test-config.json`). |
| **Calnode** | Autonomous scheduled reminders (`calnode.builtin.reminder_cron`) | **DISABLED / EXCLUDED** | Cron-triggered reminders cannot be serialized or held per conversation in this pilot. Event type `consultation-30m` is configured with `reminders: []`. |
| **n8n** | Native SMTP / Email / SMS / unauthenticated webhook triggers | **PROHIBITED / GUARDED** | Exported workflows `workflows/n8n/lead_intake_gated_followup.json` and `workflows/n8n/approved_followup_dispatch.json` have zero direct external send nodes, require valid `X-SAFI-Webhook-Secret` (+ `gateApproved === true` on follow-up dispatch), and submit controlled effects exclusively to `POST /gate/dispatch` with `X-SAFI-Gate-Secret`. |
| **Windmill** | Direct email/Slack/webhook side effects inside scripts | **PROHIBITED** | Exported script `workflows/windmill/ops_task_assignment.py` produces deterministic assignment state only; outbound notifications must go through the Gate Adapter. |

## 2. External SAFI Preconditions Currently `BLOCKED` (SOW §3 & §10)
The local UNIX stack (Ubuntu 24.04 LTS WSL2 + Docker + Compose), all three engines (`n8n:1.68.0`, `Calnode v0.14.0`, `Windmill CE v1.425.0`), local NATS JetStream (`nats:2.10-alpine`), and the hardened Local Gate Adapter have been verified live on `127.0.0.1` with exact SHA256 image pins and real captured payloads in `contracts/*.example.json`.

The following items depend on SAFI-supplied infrastructure and are explicitly reported as **`BLOCKED`** (not partial pass) until supplied by SAFI:
- **Assigned SAFI Git Remote:** No SAFI remote repository URL/credentials have been attached (`git remote -v` is empty); local Git commit SHA is recorded in `evidence/README.md` and `evidence/live_local_run_report.json`.
- **Live SafiChat / Meeting Manager UI Hook (`V-06` / `A02`):** Authenticated REST endpoints (`/safichat/ops/*`, `/safichat/ops/gate/*`) are verified live, but wiring into SAFI's native SafiChat UI requires SAFI's operation-invocation contract (`V-06`).
- **Live SAFI Cognito / Authz Endpoint (`V-08` / `A09`):** `HttpPermissionProvider` is implemented and fail-closed (`AUTHZ_MODE=http` rejects if `SAFI_AUTHZ_URL` is unset). Local live testing uses explicit default-deny `AUTHZ_MODE=local_acl`; connecting SAFI's real Cognito permission-recheck endpoint requires `SAFI_AUTHZ_URL` + `SAFI_AUTHZ_TOKEN`.
- **SAFI Production NATS/JetStream Cluster (`V-07` / `A11`):** Verified live against local `nats:2.10-alpine` stream `SAFI_HISTORY` (`safi.control.events.<conversationId>.<eventType>`). Connecting to SAFI's existing NATS cluster requires `NATS_URL`, credentials, and confirmation of SAFI's permitted subject prefix.
- **Independent SAFI Tester Trial (`A12`):** Operator guide is ready at [docs/safichat_user_guide.md](safichat_user_guide.md); requires an independent SAFI tester to execute end-to-end.

## 3. Explicit Out-of-Scope Boundaries (SOW §10 / Part B5)
- Undo of already-dispatched external actions (never promised; `undone: false`, Invariant 6).
- High-availability / multi-node clustering of the Local Gate Adapter.
- Replacing JetStream as authoritative Chat History (terminal payloads are purged from the Gate's SQLite operational store; only refs/hashes and operational states remain).
- Public unauthenticated webhooks, live ad spend, production financial transactions, or bulk outbound outreach.
