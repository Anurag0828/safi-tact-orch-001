# Known Limitations & Excluded Notification Paths (Part H / R-GATE-02)

## 1. Excluded Automatic Notification Paths (R-GATE-02)
Per SOW §4 and Requirement R-GATE-02, automatic notification paths that cannot be routed through the Local Gate Adapter are **disabled and explicitly excluded** in this pilot:

| Engine | Notification Path | Status | Reason & Enforcement |
|---|---|---|---|
| **Calnode** | Built-in booking confirmation emails (`calnode.builtin.email_confirmation`) | **DISABLED / EXCLUDED** | Would bypass the Gate Adapter on booking creation. Disabled via `DISABLE_NOTIFICATIONS=true`, empty `SMTP_HOST`, and `suppressNotifications: true` in `adapters/calnode.py` and `calnode/test-config.json`. |
| **Calnode** | Autonomous scheduled reminders (`calnode.builtin.reminder_cron`) | **DISABLED / EXCLUDED** | Cron-triggered notifications cannot be serialized or held per conversation in this pilot. |
| **n8n** | Native SMTP / Email / SMS / external HTTP nodes | **PROHIBITED** | Exported workflow `workflows/n8n/lead_intake_gated_followup.json` routes all controlled outbound effects exclusively to `POST /gate/dispatch`. |
| **Windmill** | Direct email/Slack/webhook side effects inside scripts | **PROHIBITED** | Exported script `workflows/windmill/ops_task_assignment.py` produces deterministic assignment state only; outbound notifications must go through the Gate. |

## 2. Live Environment Verification Items (`UNVERIFIED` Until Phase 0/1 on Linux Host)
Because this build was authored and verified on a Windows workstation without Docker or live SAFI backend connectivity:
- **Docker / UNIX Host (A01):** `deploy/docker-compose.yml` is complete with pinned versions for n8n (`1.68.0`), Windmill (`1.425.0`), and Postgres (`16.4-alpine`), but `CALNODE_PINNED_IMAGE` must be pinned to the exact verified release digest from `https://github.com/Calnode/calnode` during Phase 1 T-01 on the UNIX/Linux host.
- **Real Engine Payloads (C4: V-01 to V-05):** `contracts/*.example.json` currently contain clearly marked `UNVERIFIED_TEMPLATE` payloads; replace them with real captured responses in T-02.
- **SAFI Backend Contracts (C4: V-06 to V-08):** Live SafiChat invocation URLs, JetStream stream/subject names, and SAFI authz endpoint URLs must be supplied via `deploy/.env` during Phase 0 Preflight.

## 3. Explicit Out-of-Scope Boundaries (SOW §10 / Part B5)
- Undo of already-dispatched external actions (never promised; Invariant 6).
- High-availability / multi-node clustering of the Local Gate Adapter.
- Replacing JetStream as authoritative Chat History.
- Public unauthenticated webhooks or bulk outbound outreach.
