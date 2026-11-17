# Phase 0 Preflight Checklist (T-00 / Part B6)

Before starting the 6-working-hour SOW clock on a UNIX/Linux host, verify EVERY item below and record evidence. If any item is `MISSING`, mark **BLOCKED** and stop.

| Precondition | Status | Verification Command / Evidence |
|---|---|---|
| Assigned SAFI Git repository | `PENDING_REMOTE` | Local repo initialized; attach SAFI remote URL and push exact SHA |
| Local UNIX/Linux machine with Docker + Compose | `PENDING_LINUX_HOST` | Run `./scripts/preflight.sh` on target UNIX/Linux host |
| Authenticated SafiChat test User | `UNVERIFIED` | Supply Cognito / SafiChat test credentials & actor GUID |
| Working Meeting Manager / NATS / JetStream adapter | `UNVERIFIED` | Supply `NATS_URL`, `JETSTREAM_STREAM`, and permitted subject prefix (V-07) |
| Bounded test calendar (Calnode) | `CONFIGURED_LOCAL` | `calnode/test-config.json` ready; pin live Calnode image in `deploy/.env` |
| Approved service credentials (non-default) | `TEMPLATE_READY` | Copy `deploy/.env.template` to `deploy/.env` and replace all placeholders |
| Consenting/allowlisted test contacts | `CONFIGURED_LOCAL` | `SAFI_ALLOWED_RECIPIENTS` enforced in `gate/config.py` (R-SEC-01) |
| SafiChat operation-invocation contract (V-06) | `UNVERIFIED` | Verify live SafiChat hook mapping to `/safichat/ops/*` |
| SAFI permission-recheck endpoint (V-08 / R-GATE-09) | `UNVERIFIED` | Configure `SAFI_AUTHZ_URL` and `SAFI_AUTHZ_TOKEN` in `deploy/.env` |
