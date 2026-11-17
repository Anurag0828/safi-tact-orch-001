# Phase 0 Preflight Checklist (T-00 / Part B6)

Per SOW §3 and §10, verify EVERY precondition below before final SAFI acceptance. Items dependent on SAFI-supplied infrastructure are recorded honestly as `BLOCKED` — never hidden as partial success.

| Precondition | Status | Verification Evidence |
|---|---|---|
| Assigned SAFI Git repository remote | `BLOCKED (AWAITING SAFI)` | Local Git repo initialized & committed; attach SAFI remote URL and push exact SHA |
| Local UNIX/Linux machine with Docker + Compose | `OK` | Ubuntu 24.04 LTS (WSL2) with Docker `29.10.0` + Docker Compose `v5.8.0`; verified via `./scripts/preflight.sh` |
| Pinned installation paths for n8n, Calnode, Windmill | `OK` | Exact `@sha256:` digests pinned in `deploy/docker-compose.yml` & `deploy/Dockerfile.gate` |
| Bounded test calendar (Calnode) | `OK` | Live Calnode v0.14.0 event type `consultation-30m` configured (`invite_delivery: "none"`, `reminders: []`, SMTP disabled); documented in `calnode/test-config.json` |
| Approved non-default service credentials | `OK` | Secret-free `deploy/.env.template` committed; non-default random credentials in gitignored `deploy/.env` (including `GATE_API_TOKEN`, `N8N_WEBHOOK_SECRET`, Windmill/Calnode keys) |
| Consenting/allowlisted test contacts | `OK` | `SAFI_ALLOWED_RECIPIENTS` enforced fail-closed in `gate/config.py` and verified live |
| Authenticated SafiChat test User & Cognito Authz (`V-08`) | `BLOCKED (AWAITING SAFI)` | `HttpPermissionProvider` fail-closed ready; supply `SAFI_AUTHZ_URL` + `SAFI_AUTHZ_TOKEN` |
| Working Meeting Manager / SAFI NATS / JetStream (`V-07`) | `BLOCKED (AWAITING SAFI)` | Verified locally on `nats:2.10-alpine` (`JETSTREAM_REQUIRED=true`); supply SAFI `NATS_URL`, credentials, stream & subject prefix |
| SafiChat operation-invocation contract (`V-06`) | `BLOCKED (AWAITING SAFI)` | Authenticated `/safichat/ops/*` & `/safichat/ops/gate/*` ready to bind to native SafiChat |
