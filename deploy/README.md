# Local Stack Operations: Bootstrap, Start, Stop, Restore, Remove (R-ENV-01, R-ENV-02, R-ENV-03)

All commands are run from the repository root on a local UNIX/Linux host with Docker and Docker Compose v2+.

## 1. Initial Environment Setup (One-Time)
```bash
cp deploy/.env.template deploy/.env
# Edit deploy/.env and replace EVERY placeholder with strong, non-default credentials.
chmod 600 deploy/.env
```

- **Production SAFI backend:** Set `NATS_URL` (plus `NATS_TOKEN` or `NATS_USER`/`NATS_PASSWORD`) to SAFI's NATS cluster, keep `JETSTREAM_AUTO_CREATE_STREAM=false`, keep `AUTHZ_MODE=http`, and configure `SAFI_AUTHZ_URL` + `SAFI_AUTHZ_TOKEN`.
- **Isolated local verification (`--profile local-nats`):** Set in `deploy/.env`:
  ```env
  NATS_URL=nats://nats:4222
  JETSTREAM_AUTO_CREATE_STREAM=true
  AUTHZ_MODE=local_acl
  LOCAL_ACL_JSON={"bootstrapActors":["user_safi_pilot_owner_01"],"proxyActors":["proxy-n8n-workflow"],"conversations":{},"revokedActors":[]}
  WINDMILL_ALLOWED_SCRIPTS=f/safi/ops_task_assignment
  ```

## 2. Start the Stack
Against SAFI's existing NATS JetStream backend:
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
docker compose -f deploy/docker-compose.yml --env-file deploy/.env ps
```

With the optional local standalone NATS JetStream profile:
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats up -d --build
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats ps
```

Verify local health endpoints (all bound strictly to `127.0.0.1`):
- Gate Adapter: `curl -sf http://127.0.0.1:8000/health`
- n8n: `curl -sf http://127.0.0.1:5678/healthz`
- Calnode: `curl -sf http://127.0.0.1:3000/healthz`
- Windmill: `curl -sf http://127.0.0.1:8001/api/version`

## 3. Clean-Volume Service Bootstrap (One-Time Per Fresh Volume)
When starting from empty Docker volumes, perform these one-time non-default setup steps before running end-to-end traffic:

1. **n8n (`http://127.0.0.1:5678`)**
   - Create a non-default owner account via the n8n setup screen and generate an API key (`N8N_API_KEY`).
   - Create two native **Header Auth (`httpHeaderAuth`)** credentials (required because `N8N_BLOCK_ENV_ACCESS_IN_NODE="true"` blocks `$env` access):
     - `SAFI Webhook Header Auth`: header `X-SAFI-Webhook-Secret`, value = `N8N_WEBHOOK_SECRET`.
     - `SAFI Gate Callback Header Auth`: header `X-SAFI-Gate-Secret`, value = `GATE_API_TOKEN`.
   - Import and activate both exported workflows from `workflows/n8n/lead_intake_gated_followup.json` and `workflows/n8n/approved_followup_dispatch.json`, binding the two Header Auth credentials above.
2. **Calnode (`http://127.0.0.1:3000`)**
   - Claim the instance with a non-default admin account and generate an API key (`CALNODE_API_KEY`).
   - Create the bounded test event type matching `calnode/test-config.json` (`slug: consultation-30m`, 30 minutes, `invite_delivery: "none"`, `reminders: []`). Verify `EMAIL_SMTP_HOST=""` remains unset so built-in emails stay disabled (`R-GATE-02`).
3. **Windmill (`http://127.0.0.1:8001`)**
   - Log in with `WINDMILL_ADMIN_EMAIL` / `WINDMILL_ADMIN_PASSWORD` and confirm the default `admin@windmill.dev` account is removed from the `password` / `usr` tables (`R-ENV-02`).
   - Create workspace `safi-ops` (matching `WINDMILL_WORKSPACE`).
   - Create a **least-privilege, non-superadmin** workspace operator (e.g., `safi_ops_runner@local.test` / `u/safi_ops_runner`, `super_admin: false`, workspace `operator` role) and store its scoped token in `WINDMILL_TOKEN` (`R-SEC-03`).
   - Deploy `workflows/windmill/ops_task_assignment.py` at path `f/safi/ops_task_assignment` (matching `WINDMILL_ALLOWED_SCRIPTS`).
4. **Apply updated tokens** to `deploy/.env` and restart `gate-adapter`:
   ```bash
   docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats up -d gate-adapter
   ```

## 4. Stop (Preserves Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats stop
```

## 5. Restart & Recovery Test (R-REC-01 / A10)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats down
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats up -d
# Verify recovery summary via Gate health endpoint:
curl -sf http://127.0.0.1:8000/health
```
On restart, the Gate Adapter connects to JetStream (with bounded startup retry) and inspects its SQLite WAL store:
- `GATED` modes and `HELD` action references are restored intact.
- Any action interrupted mid-`DISPATCHING` is marked `FAILED` (`outcome="unknown"`), its conversation is set to `FAILED_CLOSED`, and a `GATE_FAILED_CLOSED` `ControlEvent` is emitted to JetStream until reconciled via `POST /safichat/ops/gate/reconcile`.
- Any conversation interrupted mid-`TRANSITIONING` is set to `FAILED_CLOSED` and logged to JetStream until reconciled.

## 6. Complete Removal (Destroys Containers and Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats down -v --remove-orphans
```
