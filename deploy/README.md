# Local Stack Operations: Start, Stop, Restore, Remove (R-ENV-03)

All commands are run from the repository root on a local UNIX/Linux host with Docker and Docker Compose v2+.

## 1. Initial Setup (One-Time)
```bash
cp deploy/.env.template deploy/.env
# Edit deploy/.env and replace EVERY placeholder with strong, non-default credentials.
chmod 600 deploy/.env
```

## 2. Start the Stack
Against SAFI's existing NATS JetStream backend:
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
docker compose -f deploy/docker-compose.yml --env-file deploy/.env ps
```

With the optional local standalone NATS JetStream profile (for isolated local verification when SAFI's NATS backend is not attached):
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats up -d --build
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats ps
```

Verify local health endpoints (all bound strictly to `127.0.0.1`):
- Gate Adapter: `curl -sf http://127.0.0.1:8000/health`
- n8n: `curl -sf http://127.0.0.1:5678/healthz`
- Calnode: `curl -sf http://127.0.0.1:3000/healthz`
- Windmill: `curl -sf http://127.0.0.1:8001/api/version`

## 3. Stop (Preserves Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats stop
```

## 4. Restart & Recovery Test (R-REC-01 / A10)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats down
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats up -d
# Verify recovery summary via Gate health endpoint:
curl -sf http://127.0.0.1:8000/health
```
On restart, the Gate Adapter connects to JetStream and inspects its SQLite WAL store:
- `GATED` modes and `HELD` action references are restored intact.
- Any action interrupted mid-`DISPATCHING` is marked `FAILED` (`outcome="unknown"`), its conversation is set to `FAILED_CLOSED`, and a `GATE_FAILED_CLOSED` `ControlEvent` is emitted to JetStream until reconciled via `POST /safichat/ops/gate/reconcile`.
- Any conversation interrupted mid-`TRANSITIONING` is set to `FAILED_CLOSED` and logged to JetStream until reconciled.

## 5. Complete Removal (Destroys Containers and Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats down -v --remove-orphans
```
