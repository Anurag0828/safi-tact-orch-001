# Local Stack Operations: Start, Stop, Restore, Remove (R-ENV-03)

All commands are run from the repository root on a local UNIX/Linux host with Docker and Docker Compose v2+.

## 1. Initial Setup (One-Time)
```bash
cp deploy/.env.template deploy/.env
# Edit deploy/.env and replace EVERY placeholder with strong, non-default credentials.
chmod 600 deploy/.env
```

## 2. Start the Stack
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
docker compose -f deploy/docker-compose.yml ps
```

Verify local health endpoints (bound strictly to `127.0.0.1`):
- Gate Adapter: `curl -sf http://127.0.0.1:8000/health`
- n8n: `curl -sf http://127.0.0.1:5678/healthz`
- Calnode: `curl -sf http://127.0.0.1:3000/api/health`
- Windmill: `curl -sf http://127.0.0.1:8001/api/version`

## 3. Stop (Preserves Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml stop
```

## 4. Restart & Recovery Test (R-REC-01 / A10)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env down
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d
# Verify recovery summary via Gate health endpoint:
curl -sf http://127.0.0.1:8000/health
```
On restart, the Gate Adapter inspects its SQLite WAL store:
- `GATED` modes and `HELD` action references are restored intact.
- Any conversation interrupted mid-`TRANSITIONING` or mid-`DISPATCHING` is automatically set to `FAILED_CLOSED` until explicitly reconciled via `/safichat/ops/gate/reconcile`.

## 5. Complete Removal (Destroys Containers and Persistent Volumes)
```bash
docker compose -f deploy/docker-compose.yml --env-file deploy/.env down -v --remove-orphans
```
