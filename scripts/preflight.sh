#!/usr/bin/env bash
# SAFI-TACT-ORCH-001 Phase 0 Preflight Check (Part B6 / T-00)
# Exits non-zero and prints BLOCKED if any mandatory precondition is missing.

set -euo pipefail

BLOCKED=0

check_cmd() {
  if command -v "$1" >/dev/null 2>&1; then
    echo "[OK] $1 found: $($1 --version 2>&1 | head -n 1)"
  else
    echo "[MISSING] $1 not found in PATH"
    BLOCKED=1
  fi
}

echo "=== SAFI-TACT-ORCH-001 Preflight Verification ==="

# 1. Host OS check (must be UNIX/Linux)
OS_NAME="$(uname -s || true)"
if [[ "$OS_NAME" == "Linux" || "$OS_NAME" == "Darwin" ]]; then
  echo "[OK] UNIX/Linux host detected: $OS_NAME"
else
  echo "[MISSING] Non-UNIX host detected: $OS_NAME (SOW requires local UNIX/Linux machine)"
  BLOCKED=1
fi

# 2. Required tooling
check_cmd git
check_cmd docker
if docker compose version >/dev/null 2>&1; then
  echo "[OK] docker compose: $(docker compose version)"
else
  echo "[MISSING] docker compose v2 plugin not available"
  BLOCKED=1
fi
check_cmd python3

# 3. Git repository & remote check
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[OK] Git repository initialized (HEAD: $(git rev-parse --short HEAD 2>/dev/null || echo 'no-commits'))"
else
  echo "[MISSING] Not inside an assigned SAFI Git repository"
  BLOCKED=1
fi

# 4. Environment file & non-default secret check
if [[ -f "deploy/.env" ]]; then
  echo "[OK] deploy/.env exists"
  if grep -q "replace_with_" deploy/.env; then
    echo "[MISSING] deploy/.env still contains unreplaced placeholder values"
    BLOCKED=1
  fi
else
  echo "[MISSING] deploy/.env missing (copy from deploy/.env.template and fill credentials)"
  BLOCKED=1
fi

# 5. Live SAFI NATS endpoint check (if configured)
if [[ -f "deploy/.env" ]]; then
  # shellcheck disable=SC1091
  source deploy/.env
  if [[ -n "${NATS_URL:-}" ]]; then
    echo "[INFO] Configured NATS_URL=${NATS_URL} (verify live connectivity + JetStream stream '${JETSTREAM_STREAM:-SAFI_HISTORY}' in T-00)"
  else
    echo "[MISSING] NATS_URL not configured in deploy/.env"
    BLOCKED=1
  fi
fi

echo "==============================================="
if [[ "$BLOCKED" -ne 0 ]]; then
  echo "RESULT: BLOCKED — Resolve all [MISSING] items before starting the 6-hour clock."
  exit 1
else
  echo "RESULT: READY — Proceed to Phase 1 (T-01)."
  exit 0
fi
