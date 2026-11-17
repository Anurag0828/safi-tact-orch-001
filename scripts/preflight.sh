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

# 3. Git repository & assigned SAFI remote check
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "[OK] Git repository initialized (HEAD: $(git rev-parse --short HEAD 2>/dev/null || echo 'no-commits'))"
  if [[ -n "$(git remote 2>/dev/null || true)" ]]; then
    echo "[OK] Git remote configured: $(git remote -v | head -n 1)"
  else
    echo "[MISSING] Assigned SAFI Git remote is not configured (git remote -v is empty)"
    BLOCKED=1
  fi
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
  # Parse key=value safely without executing arbitrary shell code
  get_env_val() {
    grep -E "^$1=" deploy/.env | head -n 1 | cut -d'=' -f2-
  }
  GATE_API_TOKEN_VAL="$(get_env_val GATE_API_TOKEN)"
  CALNODE_IMG_VAL="$(get_env_val CALNODE_PINNED_IMAGE)"
  NATS_URL_VAL="$(get_env_val NATS_URL)"
  SAFI_AUTHZ_URL_VAL="$(get_env_val SAFI_AUTHZ_URL)"
  AUTHZ_MODE_VAL="$(get_env_val AUTHZ_MODE)"

  if [[ -n "$GATE_API_TOKEN_VAL" ]]; then
    echo "[OK] GATE_API_TOKEN configured"
  else
    echo "[MISSING] GATE_API_TOKEN must be set in deploy/.env"
    BLOCKED=1
  fi

  if [[ "$CALNODE_IMG_VAL" == *"@sha256:"* ]]; then
    echo "[OK] CALNODE_PINNED_IMAGE digest pinned"
  else
    echo "[MISSING] CALNODE_PINNED_IMAGE must include an @sha256 digest"
    BLOCKED=1
  fi

  if [[ -n "$NATS_URL_VAL" ]]; then
    echo "[OK] NATS_URL configured: ${NATS_URL_VAL}"
  else
    echo "[MISSING] NATS_URL not configured in deploy/.env"
    BLOCKED=1
  fi

  if [[ -n "$SAFI_AUTHZ_URL_VAL" ]]; then
    echo "[OK] SAFI_AUTHZ_URL configured: ${SAFI_AUTHZ_URL_VAL}"
  elif [[ "$AUTHZ_MODE_VAL" == "local_acl" ]]; then
    echo "[WARN] SAFI_AUTHZ_URL unset; running in fail-closed AUTHZ_MODE=local_acl (SAFI Cognito V-08 still BLOCKED)"
    BLOCKED=1
  else
    echo "[MISSING] SAFI_AUTHZ_URL not configured (V-08)"
    BLOCKED=1
  fi
else
  echo "[MISSING] deploy/.env missing (copy from deploy/.env.template and fill credentials)"
  BLOCKED=1
fi

echo "==============================================="
if [[ "$BLOCKED" -ne 0 ]]; then
  echo "RESULT: BLOCKED — Resolve all [MISSING]/[WARN] SAFI preconditions before final acceptance."
  exit 1
else
  echo "RESULT: READY — Proceed to Phase 1 (T-01)."
  exit 0
fi
