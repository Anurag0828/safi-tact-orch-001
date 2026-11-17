# Acceptance Test Evidence Log (Part G: A01 to A12)

> **Rule (SOW §8 & Spec Part G):** An acceptance test is marked **PASS** ONLY when observed against the REAL local UNIX services (n8n, Calnode, Windmill, SafiChat, NATS JetStream).
> Automated local unit/integration tests verify all Gate Adapter logic, contracts, state machine transitions, concurrency, authorization, and recovery behavior, while live host/container tests remain **BLOCKED** until executed on the SAFI-supplied UNIX/Linux host with Docker and SAFI endpoints.

## Current Verification Matrix

| Test | Area | Automated Logic / Contract Verification | Live UNIX Host Acceptance Status | Notes |
|---|---|---|---|---|
| **A01** | Local UNIX stack | Compose manifest & preflight script authored | **BLOCKED** | Requires UNIX/Linux host with Docker & Compose |
| **A02** | SafiChat-only control | `tests/test_e2e_safichat.py` | **BLOCKED** | Requires live SafiChat test user & endpoints |
| **A03** | Marketing (n8n) | `tests/test_e2e_safichat.py`, `workflows/n8n/*.json` | **BLOCKED** | Requires running n8n container |
| **A04** | Sales (Calnode) | `tests/test_gate_dispatcher.py`, `tests/test_e2e_safichat.py` | **BLOCKED** | Requires running Calnode container |
| **A05** | Operations (Windmill) | `tests/test_e2e_safichat.py`, `workflows/windmill/*.py` | **BLOCKED** | Requires running Windmill container |
| **A06** | Selective Gated | `tests/test_modes_and_barge_in.py::test_a06_*` | **BLOCKED** | Requires live 3-conversation SafiChat run |
| **A07** | Ungate / no duplicate | `tests/test_modes_and_barge_in.py::test_a07_*` | **BLOCKED** | Requires live service-side counts |
| **A08** | Race and Barge In | `tests/test_modes_and_barge_in.py::test_a08_*` | **BLOCKED** | Requires live dispatch race capture |
| **A09** | Authorization | `tests/test_authz.py` | **BLOCKED** | Requires live SAFI Cognito / authz endpoint |
| **A10** | Restart / failure | `tests/test_modes_and_barge_in.py::test_a10_*` | **BLOCKED** | Requires `docker compose down && up` proof |
| **A11** | History / Attribution | `tests/test_e2e_safichat.py::test_a11_*` | **BLOCKED** | Requires live NATS JetStream stream capture |
| **A12** | Independent use | `docs/safichat_user_guide.md` delivered | **BLOCKED** | Requires independent SAFI tester trial |

---

## Live Evidence Record Template (Fill per test A01–A12 on Linux Host)

```text
Test: A__   Status: PASS | FAIL | BLOCKED
Timestamp: ____   Git SHA: ____
ConversationID(s): ____   Booking ID: ____   Job ID: ____   Event ID(s): ____
Observed: ____   Redacted logs / screenshots: ____
```
