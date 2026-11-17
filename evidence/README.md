# Acceptance Test Evidence Log (Part G: A01 to A12)

- **Tested Code Commit SHA:** `bd5039252009cc3f9379613a3c8cd60f2d70e812`
- **Automated Regression Suite:** `54 passed` (`python -m pytest`)
- **Machine-Captured Live Evidence Report:** [live_local_run_report.json](live_local_run_report.json)
- **Redacted Container Logs:** [redacted_stack_logs.txt](redacted_stack_logs.txt)

> **Strict SOW §8 / §10 Compliance:** Tests depending on SAFI-owned infrastructure (`A02` live SafiChat UI, `A09` live SAFI Cognito authz, `A11` SAFI's production NATS cluster, `A12` independent SAFI tester) are explicitly recorded as **BLOCKED** — never misrepresented as PASS.

## Observed Live Acceptance Matrix (Ubuntu 24.04 WSL2 + Docker 29.10.0 + Compose v5.8.0)

| Test | Area | Status | Timestamp (UTC) | Observed Evidence (Real Local Containers) |
|---|---|---|---|---|
| **A01** | Local UNIX stack | **PASS** | `2026-11-17T22:20:33Z` | All 7 containers healthy on `127.0.0.1` with SHA256-pinned digests, non-root Gate container, persistent named volumes, non-default creds (`admin@windmill.dev` deleted), `N8N_BLOCK_ENV_ACCESS_IN_NODE=true`. |
| **A02** | SafiChat-only control | **BLOCKED** | `2026-11-17T22:20:36Z` | REST control surface (`/safichat/ops/*`) verified live for `conv_sow_a_1794954032`; awaiting SAFI's native SafiChat UI hook (`V-06`). |
| **A03** | Marketing (n8n) | **PASS** | `2026-11-17T22:20:35Z` | Native n8n HeaderAuth rejected direct bypass calls (`403`/`403`); Gate-mediated lead intake (`exec=58`) triggered single Gate-authorized follow-up (`exec=59`, receipt `rcpt_59_1794954033084`, verified single execution via `idempotentReplay: true`). |
| **A04** | Sales (Calnode) | **PASS** | `2026-11-17T22:20:35Z` | Real Calnode v0.14.0 returned available slot `2026-11-18T14:00:00Z`, created booking `8e3bfb51-cff9-4e0b-a00e-bd4ff83b6f13` (`invite_delivery: "none"`, zero SMTP), and duplicate request replayed (`0 -> 1` confirmed total). |
| **A05** | Operations (Windmill) | **PASS** | `2026-11-17T22:20:36Z` | Executed under least-privilege operator `u/safi_ops_runner` (`super_admin: false`): job `01a1eb98-fec5-e302-c1aa-a01a89d16f3a` (`COMPLETED`, `assignmentId=asgn_697cff9afeb5`) + live error reporting verified (`502`, failed `jobId=01a1eb99-0078-3c62-d6f5-18c782eeefe1`). |
| **A06** | Selective Gated | **PASS** | `2026-11-17T22:20:37Z` | 3 active conversations (`conv_sow_a/b/c_1794954032`); Barge In on Conv B by Subject Line held 2 actions (`act_8204b403bd08`, `act_4eda0465e3b2`) while Conv C completed Windmill job `01a1eb99-03dc-3a18-d9a8-c286ab3f0233`; per-conversation idempotency isolation verified. |
| **A07** | Ungate / no duplicate | **PASS** | `2026-11-17T22:20:51Z` | Released 1 (`exec=62`), cancelled 1 (`act_4eda0465e3b2`), confirmed `UNGATED`, retry of cancelled key returned clean `CANCELLED` replay. |
| **A08** | Race and Barge In | **PASS** | `2026-11-17T22:20:52Z` | Concurrent real Windmill dispatch + Barge In on `conv_sow_race_1794954032`: waited for `act_8f1ec2172545` (`DONE`), confirmed `GATED` with `undone: false`, subsequent action held (`HELD`). |
| **A09** | Authorization | **BLOCKED** | `2026-11-17T22:20:52Z` | **Local fail-closed verification: PASS** (missing Bearer `403`, missing callback secret `403`, stranger `403`, cross-user access `403`, cross-user Subject-Line enumeration `403`, cross-user incomplete-context DoS `403`, cross-conv Windmill job IDOR `404`, revoked actor `403`, unlisted recipient `403`, owner incomplete context `401`). Live SAFI Cognito (`V-08`) awaiting SAFI. |
| **A10** | Restart / failure | **PASS** | `2026-11-17T22:20:50Z` | **Full 7-container Docker Compose stack restart** (`gate-adapter`, `n8n`, `calnode`, `windmill-server`, `windmill-worker`, `windmill-db`, `nats`) during held actions on `conv_sow_b_1794954032`: all services recovered healthy, `modeAfterRestart: GATED`, `heldCountAfterRestart: 2`, Calnode booking survived restart. |
| **A11** | History / Attribution | **BLOCKED** | `2026-11-17T22:20:53Z` | **Local JetStream verification: PASS** (**170 real schema-validated `ControlEvent` records** persisted in `SAFI_HISTORY`, `JETSTREAM_AUTO_CREATE_STREAM=false`, no unbounded in-memory duplicate). Binding to SAFI's production NATS cluster (`V-07`) awaiting SAFI. |
| **A12** | Independent use | **BLOCKED** | `2026-11-17T22:20:53Z` | Operator guide ready at [docs/safichat_user_guide.md](../docs/safichat_user_guide.md); awaiting independent SAFI tester execution. |
