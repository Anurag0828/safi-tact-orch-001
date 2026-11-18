# Acceptance Test Evidence Log (Part G: A01 to A12)

- **Tested Code Commit SHA (`TESTED_SHA`):** `11a31766f79c035d1a115d5f0b38ad4e92924753`
- **Verification Run ID:** `20261118T081009Z`
- **Automated Regression Suite:** `73 passed, 0 failed, 0 skipped, 0 deselected` across **3 consecutive runs** (`--timeout=60`, `83%` coverage across `gate/`, `adapters/`, `safichat/`)
- **Independent Verification Package:** [verification/20261118T081009Z/REPORT.md](verification/20261118T081009Z/REPORT.md) | [RESULTS.json](verification/20261118T081009Z/RESULTS.json) | [00_manifest.json](verification/20261118T081009Z/00_manifest.json) | [SHA256SUMS](verification/20261118T081009Z/SHA256SUMS)
- **Redacted Stack Logs:** [verification/20261118T081009Z/04_stack/logs_redacted.txt](verification/20261118T081009Z/04_stack/logs_redacted.txt) (`0` real `.env` secret hits verified)

> **Strict SOW §8 / §10 Compliance:** Tests depending on SAFI-owned infrastructure (`A02` live SafiChat UI, `A09` live SAFI Cognito authz, `A11` SAFI's production NATS cluster, `A12` independent SAFI tester) are explicitly recorded as **BLOCKED** — never misrepresented as PASS. Every PASS below was cross-checked against independent downstream oracles (`verification/20261118T081009Z/06_independent_oracles/`).

## Observed Live Acceptance Matrix (Ubuntu 24.04 WSL2 + Docker 29.10.0 + Compose v5.8.0)

| Test | Area | Verdict | Observed Evidence & Independent Oracle Proof (`TESTED_SHA=11a31766...`) |
|---|---|---|---|
| **A01** | Local UNIX stack | **PASS** | All 7 containers healthy on `127.0.0.1` (`0` bindings on `0.0.0.0`), exact tag+`@sha256:` pins, non-root `USER safi` + `read_only: true` + `no-new-privileges:true`, named volumes, default credentials rejected (`04_stack/`). |
| **A02** | SafiChat-only control | **BLOCKED** | Awaiting SAFI's native SafiChat / Meeting Manager UI hook (`V-06`). REST surrogate (`/safichat/ops/*`) verified across A03–A08. |
| **A03** | Marketing (n8n) | **PASS** | n8n oracle confirmed `+2` (`exec=269` ingest, `exec=270` Gate-mediated follow-up); direct no-secret/wrong-secret/`gateApproved=false` returned `403`; non-allowlisted `403`; duplicate key replayed with `0` extra executions. |
| **A04** | Sales (Calnode) | **PASS** | Slots matched Calnode direct API 100%; booking `fe4767e4-a8af-4aad-8f21-3746e4444b31` (`invite_delivery: "none"`, `EMAIL_SMTP_HOST=""`); same-key replay & different-key same-slot rejection kept Calnode confirmed delta = `+1`; out-of-bounds `calendarId` `403`. |
| **A05** | Operations (Windmill) | **PASS** | Operator `u/safi_ops_runner` (`super_admin: false`); job `01a1edb7-0f86-e943-691e-167adfbe3852` matched Windmill direct API; forced failure `01a1edb7-1129-2132-ce0f-86254404355e` surfaced `502`; disallowed `scriptPath` `403`; cross-conversation IDOR `404`. |
| **A06** | Selective Gated | **PASS** | Conv B gated by Subject Line (`0` downstream effects) while Conv A (`+1` n8n) & Conv C (`+1` Windmill) ran concurrently; `409 AMBIGUOUS_SUBJECT_LINE` + `conversationId` selection verified; wildcard Subject Line `'foo.> a.b.*'` left NATS subjects untouched. |
| **A07** | Ungate / no duplicate | **PASS** | 4 held actions resolved (1 approve, 1 reject, 1 release, 1 cancel -> exact `+2` n8n oracle delta); 12 retries (3× each key) produced `0` extra executions; post-ungate new action executed immediately (`+1`). |
| **A08** | Race and Barge In | **PASS** | **20/20** jittered race iterations passed (**19** genuine mid-dispatch overlaps reconciled with `undone: false`, `0` post-confirmation downstream effects) (`05_live/A08/A08_iterations.json`). |
| **A09** | Authorization | **BLOCKED** | **Local fail-closed suite: PASS** (17/17 live denials with `0` oracle delta across all 3 engines + D4 live-style `AUTHZ_MODE=http` mock SAFI authz 7/7 PASS including mid-flow revocation & pre-dispatch recheck timestamps). Live SAFI Cognito (`V-08`) awaiting SAFI. |
| **A10** | Restart / failure | **PASS** | All 5 variants passed: (1) full compose restart preserves `GATED` + 2 `HELD` (`0` effects), (2) `docker kill -s KILL` mid-dispatch (`delta=1 <= 1`, no blind re-run), (3) live mid-run NATS outage (`delta=0`, `FAILED_CLOSED`, recovered on return), (4) corrupt SQLite fails closed with `StoreError`, (5) Calnode booking survives & replays (`0` duplicates). |
| **A11** | History / Attribution | **BLOCKED** | **Local JetStream: PASS** (`132/132` 100% schema-valid `ControlEvent` records in `SAFI_HISTORY`, all lifecycle types, PROXY preserved original `portContext`, `0` pre-existing subject mutations). SAFI production NATS cluster (`V-07`) awaiting SAFI. |
| **A12** | Independent use | **BLOCKED** | Operator guide verified via cold-start `A12-DRILL` (`05_live/A12/A12_DRILL.json`); awaiting independent SAFI tester execution. |
