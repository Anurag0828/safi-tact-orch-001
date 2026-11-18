# SAFI-TACT-ORCH-001 — Independent Verification & Evidence Report

- **Run ID:** `20261118T081009Z`
- **Baseline SHA (before fixes):** `476fe683941528b79bfed7a9d25306e0cb83eee5`
- **Tested Code SHA (`TESTED_SHA`):** `11a31766f79c035d1a115d5f0b38ad4e92924753`
- **Generated (UTC):** `2026-11-18T08:13:17.926693Z`
- **Host:** `Linux 6.18.58.1-microsoft-standard-WSL2` | Docker `29.10.0` | Compose `5.8.0` | `Python 3.12.3`

## 1. Defect Resolution Summary (Part 2)
| Defect | Status | Evidence |
|---|---|---|
| **D1** (JetStream fail-closed hang & mid-run outage) | **FIXED** | Reproduced 30s hang (`01_defects/D1_before.txt`); fixed with `allow_reconnect=False`, `max_reconnect_attempts=0`, `connect_timeout=3s`, total budget `<=12s`, pre-dispatch `ensure_available()`, and never silently `DONE` without audit (`01_defects/D1_after.txt`, `D1_diff.patch`, `D1_midrun_outage.txt`). |
| **D2** (Evidence SHA parity) | **FIXED** | All tests ran on clean `TESTED_SHA=11a31766f79c035d1a115d5f0b38ad4e92924753`; evidence committed separately touching ONLY `evidence/` and `docs/` (`01_defects/D2_diff_proof.txt`). |
| **D3** (Proprietary SOW in repo) | **FIXED** | Scrubbed proprietary SOW `.docx` & internal `SAFI_BUILD_SPEC.md` from all Git commits and added to `.gitignore` (`01_defects/D3_history_scrub_proof.txt`, `D3_repo_visibility.json`) so public repo `Anurag0828/safi-tact-orch-001` contains strictly SOW deliverables only. |
| **D4** (`AUTHZ_MODE=http` live-style test) | **FIXED** | Real local HTTP mock SAFI authz server verified all 7 cases (unset URL, allow, deny, timeout -> `FAILED_CLOSED`, 500/malformed -> `FAILED_CLOSED`, mid-flow revocation on held action, pre-dispatch recheck timestamp ordering) (`01_defects/D4_authz_http_pytest.txt`, `D4_authz_http_mock_log.json`). |
| **D5** (Doc & harness honesty pass) | **FIXED** | Fixed `X-SAFI-Gate-Secret` doc/harness mismatch (`N8N_WEBHOOK_SECRET`), added `:0.14.0` tag to Calnode digest pin, and added non-root `read_only: true` + `no-new-privileges:true` on `gate-adapter`. |

## 2. Static Audit (Part 3 — S1 to S15)
All **15/15** checks **PASS** (`02_static/STATIC_RESULTS.json`).

## 3. Automated Test Suite (Part 4 — 3× Repeatability + Coverage)
- **Runs:** 3 consecutive full runs (`--timeout=60`, JUnit XML captured)
- **Result per run:** `73 passed, 0 failed, 0 skipped, 0 deselected` (`03_pytest/run1..3.xml`)
- **Coverage:** `83%` overall across `gate/`, `adapters/`, `safichat/` (`03_pytest/coverage.txt`)

| Test File | SOW Acceptance Mapping |
|---|---|
| `tests/test_adapters.py` | A03–A05 adapters, D1 JetStream fail-closed & mid-run outage, A10.4 corrupt SQLite |
| `tests/test_authz.py`, `tests/test_authz_http_live.py` | A09 fail-closed authz & revocation, D4 live-style `AUTHZ_MODE=http` |
| `tests/test_contracts.py` | Part D strict Draft 2020-12 JSON Schemas & A11 event contract validation |
| `tests/test_gate_dispatcher.py` | R-GATE-01/09, R-SEC-01 allowlist, E5 invariants 1, 2, 4, 8 |
| `tests/test_modes_and_barge_in.py` | A06 selective gating, A07 ungate/no duplicates, A08 race/barge-in, A10 restart recovery |
| `tests/test_security_and_edge_cases.py` | Anti-DoS, IDOR, SQLite concurrency, TOCTOU approve race, script/calendar scope guards |
| `tests/test_e2e_safichat.py`, `test_state_machine.py`, `test_store.py` | A02 REST control surface, E2/E3/E4 state machine, R-CTX-03 store |

## 4. Live Acceptance Tests & Independent Oracle Summary (Parts 5–7)
| Test | Verdict | Independent Oracle Proof |
|---|---|---|
| **A01** | **PASS** | 7/7 containers healthy, exact tag+digest pins, `127.0.0.1` only, `read_only:true`, default creds rejected. |
| **A02** | **BLOCKED** | Awaiting SAFI native SafiChat UI hook (`V-06`); REST surrogate verified across A03–A08. |
| **A03** | **PASS** | n8n oracle confirmed `+2` (ingest `269` + follow-up `270`), `0` on non-allowlisted & replay; direct bypasses `403`. |
| **A04** | **PASS** | Slots matched Calnode direct API; booking `fe4767e4-a8af-4aad-8f21-3746e4444b31` (`invite_delivery=none`); same-key replay & diff-key same-slot rejection kept oracle delta = `+1`. |
| **A05** | **PASS** | `super_admin: false` (`u/safi_ops_runner`); job `01a1edb7-0f86-e943-691e-167adfbe3852` matched Windmill API; forced failure `01a1edb7-1129-2132-ce0f-86254404355e` surfaced `502`; scriptPath escape `403`; IDOR `404`. |
| **A06** | **PASS** | Conv B gated by Subject Line (`0` effects) while Conv A (`+1` n8n) & Conv C (`+1` Windmill) ran concurrently; `409` ambiguity & wildcard Subject Line `'foo.> a.b.*'` verified. |
| **A07** | **PASS** | 4 held actions -> approve (`+1`), reject (`0`), release (`+1`), cancel (`0`); 12 retries (3× each) -> `0` oracle delta; post-ungate action -> `+1`. |
| **A08** | **PASS** | **20/20** race iterations passed (`19` mid-dispatch overlaps with `undone: false`, `0` post-confirmation effects) (`05_live/A08/A08_iterations.json`). |
| **A09** | **BLOCKED** | **Local fail-closed suite: PASS** (17/17 live denials with `0` oracle delta across all 3 engines + D4 HTTP authz PASS); live SAFI Cognito (`V-08`) awaiting SAFI. |
| **A10** | **PASS** | All 5 variants passed: full compose restart (`0` effects, 2 held preserved), `kill -9` mid-dispatch (`delta=1 <= 1`), mid-run NATS outage (`delta=0`, `FAILED_CLOSED`, recovered), corrupt SQLite fail-closed, Calnode booking survived & replayed. |
| **A11** | **BLOCKED** | **Local JetStream: PASS** (`132/132` 100% schema-valid, all lifecycle types, PROXY preserved original `portContext`, `0` pre-existing subject mutations); SAFI production NATS (`V-07`) awaiting SAFI. |
| **A12** | **BLOCKED** | Awaiting independent SAFI tester (`05_live/A12/A12_DRILL.json` cold-start drill recorded). |

## 5. Findings
- **F1 (MED — FIXED):** D3 — With owner approval to keep Anurag0828/safi-tact-orch-001 PUBLIC for stakeholder review, proprietary SOW '.docx' and internal 'SAFI_BUILD_SPEC.md' were scrubbed from ALL Git commits via git filter-branch and excluded via .gitignore (0 occurrences in working tree or Git history).
- **F2 (LOW — FIXED):** D5 — Fixed mismatch in deploy/README.md and scripts/verify_sow_live.py where X-SAFI-Gate-Secret previously referenced GATE_API_TOKEN instead of N8N_WEBHOOK_SECRET (matching gate/app.py), and added explicit :0.14.0 tag to Calnode digest pin.
