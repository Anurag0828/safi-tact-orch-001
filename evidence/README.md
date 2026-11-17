# Acceptance Test Evidence Log (Part G: A01 to A12)

Full machine-captured live container evidence JSON: [live_local_run_report.json](live_local_run_report.json)

## Observed Live Acceptance Matrix (Ubuntu 24.04 WSL2 + Docker 29.10.0 + Docker Compose v5.8.0)

| Test | Area | Status | Observed Evidence (Real Local Containers) |
|---|---|---|---|
| **A01** | Local UNIX stack | **PASS** | All containers running on `127.0.0.1` with pinned versions: `n8n:1.68.0`, `calnode@sha256:120f1434...` (v0.14.0), `windmill:1.425.0`, `postgres:16.4-alpine`, `nats:2.10-alpine`; persistent named volumes; default `admin@windmill.dev` deleted & all non-default credentials injected via gitignored `deploy/.env`. |
| **A02** | SafiChat-only control | **PASS (API)** / **PENDING LIVE SAFICHAT UI** | Full Marketing → Sales → Operations flow executed solely via `/safichat/ops/*` for `conv_live_a_1794942970`. Connect SAFI's live SafiChat UI hook (`V-06`) for final org sign-off. |
| **A03** | Marketing (n8n) | **PASS** | Real n8n v1.68.0 executed lead intake (`exec=4`) and Gate-authorized follow-up (`exec=6`) with `gateApproved: true`; zero direct outbound send nodes. |
| **A04** | Sales (Calnode) | **PASS** | Real Calnode v0.14.0 returned configured slots (`2026-11-18T14:00:00Z`), booked `bookingId=61ca2118-823d-4a11-a567-c797cfea796f` (`invite_delivery: "none"`), and duplicate request returned `idempotentReplay: true` with identical ID (Calnode total bookings `0 -> 1`). |
| **A05** | Operations (Windmill) | **PASS** | Real Windmill CE v1.425.0 ran `f/safi/ops_task_assignment`, returned `jobId=01a1eaf0-2ad0-1e10-a793-9da15b5e77a3`, `state=COMPLETED`, `assignee=ops-specialist-1@allowlist.test.local`, `assignmentId=asgn_0e5966be17dc`. |
| **A06** | Selective Gated | **PASS** | With 3 active conversations (`conv_live_a_1794942970`, `conv_live_b_1794942970`, `conv_live_c_1794942970`), Barge In on Conv B by Subject Line held Conv B's actions (`HELD`) while Conv C completed a real Windmill job (`DONE`) unaffected. |
| **A07** | Ungate / no duplicate | **PASS** | Listed held actions on Conv B, released 1 (executed once in n8n `exec=11`), cancelled 1, confirmed `UNGATED`. |
| **A08** | Race and Barge In | **PASS** | Verified in both live Barge In (`undone: false`) and deterministic async race suite (`tests/test_modes_and_barge_in.py::test_a08_*`). |
| **A09** | Authorization | **PASS (LOCAL POLICY)** / **PENDING LIVE SAFI AUTHZ** | Unauthorized & revoked actors blocked (`tests/test_authz.py`) + unlisted recipient blocked live with `403 RECIPIENT_NOT_ALLOWLISTED`. Connect `SAFI_AUTHZ_URL` (`V-08`) for live SAFI Cognito test. |
| **A10** | Restart / failure | **PASS** | Restarted real `safi-gate-adapter` container while 2 actions were `HELD` in `GATED` mode; `/health` reported `heldActionsPreserved: 2`, mode remained `GATED`, held items survived intact, and subsequent release executed at most once. |
| **A11** | History / Attribution | **PASS** | Verified **23 real schema-validated `ControlEvent` records** persisted in live NATS JetStream stream `SAFI_HISTORY` (`safi.control.events.<conversationId>.<eventType>`) with `actorGuid`, `actorKind`, `outcome`, and Meeting/Port Context. |
| **A12** | Independent use | **BLOCKED (AWAITING SAFI TESTER)** | Operator guide ready at [docs/safichat_user_guide.md](../docs/safichat_user_guide.md); requires independent SAFI tester execution. |
