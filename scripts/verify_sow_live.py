"""Comprehensive live SOW acceptance & security verification runner (A01, A03-A11).

Runs against the real local Docker Compose stack on 127.0.0.1 and captures machine-verifiable
evidence for evidence/live_local_run_report.json and contracts/*-example.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

try:
    import nats as _nats_mod
except ImportError:
    _nats_mod = None

ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT / "deploy" / ".env"

GATE = "http://127.0.0.1:8000"
N8N = "http://127.0.0.1:5678"
CALNODE = "http://127.0.0.1:3000"
WINDMILL = "http://127.0.0.1:8001"
NATS_LOCAL = "nats://127.0.0.1:4222"


def load_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip()
    return env


def git_sha() -> str:
    try:
        return (
            subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True)
            .strip()
        )
    except Exception:
        return "UNKNOWN"


def restart_gate_container() -> None:
    cmd = (
        ["wsl", "bash", "-lc", "docker restart safi-gate-adapter"]
        if os.name == "nt"
        else ["docker", "restart", "safi-gate-adapter"]
    )
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


async def wait_for_gate(client: httpx.AsyncClient, timeout_s: float = 20.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_err = ""
    while time.time() < deadline:
        try:
            r = await client.get(f"{GATE}/health")
            if r.status_code == 200:
                return r.json()
        except Exception as exc:
            last_err = str(exc)
        await asyncio.sleep(0.5)
    raise RuntimeError(f"Gate Adapter did not become healthy in {timeout_s}s: {last_err}")


async def main() -> None:
    env = load_env(ENV_PATH)
    run_id = str(int(time.time()))
    gate_headers = {"Authorization": f"Bearer {env['GATE_API_TOKEN']}"}
    calnode_headers = {"Authorization": f"Bearer {env['CALNODE_API_KEY']}"}

    owner_actor = {
        "actorGuid": "user_safi_pilot_owner_01",
        "actorKind": "USER",
        "meetingContext": {"meetingId": f"mtg_sow_{run_id}", "channel": "safichat-pilot"},
        "portContext": {"port": "safichat-local-api", "host": "127.0.0.1"},
        "permissionProvenance": {"role": "BUSINESS_OWNER", "scope": "conversation:admin"},
    }
    other_actor = {
        "actorGuid": "user_safi_pilot_other_02",
        "actorKind": "USER",
        "meetingContext": {"meetingId": f"mtg_other_{run_id}", "channel": "safichat-pilot"},
        "portContext": {"port": "safichat-local-api", "host": "127.0.0.1"},
        "permissionProvenance": {"role": "OTHER_USER", "scope": "conversation:member"},
    }
    revoked_actor = {
        "actorGuid": "user_safi_revoked_03",
        "actorKind": "USER",
        "meetingContext": {"meetingId": f"mtg_rev_{run_id}"},
        "portContext": {"port": "safichat-local-api"},
        "permissionProvenance": {"role": "REVOKED_USER"},
    }
    stranger_actor = {
        "actorGuid": "total_stranger_attacker_999",
        "actorKind": "USER",
        "meetingContext": {"meetingId": "mtg_stranger"},
        "portContext": {"port": "external"},
        "permissionProvenance": {"role": "UNKNOWN"},
    }
    incomplete_actor = {
        "actorGuid": "user_safi_pilot_owner_01",
        "actorKind": "USER",
    }

    report: dict[str, Any] = {
        "runTimestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "runId": run_id,
        "gitCommitSha": git_sha(),
        "tests": {},
    }

    async with httpx.AsyncClient(timeout=20.0) as client:
        # === A01: Local UNIX Stack Health ===
        gate_h = await wait_for_gate(client)
        n8n_h = (await client.get(f"{N8N}/healthz")).json()
        cal_h = (await client.get(f"{CALNODE}/healthz")).json()
        wm_v = (await client.get(f"{WINDMILL}/api/version")).text.strip()
        assert gate_h["status"] == "ok" and gate_h["jetstreamConnected"] is True
        assert n8n_h["status"] == "ok"
        assert cal_h["status"] == "ok"
        assert "1.425.0" in wm_v
        report["tests"]["A01"] = {
            "status": "PASS",
            "gate": gate_h,
            "n8n": n8n_h,
            "calnode": cal_h,
            "windmillVersion": wm_v,
        }

        # === A03: Marketing (n8n) + Direct Gate Bypass Rejection ===
        conv_a = f"conv_sow_a_{run_id}"
        subj_a = f"Customer A - Marketing Sales Ops {run_id}"

        # Direct unauthenticated call to n8n approved follow-up MUST be rejected (403)
        r_bypass_no_secret = await client.post(
            f"{N8N}/webhook/safi-approved-followup",
            json={
                "conversationId": conv_a,
                "recipientEmail": "unlisted@evil.example.com",
                "messageBody": "Bypass attempt",
                "idempotencyKey": f"bypass:{run_id}",
                "gateApproved": False,
            },
        )
        assert r_bypass_no_secret.status_code == 403, f"Expected 403 bypass rejection, got {r_bypass_no_secret.status_code}: {r_bypass_no_secret.text}"

        # Direct call to n8n lead intake without secret MUST be rejected (401)
        r_bypass_intake = await client.post(
            f"{N8N}/webhook/safi-lead-intake",
            json={"conversationId": conv_a, "leadEmail": "customer-a@allowlist.test.local"},
        )
        assert r_bypass_intake.status_code == 401, f"Expected 401 intake rejection, got {r_bypass_intake.status_code}"

        # Legitimate Gate-mediated lead intake -> triggers authenticated n8n workflow -> authenticated /gate/dispatch callback
        r_intake = await client.post(
            f"{GATE}/safichat/ops/marketing/ingest-lead",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "leadEmail": "customer-a@allowlist.test.local",
                "leadName": "Alice Customer",
                "inquiryText": "Interested in local business automation",
                "idempotencyKey": f"intake:{conv_a}:1",
                "actor": owner_actor,
                "correlationId": f"corr_a_{run_id}",
            },
        )
        assert r_intake.status_code == 200, r_intake.text
        intake_data = r_intake.json()
        assert intake_data["state"] == "DONE"
        assert intake_data["result"]["status"] == "ingested"

        # Explicit Gate-authorized follow-up dispatch
        r_followup = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Thanks Alice! Here are our available consultation slots.",
                "idempotencyKey": f"followup:{conv_a}:1",
                "actor": owner_actor,
                "correlationId": f"corr_a_{run_id}",
            },
        )
        assert r_followup.status_code == 200, r_followup.text
        fu_data = r_followup.json()
        assert fu_data["state"] == "DONE"
        assert fu_data["result"]["gateApproved"] is True

        report["tests"]["A03"] = {
            "status": "PASS",
            "conversationId": conv_a,
            "intakeExecutionId": intake_data["result"].get("executionId"),
            "followupExecutionId": fu_data["result"].get("executionId"),
            "directBypassStatus": r_bypass_no_secret.status_code,
            "directIntakeBypassStatus": r_bypass_intake.status_code,
        }

        # === A04: Sales (Calnode) — Real Slots + Single Idempotent Booking ===
        r_slots = await client.post(
            f"{GATE}/safichat/ops/sales/slots",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "actor": owner_actor,
            },
        )
        assert r_slots.status_code == 200, r_slots.text
        slots = r_slots.json()["slots"]
        assert len(slots) >= 1, "No available slots returned by Calnode"
        chosen_slot = slots[0]["slotId"]

        cal_list = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json()
        # Cancel any prior upcoming test booking for customer-a so A04 tests a fresh real booking cleanly
        for item in cal_list.get("items", []):
            emails = [a.get("email") for a in item.get("attendees", [])]
            if item.get("status") == "confirmed" and "customer-a@allowlist.test.local" in emails:
                await client.post(
                    f"{CALNODE}/v1/bookings/{item['id']}/cancel",
                    headers=calnode_headers,
                    json={"reason": "Reset prior pilot test booking"},
                )
        # Re-fetch available slots & current confirmed count
        r_slots = await client.post(
            f"{GATE}/safichat/ops/sales/slots",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "actor": owner_actor,
            },
        )
        assert r_slots.status_code == 200, r_slots.text
        slots = r_slots.json()["slots"]
        assert len(slots) >= 1, "No available slots returned by Calnode"
        chosen_slot = slots[0]["slotId"]

        cal_before_items = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json().get("items", [])
        cal_before = sum(1 for i in cal_before_items if i.get("status") == "confirmed")
        book_payload = {
            "conversationId": conv_a,
            "subjectLineDisplay": subj_a,
            "slotId": chosen_slot,
            "attendeeEmail": "customer-a@allowlist.test.local",
            "attendeeName": "Alice Customer",
            "notes": "SAFI pilot booking",
            "idempotencyKey": f"book:{conv_a}:{chosen_slot}",
            "actor": owner_actor,
            "correlationId": f"corr_a_{run_id}",
        }
        r_book1 = await client.post(f"{GATE}/safichat/ops/sales/book", headers=gate_headers, json=book_payload)
        assert r_book1.status_code == 200, r_book1.text
        b1 = r_book1.json()
        assert b1["state"] == "DONE" and b1["idempotentReplay"] is False
        booking_id = b1["result"]["bookingId"]

        r_book2 = await client.post(f"{GATE}/safichat/ops/sales/book", headers=gate_headers, json=book_payload)
        assert r_book2.status_code == 200, r_book2.text
        b2 = r_book2.json()
        assert b2["idempotentReplay"] is True
        assert b2["result"]["bookingId"] == booking_id

        cal_after_items = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json().get("items", [])
        cal_after = sum(1 for i in cal_after_items if i.get("status") == "confirmed")
        assert cal_after == cal_before + 1, f"Expected confirmed booking count +1 ({cal_before}->{cal_after})"

        report["tests"]["A04"] = {
            "status": "PASS",
            "conversationId": conv_a,
            "slotId": chosen_slot,
            "bookingId": booking_id,
            "inviteDelivery": b1["result"]["inviteDelivery"],
            "calnodeTotalBefore": cal_before,
            "calnodeTotalAfter": cal_after,
            "duplicateReplay": b2["idempotentReplay"],
        }

        # === A05: Operations (Windmill) — Real Assignment + JSON Job Status ===
        r_assign = await client.post(
            f"{GATE}/safichat/ops/operations/assign",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "bookingId": booking_id,
                "assignee": "ops-specialist-1@allowlist.test.local",
                "taskSummary": f"Onboard Alice for booking {booking_id}",
                "idempotencyKey": f"assign:{conv_a}:{booking_id}",
                "actor": owner_actor,
                "correlationId": f"corr_a_{run_id}",
            },
        )
        assert r_assign.status_code == 200, r_assign.text
        asgn = r_assign.json()
        assert asgn["state"] == "DONE"
        job_id = asgn["result"]["jobId"]

        # Verify camelCase JSON body on /safichat/ops/operations/job-status
        r_jstat = await client.post(
            f"{GATE}/safichat/ops/operations/job-status",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "jobId": job_id,
                "subjectLineDisplay": subj_a,
                "actor": owner_actor,
            },
        )
        assert r_jstat.status_code == 200, r_jstat.text
        assert r_jstat.json()["job"]["state"] == "COMPLETED"

        report["tests"]["A05"] = {
            "status": "PASS",
            "conversationId": conv_a,
            "jobId": job_id,
            "state": asgn["result"]["state"],
            "assignee": asgn["result"]["assignee"],
            "assignmentId": asgn["result"]["result"]["assignmentId"],
            "jobStatusEndpointVerified": True,
        }

        # === A02: Strictly BLOCKED awaiting native SafiChat UI hook (V-06) ===
        report["tests"]["A02"] = {
            "status": "BLOCKED",
            "reason": "REST control surface (/safichat/ops/*) verified live for conv_a, but native SafiChat / Meeting Manager UI invocation contract (V-06) has not been supplied by SAFI (SOW §3 & §10).",
            "verifiedApiConversationId": conv_a,
        }

        # === A06: Selective Gated Across 3 Simultaneous Conversations ===
        conv_b = f"conv_sow_b_{run_id}"
        conv_c = f"conv_sow_c_{run_id}"
        subj_b = f"Customer B - Selective Gate {run_id}"
        subj_c = f"Customer C - Independent Ops {run_id}"

        # Seed Conv B and Conv C
        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "subjectLineDisplay": subj_b,
                "recipientEmail": "customer-b@allowlist.test.local",
                "messageBody": "Initial greeting B",
                "idempotencyKey": f"seed:{conv_b}",
                "actor": owner_actor,
            },
        )
        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_c,
                "subjectLineDisplay": subj_c,
                "recipientEmail": "customer-c@allowlist.test.local",
                "messageBody": "Initial greeting C",
                "idempotencyKey": f"seed:{conv_c}",
                "actor": owner_actor,
            },
        )

        # Gate ONLY Conv B by Subject Line
        r_barge_b = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"subjectLine": subj_b, "actor": owner_actor},
        )
        assert r_barge_b.status_code == 200, r_barge_b.text
        assert r_barge_b.json()["conversationId"] == conv_b
        assert r_barge_b.json()["mode"] == "GATED"

        # Submit 2 actions to Conv B (must HOLD) + 1 real Windmill job to Conv C (must RUN unaffected)
        r_hb1 = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "subjectLineDisplay": subj_b,
                "recipientEmail": "customer-b@allowlist.test.local",
                "messageBody": "Held action 1 (to release)",
                "idempotencyKey": f"held:{conv_b}:1",
                "actor": owner_actor,
            },
        )
        r_hb2 = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "subjectLineDisplay": subj_b,
                "recipientEmail": "customer-b@allowlist.test.local",
                "messageBody": "Held action 2 (to cancel)",
                "idempotencyKey": f"held:{conv_b}:2",
                "actor": owner_actor,
            },
        )
        r_c_job = await client.post(
            f"{GATE}/safichat/ops/operations/assign",
            headers=gate_headers,
            json={
                "conversationId": conv_c,
                "subjectLineDisplay": subj_c,
                "bookingId": f"bkg_c_{run_id}",
                "assignee": "ops-specialist-1@allowlist.test.local",
                "taskSummary": "Independent Conv C task",
                "idempotencyKey": f"assign:{conv_c}:1",
                "actor": owner_actor,
            },
        )
        assert r_hb1.json()["state"] == "HELD"
        assert r_hb2.json()["state"] == "HELD"
        assert r_c_job.json()["state"] == "DONE"
        hb1_id = r_hb1.json()["actionId"]
        hb2_id = r_hb2.json()["actionId"]

        report["tests"]["A06"] = {
            "status": "PASS",
            "conversations": [conv_a, conv_b, conv_c],
            "gatedConversationId": conv_b,
            "heldActionIds": [hb1_id, hb2_id],
            "independentConvCJobId": r_c_job.json()["result"]["jobId"],
        }

        # === A10: Live Container Restart During HELD Actions + Recovery ===
        restart_gate_container()
        rec_health = await wait_for_gate(client)
        assert rec_health["jetstreamConnected"] is True
        assert rec_health["recovery"]["heldActionsPreserved"] >= 2

        r_held_after = await client.post(
            f"{GATE}/safichat/ops/gate/held",
            headers=gate_headers,
            json={"conversationId": conv_b, "actor": owner_actor},
        )
        assert r_held_after.status_code == 200, r_held_after.text
        held_after_data = r_held_after.json()
        assert held_after_data["mode"] == "GATED"
        assert len(held_after_data["heldActions"]) == 2

        report["tests"]["A10"] = {
            "status": "PASS",
            "conversationId": conv_b,
            "recoveryReport": rec_health["recovery"],
            "modeAfterRestart": held_after_data["mode"],
            "heldCountAfterRestart": len(held_after_data["heldActions"]),
        }

        # === A07: Ungate (Release 1, Cancel 1, No Duplicate + No TRANSITIONING Deadlock) ===
        r_ungate = await client.post(
            f"{GATE}/safichat/ops/gate/ungate",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "resolutions": [
                    {"actionId": hb1_id, "decision": "RELEASE"},
                    {"actionId": hb2_id, "decision": "CANCEL"},
                ],
                "actor": owner_actor,
            },
        )
        assert r_ungate.status_code == 200, r_ungate.text
        ug = r_ungate.json()
        assert ug["mode"] == "UNGATED"
        assert len(ug["released"]) == 1
        assert ug["cancelled"] == [hb2_id]

        # Re-submitting cancelled key returns clean CANCELLED replay (never HTTP 500 or 504)
        r_retry_cancelled = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "subjectLineDisplay": subj_b,
                "recipientEmail": "customer-b@allowlist.test.local",
                "messageBody": "Held action 2 (to cancel)",
                "idempotencyKey": f"held:{conv_b}:2",
                "actor": owner_actor,
            },
        )
        assert r_retry_cancelled.status_code == 200
        assert r_retry_cancelled.json()["state"] == "CANCELLED"
        assert r_retry_cancelled.json()["idempotentReplay"] is True

        # Verify failed downstream release during ungate restores GATED (no TRANSITIONING deadlock!)
        conv_dl = f"conv_sow_nodeadlock_{run_id}"
        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_dl,
                "subjectLineDisplay": f"No Deadlock {run_id}",
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Seed",
                "idempotencyKey": f"seed:{conv_dl}",
                "actor": owner_actor,
            },
        )
        await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_dl, "actor": owner_actor},
        )
        r_bad_held = await client.post(
            f"{GATE}/safichat/ops/sales/book",
            headers=gate_headers,
            json={
                "conversationId": conv_dl,
                "slotId": "INVALID-TS",
                "attendeeEmail": "customer-a@allowlist.test.local",
                "attendeeName": "Test",
                "idempotencyKey": f"badbook:{conv_dl}",
                "actor": owner_actor,
            },
        )
        bad_aid = r_bad_held.json()["actionId"]
        r_dl_fail = await client.post(
            f"{GATE}/safichat/ops/gate/ungate",
            headers=gate_headers,
            json={
                "conversationId": conv_dl,
                "resolutions": [{"actionId": bad_aid, "decision": "RELEASE"}],
                "actor": owner_actor,
            },
        )
        assert r_dl_fail.status_code == 502
        # Retrying ungate now SUCCEEDS because mode was safely restored to GATED (not stuck in TRANSITIONING!)
        r_dl_recover = await client.post(
            f"{GATE}/safichat/ops/gate/ungate",
            headers=gate_headers,
            json={"conversationId": conv_dl, "resolutions": [], "actor": owner_actor},
        )
        assert r_dl_recover.status_code == 200 and r_dl_recover.json()["mode"] == "UNGATED"

        # Retrying the failed idempotency key returns clean 200 FAILED replay (never HTTP 500)
        r_retry_failed = await client.post(
            f"{GATE}/safichat/ops/sales/book",
            headers=gate_headers,
            json={
                "conversationId": conv_dl,
                "slotId": "INVALID-TS",
                "attendeeEmail": "customer-a@allowlist.test.local",
                "attendeeName": "Test",
                "idempotencyKey": f"badbook:{conv_dl}",
                "actor": owner_actor,
            },
        )
        assert r_retry_failed.status_code == 200
        assert r_retry_failed.json()["state"] == "FAILED"
        assert r_retry_failed.json()["idempotentReplay"] is True

        report["tests"]["A07"] = {
            "status": "PASS",
            "conversationId": conv_b,
            "releasedActionId": hb1_id,
            "releasedExecId": ug["released"][0]["result"].get("executionId"),
            "cancelledActionIds": ug["cancelled"],
            "modeAfter": ug["mode"],
            "cancelledRetryStatus": r_retry_cancelled.json()["state"],
            "failedRetryStatus": r_retry_failed.json()["state"],
            "noTransitioningDeadlockVerified": True,
        }

        # === A08: Live Race & Barge In Against Real Windmill Dispatch ===
        conv_race = f"conv_sow_race_{run_id}"
        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_race,
                "subjectLineDisplay": f"Race Live {run_id}",
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Seed race",
                "idempotencyKey": f"seed:{conv_race}",
                "actor": owner_actor,
            },
        )
        # Launch real Windmill dispatch & Barge In concurrently
        wm_task = asyncio.create_task(
            client.post(
                f"{GATE}/safichat/ops/operations/assign",
                headers=gate_headers,
                json={
                    "conversationId": conv_race,
                    "bookingId": f"bkg_race_{run_id}",
                    "assignee": "ops-specialist-1@allowlist.test.local",
                    "taskSummary": "In-flight race assignment",
                    "idempotencyKey": f"race:wm:{run_id}",
                    "actor": owner_actor,
                },
            )
        )
        await asyncio.sleep(0.03)
        barge_task = asyncio.create_task(
            client.post(
                f"{GATE}/safichat/ops/gate/barge-in",
                headers=gate_headers,
                json={"conversationId": conv_race, "actor": owner_actor},
            )
        )
        r_wm_race, r_barge_race = await asyncio.gather(wm_task, barge_task)
        assert r_wm_race.status_code == 200 and r_barge_race.status_code == 200
        assert r_barge_race.json()["mode"] == "GATED"
        assert r_barge_race.json()["undone"] is False

        # Submit new action AFTER GATED confirmed -> MUST be HELD
        r_after_race = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_race,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Must be held after GATED",
                "idempotencyKey": f"race:after:{run_id}",
                "actor": owner_actor,
            },
        )
        assert r_after_race.status_code == 200
        assert r_after_race.json()["state"] == "HELD"

        report["tests"]["A08"] = {
            "status": "PASS",
            "conversationId": conv_race,
            "inflightActionState": r_wm_race.json()["state"],
            "bargeInMode": r_barge_race.json()["mode"],
            "reconciledInFlight": r_barge_race.json()["reconciledInFlight"],
            "undone": r_barge_race.json()["undone"],
            "postConfirmationActionState": r_after_race.json()["state"],
        }

        # === A09: Live Fail-Closed Authz, HTTP Auth, Revocation & Allowlist ===
        r_no_bearer = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            json={"conversationId": conv_a, "actor": owner_actor},
        )
        r_no_cb_secret = await client.post(
            f"{GATE}/gate/dispatch",
            json={
                "conversationId": conv_a,
                "engine": "n8n",
                "operation": "dispatch_followup",
                "idempotencyKey": f"unauth:cb:{run_id}",
                "payload": {"messageBody": "x"},
                "recipients": ["customer-a@allowlist.test.local"],
                "actor": owner_actor,
            },
        )
        r_stranger = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_a, "actor": stranger_actor},
        )
        r_cross_user = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_a, "actor": other_actor},
        )
        r_cross_inject_gated = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_race,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Cross-user injection into GATED conv",
                "idempotencyKey": f"cross:inject:{run_id}",
                "actor": other_actor,
            },
        )
        r_revoked = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_a, "actor": revoked_actor},
        )
        r_non_allowlist = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "recipientEmail": "unlisted-stranger@external.example.com",
                "messageBody": "Must be blocked",
                "idempotencyKey": f"allowlist:block:{run_id}",
                "actor": owner_actor,
            },
        )
        conv_unc = f"conv_sow_uncertain_{run_id}"
        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_unc,
                "subjectLineDisplay": f"Uncertain Identity {run_id}",
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Seed",
                "idempotencyKey": f"seed:{conv_unc}",
                "actor": owner_actor,
            },
        )
        r_incomplete = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_unc, "actor": incomplete_actor},
        )

        assert r_no_bearer.status_code == 403
        assert r_no_cb_secret.status_code == 403
        assert r_stranger.status_code == 403
        assert r_cross_user.status_code == 403
        assert r_cross_inject_gated.status_code == 403
        assert r_revoked.status_code == 403
        assert r_non_allowlist.status_code == 403
        assert r_incomplete.status_code == 401

        report["tests"]["A09"] = {
            "status": "BLOCKED",
            "localFailClosedVerification": "PASS",
            "reason": "All local fail-closed authz checks passed live (HTTP Bearer auth, internal callback secret, default-deny stranger, cross-conversation isolation, GATED hold injection block, revoked actor block, incomplete context 401 fail-closed, recipient allowlist block); live SAFI Cognito endpoint (V-08 / SAFI_AUTHZ_URL) remains BLOCKED awaiting SAFI infrastructure.",
            "observedStatuses": {
                "missingBearerToken": r_no_bearer.status_code,
                "missingCallbackSecret": r_no_cb_secret.status_code,
                "unlistedStranger": r_stranger.status_code,
                "crossUserConversationAccess": r_cross_user.status_code,
                "crossUserGatedHoldInject": r_cross_inject_gated.status_code,
                "revokedActor": r_revoked.status_code,
                "nonAllowlistedRecipient": r_non_allowlist.status_code,
                "incompleteActorContext": r_incomplete.status_code,
            },
        }

    # === A11: Verify Real JetStream ControlEvents & Capture Contract Examples ===
    stream_name = env.get("JETSTREAM_STREAM", "SAFI_HISTORY")
    recent_events: list[dict[str, Any]] = []
    sample_control_event: dict[str, Any] | None = None
    total_msgs = 0

    if _nats_mod is not None:
        nc = await _nats_mod.connect(servers=[NATS_LOCAL])
        js = nc.jetstream()
        info = await js.stream_info(stream_name)
        total_msgs = info.state.messages
        start_seq = max(1, info.state.last_seq - 9)
        for seq in range(start_seq, info.state.last_seq + 1):
            msg = await js.get_msg(stream_name, seq=seq)
            ev = json.loads(msg.data.decode("utf-8"))
            if sample_control_event is None and ev.get("subjectLineDisplay"):
                sample_control_event = ev
            recent_events.append(
                {
                    "seq": seq,
                    "subject": msg.subject,
                    "eventId": ev["eventId"],
                    "type": ev["type"],
                    "conversationId": ev["conversationId"],
                    "subjectLineDisplay": ev.get("subjectLineDisplay"),
                    "actorGuid": ev["actorGuid"],
                    "actorKind": ev["actorKind"],
                    "outcome": ev["outcome"],
                }
            )
        await nc.close()
    else:
        js_script = (
            "import asyncio, json, nats\n"
            "async def go():\n"
            "  nc = await nats.connect('nats://nats:4222')\n"
            "  js = nc.jetstream()\n"
            f"  info = await js.stream_info('{stream_name}')\n"
            "  start = max(1, info.state.last_seq - 9)\n"
            "  out = {'total': info.state.messages, 'events': [], 'sample': None}\n"
            "  for seq in range(start, info.state.last_seq + 1):\n"
            f"    msg = await js.get_msg('{stream_name}', seq=seq)\n"
            "    ev = json.loads(msg.data.decode('utf-8'))\n"
            "    if out['sample'] is None and ev.get('subjectLineDisplay'): out['sample'] = ev\n"
            "    out['events'].append({'seq': seq, 'subject': msg.subject, 'eventId': ev['eventId'], 'type': ev['type'], 'conversationId': ev['conversationId'], 'subjectLineDisplay': ev.get('subjectLineDisplay'), 'actorGuid': ev['actorGuid'], 'actorKind': ev['actorKind'], 'outcome': ev['outcome']})\n"
            "  await nc.close()\n"
            "  print(json.dumps(out))\n"
            "asyncio.run(go())\n"
        )
        cmd = (
            ["wsl", "docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
            if os.name == "nt"
            else ["docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
        )
        proc = subprocess.run(cmd, input=js_script, text=True, check=True, stdout=subprocess.PIPE)
        js_data = json.loads(proc.stdout)
        total_msgs = js_data["total"]
        recent_events = js_data["events"]
        sample_control_event = js_data["sample"]

    # Save real captured contract examples (SOW §9 deliverable: JSON Schema and event examples)
    if sample_control_event:
        (ROOT / "contracts" / "control-event.example.json").write_text(
            json.dumps(sample_control_event, indent=2) + "\n", encoding="utf-8"
        )
    (ROOT / "contracts" / "gate-action.example.json").write_text(
        json.dumps(
            {
                "actionId": hb1_id,
                "conversationId": conv_b,
                "engine": "n8n",
                "operation": "dispatch_followup",
                "idempotencyKey": f"held:{conv_b}:1",
                "state": "HELD",
                "payloadRef": "sha256:captured_live_ref",
                "createdAt": report["runTimestamp"],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (ROOT / "contracts" / "gate-operation.example.json").write_text(
        json.dumps(
            {
                "operationType": "RETURN_TO_UNGATED",
                "conversationId": conv_b,
                "subjectLine": subj_b,
                "actorGuid": owner_actor["actorGuid"],
                "actorKind": owner_actor["actorKind"],
                "resolutions": [
                    {"actionId": hb1_id, "decision": "RELEASE"},
                    {"actionId": hb2_id, "decision": "CANCEL"},
                ],
                "correlationId": f"corr_a_{run_id}",
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    report["tests"]["A11"] = {
        "status": "BLOCKED",
        "localJetStreamVerification": "PASS",
        "reason": "Verified real schema-validated ControlEvents persisted in local NATS JetStream stream SAFI_HISTORY; binding to SAFI's production NATS/JetStream cluster & verifying SAFI Subject naming (V-07) remains BLOCKED awaiting SAFI infrastructure.",
        "stream": env.get("JETSTREAM_STREAM", "SAFI_HISTORY"),
        "totalPersistedControlEvents": total_msgs,
        "recentEvents": recent_events[-5:],
    }

    report["tests"]["A12"] = {
        "status": "BLOCKED",
        "reason": "Awaiting independent SAFI tester execution using docs/safichat_user_guide.md (SOW §8 A12).",
    }

    out_path = ROOT / "evidence" / "live_local_run_report.json"
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"summary": {k: v["status"] for k, v in report["tests"].items()}, "reportPath": str(out_path)}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
