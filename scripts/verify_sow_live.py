"""Comprehensive live SOW acceptance & security verification runner (A01, A03-A11).

Runs against the real local Docker Compose stack on 127.0.0.1 and captures machine-verifiable
evidence (per-test timestamps, real SQLite/JetStream/engine payloads, redacted logs) into
evidence/live_local_run_report.json, evidence/redacted_stack_logs.txt, and contracts/*.example.json.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
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


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _posix_repo_root() -> str:
    """Computes the WSL/POSIX repo path dynamically from ROOT (no hardcoded mount path)."""
    if os.name == "nt":
        drive = ROOT.drive.rstrip(":").lower()
        rest = ROOT.as_posix().split(":", 1)[1]
        return f"/mnt/{drive}{rest}"
    return ROOT.as_posix()


def _wsl_cmd(bash_script: str) -> list[str]:
    if os.name == "nt":
        return ["wsl", "-d", "Ubuntu-24.04", "-u", "root", "bash", "-lc", bash_script]
    return ["bash", "-lc", bash_script]


def restart_full_local_stack() -> None:
    """Restarts the ENTIRE local Docker Compose stack (gate, n8n, calnode, windmill, nats) for A10."""
    repo_posix = _posix_repo_root()
    cmd = _wsl_cmd(
        f"cd '{repo_posix}' && docker compose -f deploy/docker-compose.yml --env-file deploy/.env --profile local-nats restart"
    )
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def fetch_real_action_from_sqlite(action_id: str) -> dict[str, Any]:
    """Reads the ACTUAL GateAction row directly from safi-gate-adapter SQLite operational store."""
    py = (
        "import json, sqlite3\n"
        "conn = sqlite3.connect('/app/data/gate_state.db')\n"
        "conn.row_factory = sqlite3.Row\n"
        f"row = conn.execute('SELECT action_id, conversation_id, engine, operation, idempotency_key, state, payload_ref, created_at FROM actions WHERE action_id = ?', ({action_id!r},)).fetchone()\n"
        "print(json.dumps({\n"
        "  'actionId': row['action_id'],\n"
        "  'conversationId': row['conversation_id'],\n"
        "  'engine': row['engine'],\n"
        "  'operation': row['operation'],\n"
        "  'idempotencyKey': row['idempotency_key'],\n"
        "  'state': row['state'],\n"
        "  'payloadRef': row['payload_ref'],\n"
        "  'createdAt': row['created_at'],\n"
        "}))\n"
    )
    cmd = (
        ["wsl", "-d", "Ubuntu-24.04", "-u", "root", "docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
        if os.name == "nt"
        else ["docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
    )
    proc = subprocess.run(cmd, input=py, text=True, check=True, stdout=subprocess.PIPE)
    return json.loads(proc.stdout)


def capture_redacted_logs(env: dict[str, str]) -> str:
    """Captures recent logs from all local stack containers and redacts every secret/token."""
    services = [
        "safi-gate-adapter",
        "safi-n8n",
        "safi-calnode",
        "safi-windmill-server",
        "safi-local-nats",
    ]
    parts: list[str] = [f"# Redacted Stack Logs Captured {utc_now()} (Git SHA: {git_sha()})\n"]
    secrets_to_mask = [
        v
        for k, v in env.items()
        if any(tok in k.upper() for tok in ("TOKEN", "SECRET", "PASSWORD", "KEY"))
        and len(v) >= 6
    ]
    for svc in services:
        cmd = (
            ["wsl", "-d", "Ubuntu-24.04", "-u", "root", "docker", "logs", "--tail", "25", svc]
            if os.name == "nt"
            else ["docker", "logs", "--tail", "25", svc]
        )
        proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        text = proc.stdout
        for secret in secrets_to_mask:
            text = text.replace(secret, "[REDACTED]")
        text = re.sub(r"Bearer\s+[A-Za-z0-9._-]+", "Bearer [REDACTED]", text)
        parts.append(f"\n=== {svc} ===\n{text.strip()}\n")
    return "\n".join(parts)


async def wait_for_full_stack(client: httpx.AsyncClient, timeout_s: float = 45.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last_err = ""
    while time.time() < deadline:
        try:
            g = await client.get(f"{GATE}/health")
            n = await client.get(f"{N8N}/healthz")
            c = await client.get(f"{CALNODE}/healthz")
            w = await client.get(f"{WINDMILL}/api/version")
            jsz = await client.get("http://127.0.0.1:8222/jsz")
            if (
                g.status_code == 200
                and n.status_code == 200
                and c.status_code == 200
                and w.status_code == 200
                and jsz.status_code == 200
            ):
                return {
                    "gate": g.json(),
                    "n8n": n.json(),
                    "calnode": c.json(),
                    "windmillVersion": w.text.strip(),
                    "natsJetStream": True,
                }
        except Exception as exc:
            last_err = str(exc)
        await asyncio.sleep(1.0)
    raise RuntimeError(f"Full local stack did not become healthy in {timeout_s}s: {last_err}")


async def main() -> None:
    env = load_env(ENV_PATH)
    run_id = str(int(time.time()))
    gate_headers = {"Authorization": f"Bearer {env['GATE_API_TOKEN']}"}
    calnode_headers = {"Authorization": f"Bearer {env['CALNODE_API_KEY']}"}
    wm_headers = {"Authorization": f"Bearer {env['WINDMILL_TOKEN']}"}

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
    owner_incomplete_actor = {
        "actorGuid": "user_safi_pilot_owner_01",
        "actorKind": "USER",
    }
    attacker_incomplete_actor = {
        "actorGuid": "user_safi_pilot_other_02",
        "actorKind": "USER",
    }

    report: dict[str, Any] = {
        "runTimestamp": utc_now(),
        "runId": run_id,
        "gitCommitSha": git_sha(),
        "tests": {},
    }

    async with httpx.AsyncClient(timeout=25.0) as client:
        # === A01: Local UNIX Stack Health ===
        stack_h = await wait_for_full_stack(client)
        assert stack_h["gate"]["status"] == "ok" and stack_h["gate"]["jetstreamConnected"] is True
        assert stack_h["n8n"]["status"] == "ok"
        assert stack_h["calnode"]["status"] == "ok"
        assert "1.425.0" in stack_h["windmillVersion"]
        report["tests"]["A01"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            **stack_h,
        }

        # === A03: Marketing (n8n) + Native HeaderAuth Bypass Rejection + Single Follow-Up ===
        conv_a = f"conv_sow_a_{run_id}"
        subj_a = f"Customer A - Marketing Sales Ops {run_id}"

        # Direct unauthenticated calls to BOTH n8n webhooks MUST be rejected by native HeaderAuth (403)
        r_bypass_fu = await client.post(
            f"{N8N}/webhook/safi-approved-followup",
            json={
                "conversationId": conv_a,
                "recipientEmail": "unlisted@evil.example.com",
                "messageBody": "Bypass attempt",
                "idempotencyKey": f"bypass:{run_id}",
                "gateApproved": False,
            },
        )
        assert r_bypass_fu.status_code in (401, 403), f"Expected 401/403 bypass rejection, got {r_bypass_fu.status_code}"

        r_bypass_intake = await client.post(
            f"{N8N}/webhook/safi-lead-intake",
            json={"conversationId": conv_a, "leadEmail": "customer-a@allowlist.test.local"},
        )
        assert r_bypass_intake.status_code in (401, 403), f"Expected 401/403 intake rejection, got {r_bypass_intake.status_code}"

        # Legitimate Gate-mediated lead intake -> triggers n8n workflow -> authenticated /gate/dispatch callback -> single follow-up
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
        prepared_fu_key = intake_data["result"]["followupIdempotencyKey"]
        prepared_fu_body = intake_data["result"]["preparedFollowup"]

        # Wait briefly for n8n's asynchronous /gate/dispatch callback to complete the ONE prepared follow-up
        await asyncio.sleep(1.5)

        # Submitting the EXACT SAME followupIdempotencyKey via /safichat/ops/marketing/followup confirms idempotentReplay=True (ZERO double follow-up!)
        r_followup = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": prepared_fu_body,
                "idempotencyKey": prepared_fu_key,
                "actor": owner_actor,
                "correlationId": f"corr_a_{run_id}",
            },
        )
        assert r_followup.status_code == 200, r_followup.text
        fu_data = r_followup.json()
        assert fu_data["state"] == "DONE"
        assert fu_data["idempotentReplay"] is True, f"Expected single follow-up replay from n8n callback, got {fu_data}"
        assert fu_data["result"]["gateApproved"] is True
        assert fu_data["result"].get("receiptId", "").startswith("rcpt_")

        report["tests"]["A03"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "conversationId": conv_a,
            "intakeExecutionId": intake_data["result"].get("executionId"),
            "followupExecutionId": fu_data["result"].get("executionId"),
            "deliveryReceiptId": fu_data["result"].get("receiptId"),
            "singleFollowupVerifiedByIdempotentReplay": fu_data["idempotentReplay"],
            "directBypassStatus": r_bypass_fu.status_code,
            "directIntakeBypassStatus": r_bypass_intake.status_code,
        }

        # === A04: Sales (Calnode) — Real Slots + Single Idempotent Booking ===
        cal_list = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json()
        for item in cal_list.get("items", []):
            emails = [a.get("email") for a in item.get("attendees", [])]
            if item.get("status") == "confirmed" and "customer-a@allowlist.test.local" in emails:
                await client.post(
                    f"{CALNODE}/v1/bookings/{item['id']}/cancel",
                    headers=calnode_headers,
                    json={"reason": "Reset prior pilot test booking"},
                )

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
        assert cal_after == cal_before + 1

        report["tests"]["A04"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "conversationId": conv_a,
            "slotId": chosen_slot,
            "bookingId": booking_id,
            "inviteDelivery": b1["result"]["inviteDelivery"],
            "calnodeTotalBefore": cal_before,
            "calnodeTotalAfter": cal_after,
            "duplicateReplay": b2["idempotentReplay"],
        }

        # === A05: Operations (Windmill) — Least-Privilege Job + Live Error Reporting ===
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
        job_raw = r_jstat.json()["job"]
        assert job_raw["state"] == "COMPLETED"
        # Capture REAL least-privilege Windmill job payload into contracts/windmill-job.example.json
        (ROOT / "contracts" / "windmill-job.example.json").write_text(
            json.dumps(job_raw, indent=2) + "\n", encoding="utf-8"
        )

        # Live Windmill error reporting proof (ERR_ booking triggers real failed Windmill job -> 502 with jobId & error)
        r_wm_err = await client.post(
            f"{GATE}/safichat/ops/operations/assign",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "bookingId": f"ERR_INVALID_{run_id}",
                "assignee": "ops-specialist-1@allowlist.test.local",
                "taskSummary": "Verify live Windmill error reporting",
                "idempotencyKey": f"assign:err:{conv_a}:{run_id}",
                "actor": owner_actor,
            },
        )
        assert r_wm_err.status_code == 502, f"Expected 502 Windmill job error, got {r_wm_err.status_code}: {r_wm_err.text}"
        err_job_id = r_wm_err.json().get("details", {}).get("jobId")
        assert err_job_id, f"Expected structured failed Windmill jobId in error details: {r_wm_err.text}"

        report["tests"]["A05"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "conversationId": conv_a,
            "jobId": job_id,
            "permissionedAs": job_raw.get("permissioned_as"),
            "state": asgn["result"]["state"],
            "assignee": asgn["result"]["assignee"],
            "assignmentId": asgn["result"]["result"]["assignmentId"],
            "jobStatusEndpointVerified": True,
            "errorReportingVerified": {"statusCode": r_wm_err.status_code, "failedJobId": err_job_id},
        }

        # === A02: Strictly BLOCKED awaiting native SafiChat UI hook (V-06) ===
        report["tests"]["A02"] = {
            "status": "BLOCKED",
            "timestamp": utc_now(),
            "reason": "REST control surface (/safichat/ops/*) verified live for conv_a, but native SafiChat / Meeting Manager UI invocation contract (V-06) has not been supplied by SAFI (SOW §3 & §10).",
            "verifiedApiConversationId": conv_a,
        }

        # === A06: Selective Gated Across 3 Simultaneous Conversations ===
        conv_b = f"conv_sow_b_{run_id}"
        conv_c = f"conv_sow_c_{run_id}"
        subj_b = f"Customer B - Selective Gate {run_id}"
        subj_c = f"Customer C - Independent Ops {run_id}"

        await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_b,
                "subjectLineDisplay": subj_b,
                "recipientEmail": "customer-b@allowlist.test.local",
                "messageBody": "Initial greeting B",
                "idempotencyKey": "shared_seed_key",
                "actor": owner_actor,
            },
        )
        # Same idempotencyKey 'shared_seed_key' on Conv C executes independently (proves per-conversation scoping!)
        r_seed_c = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_c,
                "subjectLineDisplay": subj_c,
                "recipientEmail": "customer-c@allowlist.test.local",
                "messageBody": "Initial greeting C",
                "idempotencyKey": "shared_seed_key",
                "actor": owner_actor,
            },
        )
        assert r_seed_c.status_code == 200 and r_seed_c.json()["idempotentReplay"] is False

        r_barge_b = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"subjectLine": subj_b, "actor": owner_actor},
        )
        assert r_barge_b.status_code == 200, r_barge_b.text
        assert r_barge_b.json()["conversationId"] == conv_b
        assert r_barge_b.json()["mode"] == "GATED"

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
        r_a_while_b_gated = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "subjectLineDisplay": subj_a,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Independent Conv A follow-up while Conv B is GATED",
                "idempotencyKey": f"followup:{conv_a}:while_b_gated",
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
        assert r_a_while_b_gated.status_code == 200 and r_a_while_b_gated.json()["state"] == "DONE"
        assert r_c_job.status_code == 200 and r_c_job.json()["state"] == "DONE"
        hb1_id = r_hb1.json()["actionId"]
        hb2_id = r_hb2.json()["actionId"]

        # Capture REAL SQLite GateAction row BEFORE release (real payloadRef hash & createdAt!)
        real_gate_action_row = fetch_real_action_from_sqlite(hb1_id)
        (ROOT / "contracts" / "gate-action.example.json").write_text(
            json.dumps(real_gate_action_row, indent=2) + "\n", encoding="utf-8"
        )

        # Live 409 AMBIGUOUS_SUBJECT_LINE check (two caller-owned conversations sharing one Subject Line)
        dup_subj = f"Shared Renewal Subject {run_id}"
        conv_dup1 = f"conv_sow_dup1_{run_id}"
        conv_dup2 = f"conv_sow_dup2_{run_id}"
        for cid_dup, k_dup in ((conv_dup1, "d1"), (conv_dup2, "d2")):
            r_dup = await client.post(
                f"{GATE}/safichat/ops/marketing/followup",
                headers=gate_headers,
                json={
                    "conversationId": cid_dup,
                    "subjectLineDisplay": dup_subj,
                    "recipientEmail": "customer-a@allowlist.test.local",
                    "messageBody": f"Dup subject seed {k_dup}",
                    "idempotencyKey": f"dup:{run_id}:{k_dup}",
                    "actor": owner_actor,
                },
            )
            assert r_dup.status_code == 200, r_dup.text
        r_ambig = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"subjectLine": dup_subj, "actor": owner_actor},
        )
        assert r_ambig.status_code == 409, f"Expected 409 AMBIGUOUS_SUBJECT_LINE, got {r_ambig.status_code}: {r_ambig.text}"
        assert r_ambig.json()["errorCode"] == "AMBIGUOUS_SUBJECT_LINE"
        ambig_cands = {c["conversationId"] for c in r_ambig.json()["details"]["candidates"]}
        assert ambig_cands == {conv_dup1, conv_dup2}

        report["tests"]["A06"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "conversations": [conv_a, conv_b, conv_c],
            "gatedConversationId": conv_b,
            "heldActionIds": [hb1_id, hb2_id],
            "independentConvAFollowupExecId": r_a_while_b_gated.json()["result"].get("executionId"),
            "independentConvCJobId": r_c_job.json()["result"]["jobId"],
            "ambiguousSubjectLine409Verified": sorted(ambig_cands),
            "perConversationIdempotencyIsolationVerified": True,
        }

        # === A10: FULL LOCAL STACK RESTART During HELD Actions + Recovery ===
        restart_full_local_stack()
        rec_stack = await wait_for_full_stack(client, timeout_s=60.0)
        rec_health = rec_stack["gate"]
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

        # Verify Calnode booking survived full-stack restart AND retry produces zero duplicate bookings
        cal_post_restart = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json()
        cal_confirmed_before_retry = sum(1 for i in cal_post_restart.get("items", []) if i.get("status") == "confirmed")
        assert any(i["id"] == booking_id for i in cal_post_restart.get("items", []))
        r_book_post_restart = await client.post(
            f"{GATE}/safichat/ops/sales/book",
            headers=gate_headers,
            json=book_payload,
        )
        assert r_book_post_restart.status_code == 200, r_book_post_restart.text
        b_post = r_book_post_restart.json()
        assert b_post["idempotentReplay"] is True
        assert b_post["result"]["bookingId"] == booking_id
        cal_after_retry = (await client.get(f"{CALNODE}/v1/bookings", headers=calnode_headers)).json()
        cal_confirmed_after_retry = sum(1 for i in cal_after_retry.get("items", []) if i.get("status") == "confirmed")
        assert cal_confirmed_after_retry == cal_confirmed_before_retry

        report["tests"]["A10"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "scope": "FULL_DOCKER_COMPOSE_STACK_RESTART (gate-adapter, n8n, calnode, windmill-server, windmill-worker, windmill-db, nats)",
            "conversationId": conv_b,
            "recoveryReport": rec_health["recovery"],
            "modeAfterRestart": held_after_data["mode"],
            "heldCountAfterRestart": len(held_after_data["heldActions"]),
            "calnodeBookingSurvivedRestart": True,
            "postRestartBookingRetryReplay": b_post["idempotentReplay"],
            "calnodeConfirmedCountUnchanged": cal_confirmed_after_retry == cal_confirmed_before_retry,
        }

        # === A07: Ungate (Release 1, Cancel 1, No Duplicate + Live Approve/Reject) ===
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
        released_exec_id = ug["released"][0]["result"].get("executionId")

        # Retry the RELEASED action -> MUST be idempotentReplay=True with identical executionId (zero duplicate send!)
        r_retry_released = await client.post(
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
        assert r_retry_released.status_code == 200, r_retry_released.text
        assert r_retry_released.json()["idempotentReplay"] is True
        assert r_retry_released.json()["result"].get("executionId") == released_exec_id

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

        # Live /approve and /reject on a GATED conversation
        r_barge_dup1 = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_dup1, "actor": owner_actor},
        )
        assert r_barge_dup1.status_code == 200 and r_barge_dup1.json()["mode"] == "GATED"
        r_h_app = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_dup1,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Live approve test",
                "idempotencyKey": f"approve_live:{run_id}",
                "actor": owner_actor,
            },
        )
        r_h_rej = await client.post(
            f"{GATE}/safichat/ops/marketing/followup",
            headers=gate_headers,
            json={
                "conversationId": conv_dup1,
                "recipientEmail": "customer-a@allowlist.test.local",
                "messageBody": "Live reject test",
                "idempotencyKey": f"reject_live:{run_id}",
                "actor": owner_actor,
            },
        )
        assert r_h_app.json()["state"] == "HELD" and r_h_rej.json()["state"] == "HELD"
        h_app_id = r_h_app.json()["actionId"]
        h_rej_id = r_h_rej.json()["actionId"]

        r_live_approve = await client.post(
            f"{GATE}/safichat/ops/gate/approve",
            headers=gate_headers,
            json={"conversationId": conv_dup1, "actionId": h_app_id, "actor": owner_actor},
        )
        assert r_live_approve.status_code == 200 and r_live_approve.json()["state"] == "DONE"
        r_live_reject = await client.post(
            f"{GATE}/safichat/ops/gate/reject",
            headers=gate_headers,
            json={"conversationId": conv_dup1, "actionId": h_rej_id, "actor": owner_actor},
        )
        assert r_live_reject.status_code == 200 and r_live_reject.json()["state"] == "REJECTED"

        report["tests"]["A07"] = {
            "status": "PASS",
            "timestamp": utc_now(),
            "conversationId": conv_b,
            "releasedActionId": hb1_id,
            "releasedExecId": released_exec_id,
            "releasedRetryIdempotentReplay": r_retry_released.json()["idempotentReplay"],
            "cancelledActionIds": ug["cancelled"],
            "modeAfter": ug["mode"],
            "cancelledRetryStatus": r_retry_cancelled.json()["state"],
            "liveApproveVerified": {"actionId": h_app_id, "state": r_live_approve.json()["state"]},
            "liveRejectVerified": {"actionId": h_rej_id, "state": r_live_reject.json()["state"]},
        }

        # === A08: Live Race & Barge In Against Real Windmill Dispatch (Verified Reconciled In-Flight) ===
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
        wm_started_ts = utc_now()
        wm_finished_ts: str | None = None
        barge_confirmed_ts: str | None = None

        async def _run_wm_race():
            nonlocal wm_finished_ts
            resp = await client.post(
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
            wm_finished_ts = utc_now()
            return resp

        wm_task = asyncio.create_task(_run_wm_race())
        # Give gate-adapter ~60ms to enter DISPATCHING against Windmill before barge-in arrives
        await asyncio.sleep(0.06)
        barge_started_ts = utc_now()

        async def _run_barge_race():
            nonlocal barge_confirmed_ts
            resp = await client.post(
                f"{GATE}/safichat/ops/gate/barge-in",
                headers=gate_headers,
                json={"conversationId": conv_race, "actor": owner_actor},
            )
            barge_confirmed_ts = utc_now()
            return resp

        barge_task = asyncio.create_task(_run_barge_race())
        r_wm_race, r_barge_race = await asyncio.gather(wm_task, barge_task)
        assert r_wm_race.status_code == 200 and r_barge_race.status_code == 200
        assert r_barge_race.json()["mode"] == "GATED"
        assert r_barge_race.json()["undone"] is False
        assert len(r_barge_race.json()["reconciledInFlight"]) >= 1, (
            f"Expected in-flight Windmill action to be reconciled during barge-in, got: {r_barge_race.json()}"
        )

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
            "timestamp": utc_now(),
            "conversationId": conv_race,
            "timestamps": {
                "inflightStarted": wm_started_ts,
                "bargeInStarted": barge_started_ts,
                "inflightFinished": wm_finished_ts,
                "bargeInConfirmed": barge_confirmed_ts,
            },
            "inflightActionState": r_wm_race.json()["state"],
            "bargeInMode": r_barge_race.json()["mode"],
            "reconciledInFlight": r_barge_race.json()["reconciledInFlight"],
            "undone": r_barge_race.json()["undone"],
            "postConfirmationActionState": r_after_race.json()["state"],
        }

        # === A09: Live Fail-Closed Authz, Anti-DoS, IDOR, Allowlist & Scope Escape ===
        cb_headers = {"X-SAFI-Gate-Secret": env["GATE_API_TOKEN"]}
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
        # Unauthorized cross-user /approve, /reject, /ungate, /held on conv_race (GATED)
        held_race_id = r_after_race.json()["actionId"]
        r_unauth_approve = await client.post(
            f"{GATE}/safichat/ops/gate/approve",
            headers=gate_headers,
            json={"conversationId": conv_race, "actionId": held_race_id, "actor": other_actor},
        )
        r_unauth_reject = await client.post(
            f"{GATE}/safichat/ops/gate/reject",
            headers=gate_headers,
            json={"conversationId": conv_race, "actionId": held_race_id, "actor": other_actor},
        )
        r_unauth_ungate = await client.post(
            f"{GATE}/safichat/ops/gate/ungate",
            headers=gate_headers,
            json={"conversationId": conv_race, "resolutions": [], "actor": other_actor},
        )
        r_unauth_held = await client.post(
            f"{GATE}/safichat/ops/gate/held",
            headers=gate_headers,
            json={"conversationId": conv_race, "actor": other_actor},
        )
        # Cross-user Subject Line enumeration attempt -> 403 (zero candidate leak)
        r_cross_subject = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"subjectLine": subj_a, "actor": other_actor},
        )
        # Cross-user DoS attempt via empty meetingContext -> 403 (victim conv_a stays UNGATED!)
        r_cross_dos = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_a, "actor": attacker_incomplete_actor},
        )
        # Cross-conversation Windmill Job IDOR -> 404 (cannot read conv_a's jobId via conv_b)
        r_job_idor = await client.post(
            f"{GATE}/safichat/ops/operations/job-status",
            headers=gate_headers,
            json={"conversationId": conv_b, "jobId": job_id, "actor": owner_actor},
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
        # /gate/dispatch payload-recipient allowlist bypass attempt -> 403 RECIPIENT_NOT_ALLOWLISTED
        r_dispatch_payload_bypass = await client.post(
            f"{GATE}/gate/dispatch",
            headers=cb_headers,
            json={
                "conversationId": conv_a,
                "engine": "n8n",
                "operation": "dispatch_followup",
                "idempotencyKey": f"dispatch:bypass:{run_id}",
                "recipients": [],
                "payload": {"recipientEmail": "hidden-unlisted@evil.example", "messageBody": "bypass"},
                "actor": owner_actor,
            },
        )
        # Windmill arbitrary scriptPath escape via /gate/dispatch -> 403 UNAUTHORIZED
        r_wm_script_escape = await client.post(
            f"{GATE}/gate/dispatch",
            headers=cb_headers,
            json={
                "conversationId": conv_a,
                "engine": "windmill",
                "operation": "start_assignment",
                "idempotencyKey": f"wm:escape:{run_id}",
                "recipients": ["ops-specialist-1@allowlist.test.local"],
                "payload": {
                    "scriptPath": "f/other/evil_script",
                    "bookingId": "bkg_esc",
                    "conversationId": conv_a,
                    "assignee": "ops-specialist-1@allowlist.test.local",
                },
                "actor": owner_actor,
            },
        )
        # Calnode out-of-bounds calendarId escape -> 403 UNAUTHORIZED
        r_cal_escape = await client.post(
            f"{GATE}/safichat/ops/sales/slots",
            headers=gate_headers,
            json={
                "conversationId": conv_a,
                "calendarId": "other-unbounded-calendar",
                "actor": owner_actor,
            },
        )
        # Authorized owner sending incomplete context on their OWN conversation -> 401 FAILED_CLOSED, then live /reconcile!
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
        r_owner_incomplete = await client.post(
            f"{GATE}/safichat/ops/gate/barge-in",
            headers=gate_headers,
            json={"conversationId": conv_unc, "actor": owner_incomplete_actor},
        )
        r_live_reconcile = await client.post(
            f"{GATE}/safichat/ops/gate/reconcile",
            headers=gate_headers,
            json={"conversationId": conv_unc, "targetMode": "GATED", "actor": owner_actor},
        )

        assert r_no_bearer.status_code == 403
        assert r_no_cb_secret.status_code == 403
        assert r_stranger.status_code == 403
        assert r_cross_user.status_code == 403
        assert r_unauth_approve.status_code == 403
        assert r_unauth_reject.status_code == 403
        assert r_unauth_ungate.status_code == 403
        assert r_unauth_held.status_code == 403
        assert r_cross_subject.status_code == 403
        assert r_cross_dos.status_code == 403
        assert r_job_idor.status_code == 404
        assert r_revoked.status_code == 403
        assert r_non_allowlist.status_code == 403
        assert r_dispatch_payload_bypass.status_code == 403
        assert r_wm_script_escape.status_code == 403
        assert r_cal_escape.status_code == 403
        assert r_owner_incomplete.status_code == 401
        assert r_live_reconcile.status_code == 200 and r_live_reconcile.json()["mode"] == "GATED"

        report["tests"]["A09"] = {
            "status": "BLOCKED",
            "timestamp": utc_now(),
            "localFailClosedVerification": "PASS",
            "reason": "All local fail-closed authz, anti-DoS, Subject-Line isolation, Windmill job IDOR, revocation, allowlist bypass, script/calendar scope escape, and reconcile checks passed live; live SAFI Cognito endpoint (V-08 / SAFI_AUTHZ_URL) remains BLOCKED awaiting SAFI infrastructure.",
            "observedStatuses": {
                "missingBearerToken": r_no_bearer.status_code,
                "missingCallbackSecret": r_no_cb_secret.status_code,
                "unlistedStranger": r_stranger.status_code,
                "crossUserConversationAccess": r_cross_user.status_code,
                "crossUserApprove": r_unauth_approve.status_code,
                "crossUserReject": r_unauth_reject.status_code,
                "crossUserUngate": r_unauth_ungate.status_code,
                "crossUserHeld": r_unauth_held.status_code,
                "crossUserSubjectEnumeration": r_cross_subject.status_code,
                "crossUserIncompleteContextDos": r_cross_dos.status_code,
                "crossConversationJobStatusIdor": r_job_idor.status_code,
                "revokedActor": r_revoked.status_code,
                "nonAllowlistedRecipient": r_non_allowlist.status_code,
                "dispatchPayloadRecipientBypass": r_dispatch_payload_bypass.status_code,
                "windmillScriptPathEscape": r_wm_script_escape.status_code,
                "calnodeCalendarScopeEscape": r_cal_escape.status_code,
                "ownerIncompleteContextFailClosed": r_owner_incomplete.status_code,
                "liveReconcileFromFailedClosed": r_live_reconcile.status_code,
            },
        }

    # === A11: Verify Real JetStream ControlEvents (Full Context + All Lifecycle Types) & Capture Contract Examples ===
    stream_name = env.get("JETSTREAM_STREAM", "SAFI_HISTORY")

    js_script = (
        "import asyncio, json, nats\n"
        "async def go():\n"
        "  nc = await nats.connect('nats://nats:4222')\n"
        "  js = nc.jetstream()\n"
        f"  info = await js.stream_info('{stream_name}')\n"
        "  start = max(1, info.state.last_seq - 79)\n"
        "  out = {'total': info.state.messages, 'events': [], 'byType': {}, 'proxyEvent': None, 'sample': None}\n"
        "  for seq in range(start, info.state.last_seq + 1):\n"
        f"    msg = await js.get_msg('{stream_name}', seq=seq)\n"
        "    ev = json.loads(msg.data.decode('utf-8'))\n"
        f"    if '{run_id}' not in ev.get('conversationId', ''): continue\n"
        "    entry = {\n"
        "      'seq': seq, 'subject': msg.subject, 'eventId': ev['eventId'], 'type': ev['type'],\n"
        "      'conversationId': ev['conversationId'], 'subjectLineDisplay': ev.get('subjectLineDisplay'),\n"
        "      'actorGuid': ev['actorGuid'], 'actorKind': ev['actorKind'], 'outcome': ev['outcome'],\n"
        "      'context': ev['context'], 'ts': ev['ts']\n"
        "    }\n"
        "    out['events'].append(entry)\n"
        "    out['byType'][ev['type']] = entry\n"
        "    if ev.get('actorKind') == 'PROXY' and out['proxyEvent'] is None: out['proxyEvent'] = entry\n"
        "    if out['sample'] is None and ev.get('subjectLineDisplay'): out['sample'] = ev\n"
        "  await nc.close()\n"
        "  print(json.dumps(out))\n"
        "asyncio.run(go())\n"
    )
    cmd = (
        ["wsl", "-d", "Ubuntu-24.04", "-u", "root", "docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
        if os.name == "nt"
        else ["docker", "exec", "-i", "safi-gate-adapter", "python", "-"]
    )
    proc = subprocess.run(cmd, input=js_script, text=True, check=True, stdout=subprocess.PIPE)
    js_data = json.loads(proc.stdout)
    total_msgs = js_data["total"]
    lifecycle_by_type = js_data["byType"]
    proxy_event = js_data["proxyEvent"]
    sample_control_event = js_data["sample"]

    # Verify full lifecycle event coverage AND historical portContext preservation on n8n PROXY event
    for required_type in (
        "MODE_GATED",
        "ACTION_HELD",
        "ACTION_APPROVED",
        "ACTION_REJECTED",
        "ACTION_RELEASED",
        "ACTION_CANCELLED",
        "MODE_UNGATED",
        "GATE_FAILED_CLOSED",
    ):
        assert required_type in lifecycle_by_type, f"Missing lifecycle event type '{required_type}' in JetStream"
        ctx = lifecycle_by_type[required_type]["context"]
        assert "meetingContext" in ctx and "portContext" in ctx and "permissionProvenance" in ctx

    assert proxy_event is not None, "Expected n8n PROXY ControlEvent in JetStream"
    assert proxy_event["context"]["portContext"].get("port") == "safichat-local-api", (
        f"Original portContext.port was clobbered on PROXY event: {proxy_event['context']['portContext']}"
    )
    assert proxy_event["context"]["portContext"].get("proxyPort") == "n8n-internal"

    if sample_control_event:
        (ROOT / "contracts" / "control-event.example.json").write_text(
            json.dumps(sample_control_event, indent=2) + "\n", encoding="utf-8"
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
        "timestamp": utc_now(),
        "localJetStreamVerification": "PASS",
        "reason": "Verified real schema-validated ControlEvents across all lifecycle types (with full Meeting/Port/Permission Context and un-clobbered PROXY portContext) persisted in local NATS JetStream stream SAFI_HISTORY; binding to SAFI's production NATS/JetStream cluster & verifying SAFI Subject naming (V-07) remains BLOCKED awaiting SAFI infrastructure.",
        "stream": stream_name,
        "totalPersistedControlEvents": total_msgs,
        "runEventCount": len(js_data["events"]),
        "proxyContextPreservationVerified": proxy_event,
        "lifecycleEventsByType": lifecycle_by_type,
    }

    report["tests"]["A12"] = {
        "status": "BLOCKED",
        "timestamp": utc_now(),
        "reason": "Awaiting independent SAFI tester execution using docs/safichat_user_guide.md (SOW §8 A12).",
    }

    # Save redacted stack logs (SOW §8 deliverable)
    logs_path = ROOT / "evidence" / "redacted_stack_logs.txt"
    logs_path.write_text(capture_redacted_logs(env), encoding="utf-8")

    out_path = ROOT / "evidence" / "live_local_run_report.json"
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "summary": {k: v["status"] for k, v in report["tests"].items()},
                "reportPath": str(out_path),
                "redactedLogsPath": str(logs_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
