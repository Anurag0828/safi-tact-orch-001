#!/usr/bin/env python3
"""Part 7 — Independent Downstream Oracle (n8n, Calnode, Windmill, Gate SQLite, NATS JetStream)."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import nats

ROOT = Path("/mnt/d/Interworks/Safi")
ENV_PATH = ROOT / "deploy" / ".env"

N8N = "http://127.0.0.1:5678"
CALNODE = "http://127.0.0.1:3000"
WINDMILL = "http://127.0.0.1:8001"
NATS_URL = "nats://127.0.0.1:4222"

_N8N_CLIENT: httpx.Client | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def _get_n8n_client(env: dict[str, str], force_login: bool = False) -> httpx.Client:
    global _N8N_CLIENT
    if _N8N_CLIENT is None:
        _N8N_CLIENT = httpx.Client(timeout=15.0)
        force_login = True
    if force_login:
        for attempt in range(5):
            login = _N8N_CLIENT.post(
                f"{N8N}/rest/login",
                json={"email": "safi_admin@local.test", "password": env["N8N_ADMIN_PASSWORD"]},
            )
            if login.status_code == 429 and attempt < 4:
                time.sleep(5.0)
                continue
            login.raise_for_status()
            break
    return _N8N_CLIENT


def query_n8n(env: dict[str, str]) -> dict[str, Any]:
    """Queries n8n's local REST executions API with lastId pagination (exact counts beyond 100-item page cap)."""
    c = _get_n8n_client(env)
    wfs_resp = c.get(f"{N8N}/rest/workflows")
    if wfs_resp.status_code == 401:
        c = _get_n8n_client(env, force_login=True)
        wfs_resp = c.get(f"{N8N}/rest/workflows")
    wfs_resp.raise_for_status()
    wf_map = {str(w["id"]): w["name"] for w in wfs_resp.json().get("data", [])}

    all_results: list[dict[str, Any]] = []
    total_count = 0
    last_id: str | None = None
    for _ in range(30):
        params: dict[str, Any] = {"limit": 100}
        if last_id is not None:
            params["lastId"] = last_id
        exec_resp = c.get(f"{N8N}/rest/executions", params=params)
        if exec_resp.status_code == 401:
            c = _get_n8n_client(env, force_login=True)
            exec_resp = c.get(f"{N8N}/rest/executions", params=params)
        exec_resp.raise_for_status()
        raw = exec_resp.json().get("data", {})
        page = raw.get("results", []) if isinstance(raw, dict) else raw
        if last_id is None and isinstance(raw, dict):
            total_count = raw.get("count", len(page))
        if not page:
            break
        all_results.extend(page)
        if len(page) < 100:
            break
        last_id = str(page[-1].get("id"))

    by_workflow: dict[str, int] = {}
    success_count = 0
    error_count = 0
    recent: list[dict[str, Any]] = []
    for ex in all_results:
        wf_id = str(ex.get("workflowId", ""))
        wf_name = wf_map.get(wf_id, wf_id)
        by_workflow[wf_name] = by_workflow.get(wf_name, 0) + 1
        st = str(ex.get("status", "")).lower()
        if st == "success":
            success_count += 1
        elif st in ("error", "crashed"):
            error_count += 1
        if len(recent) < 30:
            recent.append(
                {
                    "id": str(ex.get("id")),
                    "workflowId": wf_id,
                    "workflowName": wf_name,
                    "status": ex.get("status"),
                    "startedAt": ex.get("startedAt"),
                    "stoppedAt": ex.get("stoppedAt"),
                }
            )
    return {
        "method": "n8n_rest_executions_api_paginated",
        "totalExecutions": total_count or len(all_results),
        "successExecutions": success_count,
        "errorExecutions": error_count,
        "byWorkflow": by_workflow,
        "recentExecutions": recent,
    }


def query_calnode(env: dict[str, str], include_slots: bool = False) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {env['CALNODE_API_KEY']}"}
    with httpx.Client(timeout=15.0) as c:
        r = c.get(f"{CALNODE}/v1/bookings", headers=headers)
        r.raise_for_status()
        data = r.json()
        items = data.get("items", [])
        confirmed = [i for i in items if i.get("status") == "confirmed"]
        out: dict[str, Any] = {
            "method": "calnode_v1_bookings_api",
            "totalBookings": data.get("total", len(items)),
            "confirmedCount": len(confirmed),
            "confirmedBookings": [
                {
                    "id": i.get("id"),
                    "status": i.get("status"),
                    "start_at": i.get("start_at"),
                    "invite_delivery": i.get("invite_delivery"),
                    "attendees": [a.get("email") for a in i.get("attendees", [])],
                }
                for i in confirmed
            ],
        }
        if include_slots:
            today = datetime.now(timezone.utc).date()
            sr = c.get(
                f"{CALNODE}/v1/event-types/{env.get('CALNODE_CALENDAR_ID', 'consultation-30m')}/slots",
                headers=headers,
                params={"from": today.isoformat(), "to": (today + timedelta(days=5)).isoformat(), "tz": "UTC"},
            )
            sr.raise_for_status()
            out["directSlots"] = sr.json().get("slots", [])
        return out


def query_windmill(env: dict[str, str]) -> dict[str, Any]:
    ws = env.get("WINDMILL_WORKSPACE", "safi-ops")
    headers = {"Authorization": f"Bearer {env['WINDMILL_TOKEN']}"}
    with httpx.Client(timeout=15.0) as c:
        who = c.get(f"{WINDMILL}/api/users/whoami", headers=headers)
        who.raise_for_status()
        who_data = who.json()
        comp = c.get(f"{WINDMILL}/api/w/{ws}/jobs/completed/list", headers=headers, params={"per_page": 1000})
        comp.raise_for_status()
        jobs = comp.json()
        success_jobs = [j for j in jobs if j.get("success") is True]
        failed_jobs = [j for j in jobs if j.get("success") is False]
        return {
            "method": "windmill_jobs_completed_api",
            "whoami": {
                "email": who_data.get("email"),
                "username": who_data.get("username"),
                "super_admin": who_data.get("super_admin"),
            },
            "totalCompletedJobs": len(jobs),
            "successJobs": len(success_jobs),
            "failedJobs": len(failed_jobs),
            "recentJobs": [
                {
                    "id": j.get("id"),
                    "script_path": j.get("script_path"),
                    "success": j.get("success"),
                    "created_at": j.get("created_at"),
                    "started_at": j.get("started_at"),
                    "duration_ms": j.get("duration_ms"),
                    "permissioned_as": j.get("permissioned_as"),
                }
                for j in jobs[:35]
            ],
        }


def query_gate_sqlite(conversation_id: str | None = None) -> dict[str, Any]:
    py = (
        "import json, sqlite3\n"
        "src = sqlite3.connect('file:/app/data/gate_state.db?mode=ro', uri=True)\n"
        "conn = sqlite3.connect(':memory:')\n"
        "src.backup(conn)\n"
        "src.close()\n"
        "conn.row_factory = sqlite3.Row\n"
        f"cid = {conversation_id!r}\n"
        "if cid:\n"
        "  convs = [dict(r) for r in conn.execute('SELECT conversation_id, subject_line, owner_guid, mode FROM conversations WHERE conversation_id=?', (cid,))]\n"
        "  acts = [dict(r) for r in conn.execute('SELECT action_id, conversation_id, engine, operation, idempotency_key, state, payload_json, external_ref_id, created_at FROM actions WHERE conversation_id=?', (cid,))]\n"
        "  idems = [dict(r) for r in conn.execute('SELECT conversation_id, idempotency_key, action_id, outcome FROM idempotency_records WHERE conversation_id=?', (cid,))]\n"
        "else:\n"
        "  convs = [dict(r) for r in conn.execute('SELECT conversation_id, subject_line, owner_guid, mode FROM conversations ORDER BY updated_at DESC LIMIT 20')]\n"
        "  acts = [dict(r) for r in conn.execute('SELECT action_id, conversation_id, engine, operation, idempotency_key, state, payload_json, external_ref_id, created_at FROM actions ORDER BY created_at DESC LIMIT 30')]\n"
        "  idems = []\n"
        "total_acts = conn.execute('SELECT COUNT(*) FROM actions').fetchone()[0]\n"
        "non_purged_run = conn.execute(\"SELECT COUNT(*) FROM actions WHERE conversation_id LIKE 'conv_ver_%' AND state IN ('DONE','FAILED','REJECTED','CANCELLED') AND payload_json != '{}'\").fetchone()[0]\n"
        "print(json.dumps({'method': 'sqlite_readonly_copy', 'totalActions': total_acts, 'nonPurgedTerminalPayloadsInRun': non_purged_run, 'conversations': convs, 'actions': acts, 'idempotency': idems}))\n"
    )
    p = subprocess.run(
        ["docker", "exec", "-i", "safi-gate-adapter", "python", "-"],
        input=py,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if p.returncode != 0:
        return {"method": "sqlite_readonly_copy", "error": p.stderr.strip()}
    return json.loads(p.stdout)


async def query_jetstream(stream_name: str, conversation_id: str | None = None) -> dict[str, Any]:
    nc = await nats.connect(
        servers=[NATS_URL], allow_reconnect=False, max_reconnect_attempts=0, connect_timeout=3.0
    )
    try:
        js = nc.jetstream()
        info = await js.stream_info(stream_name, subjects_filter=">")
        state = info.state
        out: dict[str, Any] = {
            "method": "nats_jetstream_direct",
            "stream": stream_name,
            "messages": state.messages,
            "firstSeq": state.first_seq,
            "lastSeq": state.last_seq,
            "subjectCount": len(state.subjects or {}),
            "subjects": dict(state.subjects or {}),
        }
        if conversation_id:
            events: list[dict[str, Any]] = []
            start = max(state.first_seq, state.last_seq - 199)
            for seq in range(start, state.last_seq + 1):
                try:
                    msg = await js.get_msg(stream_name, seq=seq)
                    ev = json.loads(msg.data.decode("utf-8"))
                    if ev.get("conversationId") == conversation_id:
                        events.append({"seq": seq, "subject": msg.subject, **ev})
                except Exception:
                    continue
            out["conversationEvents"] = events
        return out
    finally:
        await nc.close()


async def snapshot(conversation_id: str | None = None, include_slots: bool = False) -> dict[str, Any]:
    env = load_env()
    stream_name = env.get("JETSTREAM_STREAM", "SAFI_HISTORY")
    snap: dict[str, Any] = {"capturedUtc": utc_now(), "conversationFilter": conversation_id}
    try:
        snap["n8n"] = query_n8n(env)
    except Exception as exc:
        snap["n8n"] = {"error": str(exc)}
    try:
        snap["calnode"] = query_calnode(env, include_slots=include_slots)
    except Exception as exc:
        snap["calnode"] = {"error": str(exc)}
    try:
        snap["windmill"] = query_windmill(env)
    except Exception as exc:
        snap["windmill"] = {"error": str(exc)}
    snap["gateSqlite"] = query_gate_sqlite(conversation_id=conversation_id)
    try:
        snap["jetstream"] = await query_jetstream(stream_name, conversation_id=conversation_id)
    except Exception as exc:
        snap["jetstream"] = {"error": str(exc)}
    return snap


if __name__ == "__main__":
    cid = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "-" else None
    print(json.dumps(asyncio.run(snapshot(conversation_id=cid, include_slots="--slots" in sys.argv)), indent=2))
