"""Persistent local operational store using SQLite in WAL mode (R-CTX-03, E1, E6)."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

import aiosqlite

from gate.errors import (
    ActionNotFoundError,
    AmbiguousSubjectLineError,
    ConversationNotFoundError,
    IdempotencyConflictError,
    InvalidActionStateError,
    StoreError,
)
from gate.models import (
    ActionState,
    ConversationMode,
    EngineName,
    GateActionRecord,
    Outcome,
    utc_now_iso,
)
from gate.state_machine import assert_valid_action_transition


def compute_payload_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _extract_external_ref_id(result: dict[str, Any] | None) -> str | None:
    if not isinstance(result, dict):
        return None
    for key in ("jobId", "bookingId", "executionId"):
        val = result.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


class GateStore:
    def __init__(self, db_path: str = "data/gate_state.db") -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None
        # Serializes multi-statement SQLite transactions on the shared connection (prevents cross-conv commit interleaving)
        self._write_lock = asyncio.Lock()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        async with self._write_lock:
            conn = self.db
            try:
                yield conn
                await conn.commit()
            except Exception:
                try:
                    await conn.rollback()
                except Exception:
                    pass
                raise

    async def initialize(self) -> None:
        try:
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(self.db_path)
            self._db.row_factory = aiosqlite.Row

            await self._db.execute("PRAGMA journal_mode = WAL;")
            await self._db.execute("PRAGMA synchronous = FULL;")
            await self._db.execute("PRAGMA busy_timeout = 5000;")
            await self._db.execute("PRAGMA foreign_keys = ON;")

            await self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id TEXT PRIMARY KEY,
                    subject_line TEXT,
                    owner_guid TEXT,
                    mode TEXT NOT NULL,
                    target_mode TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_conversations_subject
                ON conversations(subject_line);

                CREATE TABLE IF NOT EXISTS actions (
                    action_id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_ref TEXT,
                    payload_json TEXT NOT NULL,
                    recipients_json TEXT NOT NULL,
                    actor_guid TEXT NOT NULL,
                    actor_kind TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    result_json TEXT,
                    error_json TEXT,
                    external_ref_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (conversation_id, idempotency_key),
                    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
                );

                CREATE INDEX IF NOT EXISTS idx_actions_conversation_state
                ON actions(conversation_id, state);

                CREATE TABLE IF NOT EXISTS idempotency_records (
                    conversation_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (conversation_id, idempotency_key)
                );
                """
            )
            await self._db.commit()

            # Safe migrations for pre-existing container SQLite databases
            await self._run_migrations()
            await self._db.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_actions_conv_engine_extref
                ON actions(conversation_id, engine, external_ref_id);
                """
            )
            await self._db.commit()
        except Exception as exc:
            raise StoreError(f"Failed to initialize Gate operational store: {exc}") from exc

    async def _run_migrations(self) -> None:
        assert self._db is not None

        # 1. conversations.owner_guid
        col_cursor = await self._db.execute("PRAGMA table_info(conversations);")
        conv_cols = {r["name"] for r in await col_cursor.fetchall()}
        if "owner_guid" not in conv_cols:
            await self._db.execute("ALTER TABLE conversations ADD COLUMN owner_guid TEXT;")

        # 2. actions.external_ref_id & per-conversation UNIQUE(conversation_id, idempotency_key)
        act_cursor = await self._db.execute("PRAGMA table_info(actions);")
        act_cols = {r["name"] for r in await act_cursor.fetchall()}
        if "external_ref_id" not in act_cols:
            await self._db.execute("ALTER TABLE actions ADD COLUMN external_ref_id TEXT;")

        sql_cursor = await self._db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='actions';"
        )
        act_sql_row = await sql_cursor.fetchone()
        if act_sql_row and "idempotency_key TEXT NOT NULL UNIQUE" in (act_sql_row["sql"] or ""):
            await self._db.execute("PRAGMA foreign_keys = OFF;")
            await self._db.executescript(
                """
                CREATE TABLE actions_v2 (
                    action_id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_ref TEXT,
                    payload_json TEXT NOT NULL,
                    recipients_json TEXT NOT NULL,
                    actor_guid TEXT NOT NULL,
                    actor_kind TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    result_json TEXT,
                    error_json TEXT,
                    external_ref_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE (conversation_id, idempotency_key),
                    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
                );
                INSERT INTO actions_v2 (
                    action_id, conversation_id, engine, operation, idempotency_key,
                    state, payload_ref, payload_json, recipients_json, actor_guid,
                    actor_kind, context_json, result_json, error_json, external_ref_id,
                    created_at, updated_at
                )
                SELECT
                    action_id, conversation_id, engine, operation, idempotency_key,
                    state, payload_ref, payload_json, recipients_json, actor_guid,
                    actor_kind, context_json, result_json, error_json, external_ref_id,
                    created_at, updated_at
                FROM actions;
                DROP TABLE actions;
                ALTER TABLE actions_v2 RENAME TO actions;
                CREATE INDEX IF NOT EXISTS idx_actions_conversation_state
                ON actions(conversation_id, state);
                """
            )
            await self._db.execute("PRAGMA foreign_keys = ON;")

        # 3. idempotency_records composite primary key (conversation_id, idempotency_key)
        idem_sql_cursor = await self._db.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='idempotency_records';"
        )
        idem_sql_row = await idem_sql_cursor.fetchone()
        if idem_sql_row and "idempotency_key TEXT PRIMARY KEY" in (idem_sql_row["sql"] or ""):
            await self._db.executescript(
                """
                CREATE TABLE idempotency_records_v2 (
                    conversation_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    action_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    response_json TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (conversation_id, idempotency_key)
                );
                INSERT INTO idempotency_records_v2 (
                    conversation_id, idempotency_key, action_id, operation,
                    payload_hash, outcome, response_json, created_at
                )
                SELECT
                    conversation_id, idempotency_key, action_id, operation,
                    payload_hash, outcome, response_json, created_at
                FROM idempotency_records;
                DROP TABLE idempotency_records;
                ALTER TABLE idempotency_records_v2 RENAME TO idempotency_records;
                """
            )
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if not self._db:
            raise StoreError("Database not initialized; call initialize() first")
        return self._db

    # --- Conversation Mode Management ---

    async def get_or_create_conversation(
        self,
        conversation_id: str,
        subject_line: str = "",
        initial_mode: ConversationMode = ConversationMode.UNGATED,
        owner_guid: str | None = None,
    ) -> dict[str, Any]:
        now = utc_now_iso()
        async with self._transaction() as conn:
            await conn.execute(
                """
                INSERT OR IGNORE INTO conversations (conversation_id, subject_line, owner_guid, mode, target_mode, created_at, updated_at)
                VALUES (?, ?, ?, ?, NULL, ?, ?);
                """,
                (conversation_id, subject_line, owner_guid, initial_mode.value, now, now),
            )
            cursor = await conn.execute(
                "SELECT * FROM conversations WHERE conversation_id = ?;",
                (conversation_id,),
            )
            row = await cursor.fetchone()

            needs_subject = bool(subject_line and row["subject_line"] != subject_line)
            needs_owner = bool(owner_guid and not row["owner_guid"])
            if needs_subject or needs_owner:
                new_subject = subject_line if needs_subject else row["subject_line"]
                new_owner = owner_guid if needs_owner else row["owner_guid"]
                await conn.execute(
                    "UPDATE conversations SET subject_line = ?, owner_guid = ?, updated_at = ? WHERE conversation_id = ?;",
                    (new_subject, new_owner, now, conversation_id),
                )
                row = await (
                    await conn.execute(
                        "SELECT * FROM conversations WHERE conversation_id = ?;",
                        (conversation_id,),
                    )
                ).fetchone()

            return dict(row)

    async def list_conversation_owners(self) -> dict[str, str]:
        """Returns {conversation_id: owner_guid} for restoring per-conversation ACLs after restart."""
        cursor = await self.db.execute(
            "SELECT conversation_id, owner_guid FROM conversations WHERE owner_guid IS NOT NULL AND owner_guid != '';"
        )
        return {r["conversation_id"]: r["owner_guid"] for r in await cursor.fetchall()}

    async def get_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM conversations WHERE conversation_id = ?;",
            (conversation_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def list_conversations_by_subject(self, subject_line: str) -> list[dict[str, Any]]:
        """Returns all conversations matching Subject Line (caller MUST filter by authorization)."""
        cursor = await self.db.execute(
            "SELECT * FROM conversations WHERE LOWER(subject_line) = LOWER(?);",
            (subject_line.strip(),),
        )
        return [dict(r) for r in await cursor.fetchall()]

    async def resolve_conversation_by_subject(
        self, subject_line: str
    ) -> dict[str, Any]:
        """Direct subject resolution (used by store unit tests; GateModeController filters by caller authz)."""
        rows = await self.list_conversations_by_subject(subject_line)
        if not rows:
            raise ConversationNotFoundError(subject_line)
        if len(rows) > 1:
            candidates = [
                {
                    "conversationId": r["conversation_id"],
                    "subjectLine": r["subject_line"],
                    "mode": r["mode"],
                }
                for r in rows
            ]
            raise AmbiguousSubjectLineError(subject_line, candidates)
        return rows[0]

    async def update_conversation_mode(
        self,
        conversation_id: str,
        mode: ConversationMode,
        target_mode: ConversationMode | None = None,
    ) -> None:
        now = utc_now_iso()
        target_val = target_mode.value if target_mode else None
        async with self._transaction() as conn:
            await conn.execute(
                """
                UPDATE conversations
                SET mode = ?, target_mode = ?, updated_at = ?
                WHERE conversation_id = ?;
                """,
                (mode.value, target_val, now, conversation_id),
            )

    # --- Actions Management ---

    async def record_action(
        self,
        action_id: str,
        conversation_id: str,
        engine: EngineName,
        operation: str,
        idempotency_key: str,
        initial_state: ActionState,
        payload: dict[str, Any],
        recipients: list[str],
        actor_guid: str,
        actor_kind: str,
        context: dict[str, Any],
        payload_ref: str | None = None,
    ) -> GateActionRecord:
        now = utc_now_iso()
        payload_hash = compute_payload_hash(payload)
        effective_ref = payload_ref or f"sha256:{payload_hash[:16]}"

        async with self._transaction() as conn:
            cursor = await conn.execute(
                "SELECT * FROM idempotency_records WHERE conversation_id = ? AND idempotency_key = ?;",
                (conversation_id, idempotency_key),
            )
            existing_idempotency = await cursor.fetchone()
            if existing_idempotency:
                raise IdempotencyConflictError(idempotency_key)

            await conn.execute(
                """
                INSERT INTO actions (
                    action_id, conversation_id, engine, operation, idempotency_key,
                    state, payload_ref, payload_json, recipients_json, actor_guid,
                    actor_kind, context_json, result_json, error_json, external_ref_id,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?);
                """,
                (
                    action_id,
                    conversation_id,
                    engine.value,
                    operation,
                    idempotency_key,
                    initial_state.value,
                    effective_ref,
                    json.dumps(payload),
                    json.dumps(recipients),
                    actor_guid,
                    actor_kind,
                    json.dumps(context),
                    now,
                    now,
                ),
            )

            await conn.execute(
                """
                INSERT INTO idempotency_records (
                    conversation_id, idempotency_key, action_id, operation,
                    payload_hash, outcome, response_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?);
                """,
                (
                    conversation_id,
                    idempotency_key,
                    action_id,
                    operation,
                    payload_hash,
                    Outcome.UNKNOWN.value,
                    now,
                ),
            )

        record = GateActionRecord(
            actionId=action_id,
            conversationId=conversation_id,
            engine=engine,
            operation=operation,
            idempotencyKey=idempotency_key,
            state=initial_state,
            payloadRef=effective_ref,
            createdAt=now,
        )
        record.to_schema_dict()
        return record

    async def get_action(self, action_id: str) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM actions WHERE action_id = ?;", (action_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        data = dict(row)
        data["payload"] = json.loads(data["payload_json"])
        data["recipients"] = json.loads(data["recipients_json"])
        data["context"] = json.loads(data["context_json"])
        if data["result_json"]:
            data["result"] = json.loads(data["result_json"])
        if data["error_json"]:
            data["error"] = json.loads(data["error_json"])
        return data

    async def has_external_ref(
        self, conversation_id: str, engine: str, external_ref_id: str
    ) -> bool:
        """Verifies a downstream jobId/bookingId/executionId belongs to the given conversation (prevents IDOR)."""
        cursor = await self.db.execute(
            """
            SELECT 1 FROM actions
            WHERE conversation_id = ? AND engine = ? AND external_ref_id = ?
            LIMIT 1;
            """,
            (conversation_id, engine, external_ref_id),
        )
        if await cursor.fetchone():
            return True
        # Fallback check for rows persisted prior to external_ref_id backfill
        fb_cursor = await self.db.execute(
            "SELECT result_json FROM actions WHERE conversation_id = ? AND engine = ? AND result_json IS NOT NULL;",
            (conversation_id, engine),
        )
        for row in await fb_cursor.fetchall():
            try:
                parsed = json.loads(row["result_json"])
                if _extract_external_ref_id(parsed) == external_ref_id:
                    return True
            except ValueError:
                continue
        return False

    async def get_held_actions(self, conversation_id: str) -> list[dict[str, Any]]:
        cursor = await self.db.execute(
            """
            SELECT * FROM actions
            WHERE conversation_id = ? AND state = ?
            ORDER BY created_at ASC;
            """,
            (conversation_id, ActionState.HELD.value),
        )
        rows = await cursor.fetchall()
        result = []
        for r in rows:
            item = dict(r)
            item["payload"] = json.loads(item["payload_json"])
            result.append(item)
        return result

    async def get_inflight_actions(self, conversation_id: str) -> list[dict[str, Any]]:
        cursor = await self.db.execute(
            """
            SELECT * FROM actions
            WHERE conversation_id = ? AND state = ?;
            """,
            (conversation_id, ActionState.DISPATCHING.value),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def update_action_state(
        self,
        action_id: str,
        state: ActionState,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        outcome_override: Outcome | None = None,
    ) -> None:
        now = utc_now_iso()
        res_json = json.dumps(result) if result is not None else None
        err_json = json.dumps(error) if error is not None else None
        ext_ref = _extract_external_ref_id(result)

        terminal_states = (
            ActionState.DONE,
            ActionState.FAILED,
            ActionState.REJECTED,
            ActionState.CANCELLED,
        )

        async with self._transaction() as conn:
            cursor = await conn.execute(
                "SELECT state FROM actions WHERE action_id = ?;",
                (action_id,),
            )
            row = await cursor.fetchone()
            if not row:
                raise ActionNotFoundError(action_id)

            current_state = ActionState(row["state"])
            try:
                assert_valid_action_transition(current_state, state)
            except ValueError as exc:
                raise InvalidActionStateError(
                    action_id, current_state.value, state.value
                ) from exc

            # R-CTX-03: Purge message payload body once terminal; retain only operational ref/hash
            if state in terminal_states:
                await conn.execute(
                    """
                    UPDATE actions
                    SET state = ?, payload_json = '{}',
                        result_json = COALESCE(?, result_json),
                        error_json = COALESCE(?, error_json),
                        external_ref_id = COALESCE(?, external_ref_id),
                        updated_at = ?
                    WHERE action_id = ?;
                    """,
                    (state.value, res_json, err_json, ext_ref, now, action_id),
                )
                if outcome_override is not None:
                    outcome_val = outcome_override.value
                elif state == ActionState.DONE:
                    outcome_val = Outcome.OK.value
                elif state in (ActionState.CANCELLED, ActionState.REJECTED):
                    outcome_val = Outcome.DENIED.value
                else:
                    outcome_val = Outcome.ERROR.value
                resp_val = res_json or err_json
                await conn.execute(
                    """
                    UPDATE idempotency_records
                    SET outcome = ?, response_json = ?
                    WHERE action_id = ?;
                    """,
                    (outcome_val, resp_val, action_id),
                )
            else:
                await conn.execute(
                    """
                    UPDATE actions
                    SET state = ?, result_json = COALESCE(?, result_json),
                        error_json = COALESCE(?, error_json),
                        external_ref_id = COALESCE(?, external_ref_id),
                        updated_at = ?
                    WHERE action_id = ?;
                    """,
                    (state.value, res_json, err_json, ext_ref, now, action_id),
                )

    async def get_idempotency_record(
        self, conversation_id: str, idempotency_key: str
    ) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM idempotency_records WHERE conversation_id = ? AND idempotency_key = ?;",
            (conversation_id, idempotency_key),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_action_by_idempotency_key(
        self, conversation_id: str, idempotency_key: str
    ) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM actions WHERE conversation_id = ? AND idempotency_key = ?;",
            (conversation_id, idempotency_key),
        )
        row = await cursor.fetchone()
        if not row:
            return None
        data = dict(row)
        data["payload"] = json.loads(data["payload_json"])
        data["recipients"] = json.loads(data["recipients_json"])
        data["context"] = json.loads(data["context_json"])
        if data["result_json"]:
            data["result"] = json.loads(data["result_json"])
        if data["error_json"]:
            data["error"] = json.loads(data["error_json"])
        return data

    async def update_idempotency_outcome(
        self,
        conversation_id: str,
        idempotency_key: str,
        outcome: Outcome,
        response: dict[str, Any] | None = None,
    ) -> None:
        resp_json = json.dumps(response) if response is not None else None
        async with self._transaction() as conn:
            await conn.execute(
                """
                UPDATE idempotency_records
                SET outcome = ?, response_json = COALESCE(?, response_json)
                WHERE conversation_id = ? AND idempotency_key = ?;
                """,
                (outcome.value, resp_json, conversation_id, idempotency_key),
            )

    # --- Restart & Crash Recovery (E6, R-REC-01) ---

    async def recover_on_startup(self) -> dict[str, Any]:
        """Scans store on restart: recovers modes & HELD actions, settles interrupted states, and fails closed."""
        affected_convs: set[str] = set()
        interrupted_dispatching_ids: list[str] = []
        interrupted_pre_dispatch_ids: list[str] = []

        cursor = await self.db.execute(
            "SELECT action_id, conversation_id FROM actions WHERE state = ?;",
            (ActionState.DISPATCHING.value,),
        )
        for row in await cursor.fetchall():
            aid = row["action_id"]
            cid = row["conversation_id"]
            interrupted_dispatching_ids.append(aid)
            affected_convs.add(cid)
            await self.update_action_state(
                aid,
                ActionState.FAILED,
                error={"code": "INTERRUPTED_ON_RESTART", "outcome": "unknown"},
                outcome_override=Outcome.UNKNOWN,
            )

        cursor = await self.db.execute(
            "SELECT action_id, conversation_id FROM actions WHERE state IN (?, ?, ?);",
            (
                ActionState.RECEIVED.value,
                ActionState.APPROVED.value,
                ActionState.AUTHORIZED.value,
            ),
        )
        for row in await cursor.fetchall():
            aid = row["action_id"]
            cid = row["conversation_id"]
            interrupted_pre_dispatch_ids.append(aid)
            affected_convs.add(cid)
            await self.update_action_state(
                aid,
                ActionState.FAILED,
                error={"code": "INTERRUPTED_BEFORE_DISPATCH"},
                outcome_override=Outcome.ERROR,
            )

        cursor = await self.db.execute(
            "SELECT conversation_id FROM conversations WHERE mode = ?;",
            (ConversationMode.TRANSITIONING.value,),
        )
        for row in await cursor.fetchall():
            affected_convs.add(row["conversation_id"])

        failed_closed_details: list[dict[str, Any]] = []
        for conv_id in sorted(affected_convs):
            conv = await self.get_conversation(conv_id)
            await self.update_conversation_mode(conv_id, ConversationMode.FAILED_CLOSED)
            failed_closed_details.append(
                {
                    "conversationId": conv_id,
                    "subjectLine": (conv["subject_line"] if conv else "") or "",
                }
            )

        cursor = await self.db.execute(
            "SELECT COUNT(*) as count FROM actions WHERE state = ?;",
            (ActionState.HELD.value,),
        )
        held_count = (await cursor.fetchone())["count"]

        return {
            "interruptedConversationsFailedClosed": [d["conversationId"] for d in failed_closed_details],
            "failedClosedDetails": failed_closed_details,
            "failedClosedCount": len(failed_closed_details),
            "interruptedDispatchingActions": interrupted_dispatching_ids,
            "interruptedPreDispatchActions": interrupted_pre_dispatch_ids,
            "heldActionsPreserved": held_count,
        }
