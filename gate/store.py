"""Persistent local operational store using SQLite in WAL mode (R-CTX-03, E1, E6)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import aiosqlite

from gate.errors import (
    AmbiguousSubjectLineError,
    ConversationNotFoundError,
    IdempotencyConflictError,
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


def compute_payload_hash(payload: dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


class GateStore:
    def __init__(self, db_path: str = "data/gate_state.db") -> None:
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row

        # Enable WAL mode and synchronous=NORMAL for durability + concurrent reads
        await self._db.execute("PRAGMA journal_mode = WAL;")
        await self._db.execute("PRAGMA busy_timeout = 5000;")
        await self._db.execute("PRAGMA foreign_keys = ON;")

        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS conversations (
                conversation_id TEXT PRIMARY KEY,
                subject_line TEXT,
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
                idempotency_key TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                payload_ref TEXT,
                payload_json TEXT NOT NULL,
                recipients_json TEXT NOT NULL,
                actor_guid TEXT NOT NULL,
                actor_kind TEXT NOT NULL,
                context_json TEXT NOT NULL,
                result_json TEXT,
                error_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id)
            );

            CREATE INDEX IF NOT EXISTS idx_actions_conversation_state 
            ON actions(conversation_id, state);

            CREATE TABLE IF NOT EXISTS idempotency_records (
                idempotency_key TEXT PRIMARY KEY,
                conversation_id TEXT NOT NULL,
                action_id TEXT NOT NULL,
                operation TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                outcome TEXT NOT NULL,
                response_json TEXT,
                created_at TEXT NOT NULL
            );
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
    ) -> dict[str, Any]:
        cursor = await self.db.execute(
            "SELECT * FROM conversations WHERE conversation_id = ?;",
            (conversation_id,),
        )
        row = await cursor.fetchone()
        now = utc_now_iso()
        if not row:
            await self.db.execute(
                """
                INSERT INTO conversations (conversation_id, subject_line, mode, target_mode, created_at, updated_at)
                VALUES (?, ?, ?, NULL, ?, ?);
                """,
                (conversation_id, subject_line, initial_mode.value, now, now),
            )
            await self.db.commit()
            return {
                "conversationId": conversation_id,
                "subjectLine": subject_line,
                "mode": initial_mode.value,
                "targetMode": None,
                "createdAt": now,
                "updatedAt": now,
            }

        # Update subject line if newly provided
        if subject_line and row["subject_line"] != subject_line:
            await self.db.execute(
                "UPDATE conversations SET subject_line = ?, updated_at = ? WHERE conversation_id = ?;",
                (subject_line, now, conversation_id),
            )
            await self.db.commit()
            row = await (
                await self.db.execute(
                    "SELECT * FROM conversations WHERE conversation_id = ?;",
                    (conversation_id,),
                )
            ).fetchone()

        return dict(row)

    async def get_conversation(self, conversation_id: str) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM conversations WHERE conversation_id = ?;",
            (conversation_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def resolve_conversation_by_subject(
        self, subject_line: str
    ) -> dict[str, Any]:
        """Resolves conversation by Subject Line. Ambiguity forces user choice (R-GATE-04)."""
        cursor = await self.db.execute(
            "SELECT * FROM conversations WHERE LOWER(subject_line) = LOWER(?);",
            (subject_line.strip(),),
        )
        rows = [dict(r) for r in await cursor.fetchall()]
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
        await self.db.execute(
            """
            UPDATE conversations 
            SET mode = ?, target_mode = ?, updated_at = ? 
            WHERE conversation_id = ?;
            """,
            (mode.value, target_val, now, conversation_id),
        )
        await self.db.commit()

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

        # Check existing idempotency key (E5 Invariant 1)
        cursor = await self.db.execute(
            "SELECT * FROM idempotency_records WHERE idempotency_key = ?;",
            (idempotency_key,),
        )
        existing_idempotency = await cursor.fetchone()
        if existing_idempotency:
            if existing_idempotency["payload_hash"] != payload_hash:
                raise IdempotencyConflictError(idempotency_key)

        await self.db.execute(
            """
            INSERT INTO actions (
                action_id, conversation_id, engine, operation, idempotency_key,
                state, payload_ref, payload_json, recipients_json, actor_guid,
                actor_kind, context_json, result_json, error_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?);
            """,
            (
                action_id,
                conversation_id,
                engine.value,
                operation,
                idempotency_key,
                initial_state.value,
                payload_ref,
                json.dumps(payload),
                json.dumps(recipients),
                actor_guid,
                actor_kind,
                json.dumps(context),
                now,
                now,
            ),
        )

        if not existing_idempotency:
            await self.db.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key, conversation_id, action_id, operation,
                    payload_hash, outcome, response_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?);
                """,
                (
                    idempotency_key,
                    conversation_id,
                    action_id,
                    operation,
                    payload_hash,
                    Outcome.UNKNOWN.value,
                    now,
                ),
            )

        await self.db.commit()

        record = GateActionRecord(
            actionId=action_id,
            conversationId=conversation_id,
            engine=engine,
            operation=operation,
            idempotencyKey=idempotency_key,
            state=initial_state,
            payloadRef=payload_ref,
            createdAt=now,
        )
        record.to_schema_dict()  # strict contract validation
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
    ) -> None:
        now = utc_now_iso()
        res_json = json.dumps(result) if result is not None else None
        err_json = json.dumps(error) if error is not None else None

        await self.db.execute(
            """
            UPDATE actions 
            SET state = ?, result_json = COALESCE(?, result_json), 
                error_json = COALESCE(?, error_json), updated_at = ? 
            WHERE action_id = ?;
            """,
            (state.value, res_json, err_json, now, action_id),
        )

        # Update idempotency record outcome if terminal
        if state in (ActionState.DONE, ActionState.FAILED, ActionState.CANCELLED):
            outcome_val = (
                Outcome.OK.value
                if state == ActionState.DONE
                else (Outcome.DENIED.value if state == ActionState.CANCELLED else Outcome.ERROR.value)
            )
            resp_val = res_json or err_json
            await self.db.execute(
                """
                UPDATE idempotency_records 
                SET outcome = ?, response_json = ? 
                WHERE action_id = ?;
                """,
                (outcome_val, resp_val, action_id),
            )

        await self.db.commit()

    async def get_idempotency_record(
        self, idempotency_key: str
    ) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM idempotency_records WHERE idempotency_key = ?;",
            (idempotency_key,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def get_action_by_idempotency_key(
        self, idempotency_key: str
    ) -> dict[str, Any] | None:
        cursor = await self.db.execute(
            "SELECT * FROM actions WHERE idempotency_key = ?;",
            (idempotency_key,),
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
        idempotency_key: str,
        outcome: Outcome,
        response: dict[str, Any] | None = None,
    ) -> None:
        resp_json = json.dumps(response) if response is not None else None
        await self.db.execute(
            """
            UPDATE idempotency_records
            SET outcome = ?, response_json = COALESCE(?, response_json)
            WHERE idempotency_key = ?;
            """,
            (outcome.value, resp_json, idempotency_key),
        )
        await self.db.commit()

    # --- Restart & Crash Recovery (E6, R-REC-01) ---

    async def recover_on_startup(self) -> dict[str, Any]:
        """Scans store on restart: recovers modes and pending actions, or fails closed."""
        # Any actions left in DISPATCHING when process died have unknown outcome -> fail closed
        cursor = await self.db.execute(
            "SELECT DISTINCT conversation_id FROM actions WHERE state = ?;",
            (ActionState.DISPATCHING.value,),
        )
        interrupted_convs = [r["conversation_id"] for r in await cursor.fetchall()]

        failed_closed_count = 0
        for conv_id in interrupted_convs:
            await self.update_conversation_mode(conv_id, ConversationMode.FAILED_CLOSED)
            failed_closed_count += 1

        # Also any conversations left in TRANSITIONING fail closed until reconciled
        cursor = await self.db.execute(
            "SELECT conversation_id FROM conversations WHERE mode = ?;",
            (ConversationMode.TRANSITIONING.value,),
        )
        transitioning = [r["conversation_id"] for r in await cursor.fetchall()]
        for conv_id in transitioning:
            await self.update_conversation_mode(conv_id, ConversationMode.FAILED_CLOSED)
            failed_closed_count += 1

        cursor = await self.db.execute(
            "SELECT COUNT(*) as count FROM actions WHERE state = ?;",
            (ActionState.HELD.value,),
        )
        held_count = (await cursor.fetchone())["count"]

        return {
            "interruptedConversationsFailedClosed": interrupted_convs + transitioning,
            "failedClosedCount": failed_closed_count,
            "heldActionsPreserved": held_count,
        }
