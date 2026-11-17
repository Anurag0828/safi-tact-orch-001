# Gate Adapter Error Codes (Part D3)

Every error code produced by the Gate Adapter, its semantic meaning, HTTP status, and required handling rule.

| Error Code | HTTP | Meaning | Handling Rule |
|---|---|---|---|
| `UNAUTHORIZED` | 403 | Actor lacks permission for this conversation or operation (R-AUTH-01, R-AUTH-02, R-GATE-09) | Reject immediately; record `denied` ControlEvent; never dispatch |
| `IDENTITY_UNCERTAIN` | 401 | Missing, malformed, or unverifiable actor GUID or Meeting/Port Context (R-SEC-04) | Fail closed; record `GATE_FAILED_CLOSED`; never dispatch |
| `PERMISSION_CHECK_FAILED` | 503 | SAFI permission recheck endpoint unavailable, timed out, or returned ambiguous result (R-SEC-04, R-GATE-09) | Fail closed; transition conversation to `FAILED_CLOSED`; never dispatch |
| `GATE_FAILED_CLOSED` | 503 | Conversation is currently in `FAILED_CLOSED` state due to prior uncertainty or crash recovery (E4, E6) | Block all new dispatches until explicit operator `RECONCILE` |
| `CONVERSATION_NOT_FOUND` | 404 | Provided Subject Line or ConversationID matches zero known conversations | Surface to user; no state change |
| `AMBIGUOUS_SUBJECT_LINE` | 409 | Subject Line matches multiple conversations (R-GATE-04) | Return candidate `ConversationID` list; force explicit user selection |
| `INVALID_MODE_TRANSITION` | 409 | Requested mode transition is illegal from the current conversation state (E4) | Reject; surface current mode to user |
| `HELD_ACTIONS_UNRESOLVED` | 409 | `RETURN_TO_UNGATED` requested without explicit `RELEASE` or `CANCEL` decision for every held action (R-GATE-06) | Return list of held actions; remain in `GATED`/`TRANSITIONING` until all resolved |
| `ACTION_NOT_FOUND` | 404 | Specified `actionId` does not exist in this conversation | Reject; surface to user |
| `INVALID_ACTION_STATE` | 409 | Action is not in a state permitting the requested operation (e.g. approving an already rejected action) | Reject; surface current action state |
| `RECIPIENT_NOT_ALLOWLISTED` | 403 | Target recipient is not in the consenting allowlist (R-SEC-01, R-SEC-02) | Reject immediately; record `denied` ControlEvent; never dispatch |
| `IDEMPOTENCY_CONFLICT` | 409 | Idempotency key was already executed with a different payload/operation (E5 Invariant 1) | Reject; never dispatch |
| `DOWNSTREAM_OUTCOME_UNKNOWN` | 504 | Downstream engine timed out or dropped connection mid-dispatch (E5 Invariant 8) | Record `outcome="unknown"`; fail closed or require reconciliation; retry MUST reuse exact same `idempotencyKey` |
| `DOWNSTREAM_ERROR` | 502 | Downstream engine returned a confirmed error (4xx/5xx) | Record `ACTION_FAILED` with `outcome="error"`; surface error & retry status to SafiChat |
| `STORE_ERROR` | 500 | Local SQLite operational store encountered corruption or I/O failure (E4, E6) | Transition affected conversation to `FAILED_CLOSED`; never dispatch |
| `CONTRACT_VALIDATION_ERROR` | 422 | Payload or ControlEvent failed strict JSON Schema validation (`additionalProperties: false`) | Reject immediately; never emit invalid events to JetStream |
