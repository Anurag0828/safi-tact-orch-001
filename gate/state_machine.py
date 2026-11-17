"""Per-conversation state machine and concurrency primitives (Part E2, E4, R-GATE-07, R-GATE-08)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from gate.errors import InvalidModeTransitionError
from gate.models import ActionState, ConversationMode


# Valid conversation mode transitions per Part E4 transition table
ALLOWED_MODE_TRANSITIONS: dict[ConversationMode, set[ConversationMode]] = {
    ConversationMode.UNGATED: {
        ConversationMode.TRANSITIONING,
        ConversationMode.FAILED_CLOSED,
    },
    ConversationMode.TRANSITIONING: {
        ConversationMode.GATED,
        ConversationMode.UNGATED,
        ConversationMode.FAILED_CLOSED,
    },
    ConversationMode.GATED: {
        ConversationMode.TRANSITIONING,
        ConversationMode.FAILED_CLOSED,
    },
    ConversationMode.FAILED_CLOSED: {
        ConversationMode.UNGATED,
        ConversationMode.GATED,
        ConversationMode.FAILED_CLOSED,
    },
}

# Valid action lifecycle transitions per Part E3
ALLOWED_ACTION_TRANSITIONS: dict[ActionState, set[ActionState]] = {
    ActionState.RECEIVED: {ActionState.HELD, ActionState.AUTHORIZED, ActionState.FAILED},
    ActionState.HELD: {ActionState.APPROVED, ActionState.REJECTED, ActionState.CANCELLED},
    ActionState.APPROVED: {ActionState.AUTHORIZED, ActionState.FAILED},
    ActionState.AUTHORIZED: {ActionState.DISPATCHING, ActionState.FAILED},
    ActionState.DISPATCHING: {ActionState.DONE, ActionState.FAILED},
    ActionState.DONE: set(),
    ActionState.FAILED: set(),
    ActionState.REJECTED: set(),
    ActionState.CANCELLED: set(),
}


def assert_valid_mode_transition(
    current: ConversationMode, target: ConversationMode
) -> None:
    if target not in ALLOWED_MODE_TRANSITIONS[current]:
        raise InvalidModeTransitionError(current.value, target.value)


def assert_valid_action_transition(
    current: ActionState, target: ActionState
) -> None:
    if target not in ALLOWED_ACTION_TRANSITIONS[current]:
        raise ValueError(
            f"Illegal action state transition: {current.value} -> {target.value}"
        )


@dataclass
class ConversationRuntime:
    """Isolated per-conversation synchronization primitives (NO global lock - E1, Invariant 5)."""

    conversation_id: str
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    dispatch_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight_action_ids: set[str] = field(default_factory=set)
    inflight_zero: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self) -> None:
        self.inflight_zero.set()

    def mark_inflight(self, action_id: str) -> None:
        self.inflight_action_ids.add(action_id)
        self.inflight_zero.clear()

    def mark_settled(self, action_id: str) -> None:
        self.inflight_action_ids.discard(action_id)
        if not self.inflight_action_ids:
            self.inflight_zero.set()


class ConversationRuntimeRegistry:
    """Registry holding per-conversation runtime locks and in-flight trackers."""

    def __init__(self) -> None:
        self._runtimes: dict[str, ConversationRuntime] = {}

    def get(self, conversation_id: str) -> ConversationRuntime:
        runtime = self._runtimes.get(conversation_id)
        if runtime is None:
            runtime = ConversationRuntime(conversation_id=conversation_id)
            self._runtimes[conversation_id] = runtime
        return runtime

    def clear(self) -> None:
        self._runtimes.clear()
