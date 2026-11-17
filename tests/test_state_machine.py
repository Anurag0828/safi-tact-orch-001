"""Tests for Part E state machine, transition guards, and lifecycle assertions."""

import pytest

from gate.errors import InvalidModeTransitionError
from gate.models import ActionState, ConversationMode
from gate.state_machine import (
    assert_valid_action_transition,
    assert_valid_mode_transition,
)


def test_valid_mode_transitions():
    assert_valid_mode_transition(ConversationMode.UNGATED, ConversationMode.TRANSITIONING)
    assert_valid_mode_transition(ConversationMode.TRANSITIONING, ConversationMode.GATED)
    assert_valid_mode_transition(ConversationMode.GATED, ConversationMode.TRANSITIONING)
    assert_valid_mode_transition(ConversationMode.TRANSITIONING, ConversationMode.UNGATED)
    assert_valid_mode_transition(ConversationMode.UNGATED, ConversationMode.FAILED_CLOSED)
    assert_valid_mode_transition(ConversationMode.FAILED_CLOSED, ConversationMode.UNGATED)


def test_invalid_mode_transitions():
    # Direct jump from UNGATED to GATED without TRANSITIONING is forbidden (E4)
    with pytest.raises(InvalidModeTransitionError):
        assert_valid_mode_transition(ConversationMode.UNGATED, ConversationMode.GATED)

    # Direct jump from GATED to UNGATED without TRANSITIONING is forbidden (E4)
    with pytest.raises(InvalidModeTransitionError):
        assert_valid_mode_transition(ConversationMode.GATED, ConversationMode.UNGATED)


def test_action_lifecycle_transitions():
    assert_valid_action_transition(ActionState.RECEIVED, ActionState.HELD)
    assert_valid_action_transition(ActionState.HELD, ActionState.APPROVED)
    assert_valid_action_transition(ActionState.APPROVED, ActionState.AUTHORIZED)
    assert_valid_action_transition(ActionState.AUTHORIZED, ActionState.DISPATCHING)
    assert_valid_action_transition(ActionState.DISPATCHING, ActionState.DONE)

    with pytest.raises(ValueError):
        # Cannot jump from RECEIVED directly to DONE
        assert_valid_action_transition(ActionState.RECEIVED, ActionState.DONE)
