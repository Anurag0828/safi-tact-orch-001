"""Permission recheck and authorization provider (R-GATE-09, R-AUTH-01, R-AUTH-02, R-SEC-04)."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

from gate.config import GateConfig
from gate.errors import (
    IdentityUncertainError,
    PermissionCheckFailedError,
    UnauthorizedError,
)
from gate.models import ActorContext, ActorKind

logger = logging.getLogger("gate.authz")


class PermissionProvider(ABC):
    @abstractmethod
    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        """Returns True if authorized, raises UnauthorizedError or PermissionCheckFailedError if denied/uncertain."""
        pass


class LocalPolicyPermissionProvider(PermissionProvider):
    """Local policy engine for testing and autonomous execution.
    Enforces conversation-level access lists, proxy validation, and revocations.
    """

    def __init__(self) -> None:
        # Map conversation_id -> set of allowed actor GUIDs (empty set = any valid non-revoked actor)
        self.conversation_access_map: dict[str, set[str]] = {}
        # Revoked actors: immediately blocks subsequent actions (R-AUTH-02)
        self.revoked_actors: set[str] = set()
        # Simulated failure switch to test R-SEC-04 (uncertainty = fail closed)
        self.simulate_check_failure: bool = False

    def authorize_actor_for_conversation(self, conversation_id: str, actor_guid: str) -> None:
        if conversation_id not in self.conversation_access_map:
            self.conversation_access_map[conversation_id] = set()
        self.conversation_access_map[conversation_id].add(actor_guid)

    def revoke_actor(self, actor_guid: str) -> None:
        self.revoked_actors.add(actor_guid)

    def restore_actor(self, actor_guid: str) -> None:
        self.revoked_actors.discard(actor_guid)

    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        if self.simulate_check_failure:
            raise PermissionCheckFailedError(
                "Simulated SAFI authz service timeout/uncertainty (R-SEC-04)"
            )

        if not actor.actorGuid:
            raise IdentityUncertainError("Actor GUID is required and cannot be empty")

        # R-AUTH-02: Revoked permission blocks subsequent actions
        if actor.actorGuid in self.revoked_actors:
            logger.warning(
                "Access DENIED: Actor '%s' has revoked permissions", actor.actorGuid
            )
            raise UnauthorizedError(
                f"Actor '{actor.actorGuid}' permissions have been revoked"
            )

        # Check conversation-level access (R-AUTH-01)
        allowed = self.conversation_access_map.get(conversation_id)
        if allowed is not None and actor.actorGuid not in allowed:
            logger.warning(
                "Access DENIED: Actor '%s' not authorized for conversation '%s'",
                actor.actorGuid,
                conversation_id,
            )
            raise UnauthorizedError(
                f"Actor '{actor.actorGuid}' is unauthorized for conversation '{conversation_id}'"
            )

        logger.debug(
            "Access GRANTED: Actor '%s' (%s) for conv '%s', op '%s'",
            actor.actorGuid,
            actor.actorKind.value,
            conversation_id,
            operation,
        )
        return True


class HttpPermissionProvider(PermissionProvider):
    """Calls live SAFI permission recheck endpoint when SAFI_AUTHZ_URL is configured."""

    def __init__(self, config: GateConfig) -> None:
        self.config = config
        self.fallback = LocalPolicyPermissionProvider()

    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        if not self.config.safi_authz_url:
            return await self.fallback.check_permission(actor, conversation_id, operation)

        url = f"{self.config.safi_authz_url}/check"
        headers = {}
        if self.config.safi_authz_token:
            headers["Authorization"] = f"Bearer {self.config.safi_authz_token}"

        payload = {
            "actorGuid": actor.actorGuid,
            "actorKind": actor.actorKind.value,
            "conversationId": conversation_id,
            "operation": operation,
            "context": actor.to_event_context(),
        }

        try:
            async with httpx.AsyncClient(timeout=self.config.authz_timeout_seconds) as client:
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("allowed") is True:
                        return True
                    raise UnauthorizedError("SAFI authz rejected request")
                if resp.status_code == 403:
                    raise UnauthorizedError("SAFI authz returned 403 Forbidden")
                raise PermissionCheckFailedError(
                    f"SAFI authz returned unexpected status {resp.status_code}"
                )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            # Uncertain outcome = FAIL CLOSED (R-SEC-04)
            raise PermissionCheckFailedError(f"SAFI authz network error: {exc}") from exc
