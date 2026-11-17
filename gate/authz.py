"""Permission recheck and authorization provider (R-GATE-09, R-AUTH-01, R-AUTH-02, R-SEC-04)."""

from __future__ import annotations

import json
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


def validate_actor_context(actor: ActorContext) -> None:
    """Fails closed with IdentityUncertainError if GUID or mandatory Context is missing (R-CTX-01, R-SEC-04)."""
    if not actor.actorGuid or not actor.actorGuid.strip():
        raise IdentityUncertainError("Actor GUID is required and cannot be empty")
    if not actor.meetingContext:
        raise IdentityUncertainError("Historical meetingContext is required on every action (R-CTX-01)")
    if not actor.portContext:
        raise IdentityUncertainError("Historical portContext is required on every action (R-CTX-01)")
    if not actor.permissionProvenance:
        raise IdentityUncertainError("permissionProvenance is required on every action (R-CTX-01)")


class PermissionProvider(ABC):
    @abstractmethod
    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        """Returns True if authorized; raises UnauthorizedError, IdentityUncertainError, or PermissionCheckFailedError."""
        pass


class LocalPolicyPermissionProvider(PermissionProvider):
    """Fail-closed (default-DENY) local policy engine for local pilot & automated tests.

    Security model (R-AUTH-01, R-AUTH-02, R-SEC-04):
    - Every actor must present complete GUID + Meeting/Port/Permission Context.
    - Revoked actors are denied immediately (including proxies acting on their behalf).
    - If a conversation already has an explicit access list in `conversation_access_map`,
      ONLY actors in that list (or authorized proxies acting for an allowed, non-revoked owner) are permitted.
    - If a conversation has no access list yet, ONLY actors in `bootstrap_actors` may initialize it,
      which automatically pins ownership to that actor so other users cannot access it.
    - Any unknown actor is DENIED by default.
    """

    def __init__(
        self,
        bootstrap_actors: set[str] | None = None,
        proxy_actors: set[str] | None = None,
        conversation_access_map: dict[str, set[str]] | None = None,
        revoked_actors: set[str] | None = None,
    ) -> None:
        self.conversation_access_map: dict[str, set[str]] = (
            {k: set(v) for k, v in conversation_access_map.items()}
            if conversation_access_map
            else {}
        )
        self.bootstrap_actors: set[str] = set(bootstrap_actors or ())
        self.proxy_actors: set[str] = set(proxy_actors or ())
        self.revoked_actors: set[str] = set(revoked_actors or ())
        self.simulate_check_failure: bool = False

    @classmethod
    def from_json(cls, raw_json: str) -> "LocalPolicyPermissionProvider":
        if not raw_json or not raw_json.strip():
            return cls()
        try:
            data = json.loads(raw_json)
        except ValueError as exc:
            raise PermissionCheckFailedError(f"Invalid LOCAL_ACL_JSON configuration: {exc}") from exc
        if not isinstance(data, dict):
            raise PermissionCheckFailedError("LOCAL_ACL_JSON must be a JSON object")
        conv_map = {
            str(cid): {str(a) for a in actors}
            for cid, actors in (data.get("conversations") or {}).items()
            if isinstance(actors, list)
        }
        return cls(
            bootstrap_actors={str(a) for a in (data.get("bootstrapActors") or [])},
            proxy_actors={str(a) for a in (data.get("proxyActors") or [])},
            conversation_access_map=conv_map,
            revoked_actors={str(a) for a in (data.get("revokedActors") or [])},
        )

    def authorize_actor_for_conversation(self, conversation_id: str, actor_guid: str) -> None:
        self.conversation_access_map.setdefault(conversation_id, set()).add(actor_guid)

    def authorize_bootstrap_actor(self, actor_guid: str) -> None:
        self.bootstrap_actors.add(actor_guid)

    def authorize_proxy_actor(self, proxy_guid: str) -> None:
        self.proxy_actors.add(proxy_guid)

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

        validate_actor_context(actor)

        # R-AUTH-02: Revoked permission blocks subsequent actions immediately
        if actor.actorGuid in self.revoked_actors:
            logger.warning("Access DENIED: Actor '%s' has revoked permissions", actor.actorGuid)
            raise UnauthorizedError(
                f"Actor '{actor.actorGuid}' permissions have been revoked"
            )

        # PROXY actor validation: must be explicitly registered & delegating user must be authorized/unrevoked
        if actor.actorKind == ActorKind.PROXY:
            if actor.actorGuid not in self.proxy_actors:
                logger.warning("Access DENIED: Unregistered proxy actor '%s'", actor.actorGuid)
                raise UnauthorizedError(
                    f"Proxy actor '{actor.actorGuid}' is not authorized"
                )
            delegated_by = actor.permissionProvenance.get("delegatedBy")
            if delegated_by:
                if delegated_by in self.revoked_actors:
                    raise UnauthorizedError(
                        f"Delegating actor '{delegated_by}' permissions have been revoked"
                    )
                allowed = self.conversation_access_map.get(conversation_id)
                if allowed is not None and delegated_by not in allowed:
                    raise UnauthorizedError(
                        f"Delegating actor '{delegated_by}' is unauthorized for conversation '{conversation_id}'"
                    )
            else:
                # Without delegatedBy, proxy may only act on conversations where it is explicitly listed
                allowed = self.conversation_access_map.get(conversation_id)
                if allowed is None or actor.actorGuid not in allowed:
                    raise UnauthorizedError(
                        f"Proxy actor '{actor.actorGuid}' lacks explicit access for conversation '{conversation_id}'"
                    )
            return True

        # USER actor validation (R-AUTH-01): strict per-conversation isolation & default-deny
        allowed = self.conversation_access_map.get(conversation_id)
        if allowed is not None:
            if actor.actorGuid not in allowed:
                logger.warning(
                    "Access DENIED: Actor '%s' not authorized for conversation '%s'",
                    actor.actorGuid,
                    conversation_id,
                )
                raise UnauthorizedError(
                    f"Actor '{actor.actorGuid}' is unauthorized for conversation '{conversation_id}'"
                )
            return True

        # New conversation not yet mapped: only bootstrap actors can claim/initialize ownership
        if actor.actorGuid in self.bootstrap_actors:
            self.conversation_access_map[conversation_id] = {actor.actorGuid}
            logger.debug(
                "Pinned conversation '%s' ownership to bootstrap actor '%s'",
                conversation_id,
                actor.actorGuid,
            )
            return True

        logger.warning(
            "Access DENIED (default-deny): Actor '%s' not authorized for conversation '%s'",
            actor.actorGuid,
            conversation_id,
        )
        raise UnauthorizedError(
            f"Actor '{actor.actorGuid}' is not authorized for conversation '{conversation_id}'"
        )


class HttpPermissionProvider(PermissionProvider):
    """Calls live SAFI permission recheck endpoint when SAFI_AUTHZ_URL is configured.

    Fail-closed by default (R-SEC-04): if SAFI_AUTHZ_URL is unset, only delegates to
    LocalPolicyPermissionProvider when AUTHZ_MODE=local_acl is explicitly configured
    (which itself is default-deny). Otherwise raises PermissionCheckFailedError.
    """

    def __init__(
        self,
        config: GateConfig,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self._client = http_client
        self.local_policy: LocalPolicyPermissionProvider | None = None
        if not self.config.safi_authz_url and self.config.authz_mode == "local_acl":
            self.local_policy = LocalPolicyPermissionProvider.from_json(self.config.local_acl_json)

    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        validate_actor_context(actor)

        if not self.config.safi_authz_url:
            if self.local_policy is not None:
                return await self.local_policy.check_permission(actor, conversation_id, operation)
            raise PermissionCheckFailedError(
                "SAFI_AUTHZ_URL is not configured and AUTHZ_MODE is not 'local_acl'; failing closed (R-SEC-04)"
            )

        url = f"{self.config.safi_authz_url}/check"
        headers = {"Content-Type": "application/json"}
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
            if self._client is not None:
                resp = await self._client.post(url, json=payload, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=self.config.authz_timeout_seconds) as client:
                    resp = await client.post(url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            raise PermissionCheckFailedError(f"SAFI authz network/HTTP error: {exc}") from exc

        if resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError as exc:
                raise PermissionCheckFailedError(
                    f"SAFI authz returned invalid JSON: {exc}"
                ) from exc
            if isinstance(data, dict) and data.get("allowed") is True:
                return True
            if isinstance(data, dict) and data.get("allowed") is False:
                raise UnauthorizedError("SAFI authz rejected request")
            raise PermissionCheckFailedError("SAFI authz returned ambiguous response payload")
        if resp.status_code == 403:
            raise UnauthorizedError("SAFI authz returned 403 Forbidden")
        if resp.status_code == 401:
            raise IdentityUncertainError("SAFI authz returned 401 Unauthorized / identity uncertain")
        raise PermissionCheckFailedError(
            f"SAFI authz returned unexpected status {resp.status_code}"
        )
