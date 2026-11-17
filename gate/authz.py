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

_READ_ONLY_OPERATIONS = frozenset(
    {
        "calnode:list_slots",
        "windmill:get_job_status",
        "gate:list_held",
    }
)

_HUMAN_ONLY_GATE_OPERATIONS = frozenset(
    {
        "gate:barge_in",
        "gate:approve_action",
        "gate:reject_action",
        "gate:return_to_ungated",
        "gate:reconcile",
    }
)


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


def _check_operation_scope(actor: ActorContext, operation: str) -> None:
    """Enforces authorized workflow/operation scope from permissionProvenance (SOW §4)."""
    prov = actor.permissionProvenance or {}
    allowed_ops = prov.get("allowedOperations")
    if isinstance(allowed_ops, list):
        if "*" not in allowed_ops and operation not in allowed_ops:
            raise UnauthorizedError(
                f"Actor '{actor.actorGuid}' is not permitted to invoke operation '{operation}'"
            )
    elif actor.actorKind == ActorKind.PROXY and operation in _HUMAN_ONLY_GATE_OPERATIONS:
        raise UnauthorizedError(
            f"Proxy actor '{actor.actorGuid}' cannot perform human gate operation '{operation}'"
        )

    scope = str(prov.get("scope", "")).strip().lower()
    if scope in ("read_only", "conversation:read") and operation not in _READ_ONLY_OPERATIONS:
        raise UnauthorizedError(
            f"Actor '{actor.actorGuid}' has read-only scope and cannot perform '{operation}'"
        )


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

    async def can_access_conversation(
        self,
        actor: ActorContext,
        conversation_id: str,
        owner_guid: str | None = None,
    ) -> bool:
        """Non-mutating visibility check used to scope Subject-Line lookups to caller-owned conversations."""
        return True


class LocalPolicyPermissionProvider(PermissionProvider):
    """Fail-closed (default-DENY) local policy engine for local pilot & automated tests.

    Security ordering (prevents cross-conversation DoS via IdentityUncertainError):
    1. Reject empty actorGuid immediately (IdentityUncertainError without mutating conversations).
    2. Check actor revocation & conversation ACL / bootstrap ownership FIRST -> unauthorized callers
       always receive 403 UNAUTHORIZED and can NEVER trigger FAILED_CLOSED on another user's conversation.
    3. Validate mandatory Meeting/Port/Permission Context for the authorized actor (R-CTX-01, R-SEC-04).
    4. Enforce operation/role scope (SOW §4).
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

    async def can_access_conversation(
        self,
        actor: ActorContext,
        conversation_id: str,
        owner_guid: str | None = None,
    ) -> bool:
        """Non-mutating check: returns True iff actor already owns/can access this existing conversation."""
        if not actor.actorGuid or actor.actorGuid in self.revoked_actors:
            return False
        allowed = self.conversation_access_map.get(conversation_id)
        if allowed is not None:
            if actor.actorKind == ActorKind.PROXY:
                if actor.actorGuid not in self.proxy_actors:
                    return False
                delegated_by = (actor.permissionProvenance or {}).get("delegatedBy")
                if delegated_by:
                    return delegated_by not in self.revoked_actors and delegated_by in allowed
                return actor.actorGuid in allowed
            return actor.actorGuid in allowed
        if owner_guid:
            if actor.actorKind == ActorKind.PROXY:
                if actor.actorGuid not in self.proxy_actors:
                    return False
                delegated_by = (actor.permissionProvenance or {}).get("delegatedBy")
                return bool(delegated_by and delegated_by == owner_guid and delegated_by not in self.revoked_actors)
            return actor.actorGuid == owner_guid
        # Unowned legacy row: only bootstrap actors may view/claim
        if actor.actorKind == ActorKind.USER:
            return actor.actorGuid in self.bootstrap_actors
        return False

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

        if not actor.actorGuid or not actor.actorGuid.strip():
            raise IdentityUncertainError("Actor GUID is required and cannot be empty")

        # 1. R-AUTH-02: Revoked permission blocks subsequent actions immediately
        if actor.actorGuid in self.revoked_actors:
            logger.warning("Access DENIED: Actor '%s' has revoked permissions", actor.actorGuid)
            raise UnauthorizedError(
                f"Actor '{actor.actorGuid}' permissions have been revoked"
            )

        # 2. Check ownership / ACL BEFORE context validation so unauthorized callers cannot DoS another conversation
        pin_owner: str | None = None
        if actor.actorKind == ActorKind.PROXY:
            if actor.actorGuid not in self.proxy_actors:
                logger.warning("Access DENIED: Unregistered proxy actor '%s'", actor.actorGuid)
                raise UnauthorizedError(
                    f"Proxy actor '{actor.actorGuid}' is not authorized"
                )
            delegated_by = (actor.permissionProvenance or {}).get("delegatedBy")
            allowed = self.conversation_access_map.get(conversation_id)
            if delegated_by:
                if delegated_by in self.revoked_actors:
                    raise UnauthorizedError(
                        f"Delegating actor '{delegated_by}' permissions have been revoked"
                    )
                if allowed is not None:
                    if delegated_by not in allowed:
                        raise UnauthorizedError(
                            f"Delegating actor '{delegated_by}' is unauthorized for conversation '{conversation_id}'"
                        )
                else:
                    # Fail-closed: unmapped conversation via proxy requires bootstrap-authorized delegator
                    if delegated_by not in self.bootstrap_actors:
                        raise UnauthorizedError(
                            f"Delegating actor '{delegated_by}' is not authorized to initialize conversation '{conversation_id}'"
                        )
                    pin_owner = delegated_by
            else:
                if allowed is None or actor.actorGuid not in allowed:
                    raise UnauthorizedError(
                        f"Proxy actor '{actor.actorGuid}' lacks explicit access for conversation '{conversation_id}'"
                    )
        else:
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
            elif actor.actorGuid in self.bootstrap_actors:
                pin_owner = actor.actorGuid
            else:
                logger.warning(
                    "Access DENIED (default-deny): Actor '%s' not authorized for conversation '%s'",
                    actor.actorGuid,
                    conversation_id,
                )
                raise UnauthorizedError(
                    f"Actor '{actor.actorGuid}' is not authorized for conversation '{conversation_id}'"
                )

        # 3. Now that actor is confirmed authorized for this conversation, verify mandatory historical context (R-CTX-01)
        validate_actor_context(actor)

        # 4. Verify operation scope (SOW §4)
        _check_operation_scope(actor, operation)

        # 5. Pin ownership only after ALL checks pass
        if pin_owner is not None:
            self.conversation_access_map[conversation_id] = {pin_owner}
            logger.debug(
                "Pinned conversation '%s' ownership to actor '%s'",
                conversation_id,
                pin_owner,
            )

        return True


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

    async def can_access_conversation(
        self,
        actor: ActorContext,
        conversation_id: str,
        owner_guid: str | None = None,
    ) -> bool:
        if not self.config.safi_authz_url:
            if self.local_policy is not None:
                return await self.local_policy.can_access_conversation(
                    actor, conversation_id, owner_guid=owner_guid
                )
            return False
        try:
            await self._call_remote_authz(actor, conversation_id, "gate:list_held")
            return True
        except Exception:
            return False

    async def _call_remote_authz(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
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

    async def check_permission(
        self,
        actor: ActorContext,
        conversation_id: str,
        operation: str,
    ) -> bool:
        if not self.config.safi_authz_url:
            if self.local_policy is not None:
                return await self.local_policy.check_permission(actor, conversation_id, operation)
            raise PermissionCheckFailedError(
                "SAFI_AUTHZ_URL is not configured and AUTHZ_MODE is not 'local_acl'; failing closed (R-SEC-04)"
            )

        if not actor.actorGuid or not actor.actorGuid.strip():
            raise IdentityUncertainError("Actor GUID is required and cannot be empty")

        # Check remote SAFI authorization FIRST so unauthorized callers cannot fail-close a victim's conversation
        await self._call_remote_authz(actor, conversation_id, operation)
        validate_actor_context(actor)
        _check_operation_scope(actor, operation)
        return True
