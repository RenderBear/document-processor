"""Backend-owned artifact lifecycle policy."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .models import ArtifactLifecycle, ArtifactRetention, ProducerKind


class ArtifactPolicyError(ValueError):
    """Raised when an upload cannot be assigned an authorized lifecycle."""


@dataclass(frozen=True)
class ArtifactLifecyclePolicy:
    """Map trusted producer identity to storage lifetime independently of UI state."""

    session_bound_producers: frozenset[ProducerKind] = frozenset({ProducerKind.CHAT})

    def authorize(
        self,
        producer_kind: ProducerKind | None,
        *,
        session_id: str | None,
    ) -> ArtifactLifecycle:
        if producer_kind in self.session_bound_producers:
            normalized = (session_id or "").strip()
            if not normalized:
                raise ArtifactPolicyError(
                    f"artifact_session_id is required for {producer_kind.value} uploads"
                )
            return ArtifactLifecycle(
                retention=ArtifactRetention.SESSION_BOUND,
                owner_id=normalized,
            )
        return ArtifactLifecycle(retention=ArtifactRetention.BACKGROUND)

    @staticmethod
    def source_key(tenant_id: str, job_id: str, lifecycle: ArtifactLifecycle) -> str:
        tenant_scope = hashlib.sha256(f"tenant\0{tenant_id}".encode()).hexdigest()
        if lifecycle.retention is ArtifactRetention.SESSION_BOUND:
            assert lifecycle.owner_id is not None
            session_scope = hashlib.sha256(
                f"session\0{lifecycle.owner_id}".encode()
            ).hexdigest()
            return f"sources/session/{tenant_scope}/{session_scope}/{job_id}"
        return f"sources/background/{tenant_scope}/{job_id}"
