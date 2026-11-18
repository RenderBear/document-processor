"""Stateless signed upload grants for the presigned two-step upload flow.

A grant binds a caller-authorized storage key, media type, artifact lifecycle,
and size ceiling to a tenant, signed with an HMAC secret and given a short
lifetime. It lets the browser upload bytes directly to object storage (or, in
local development, to a backend receiver) without the backend proxying the body,
while the backend still authorizes the key and verifies the object afterwards.
The grant is self-contained: no server-side upload table is required.
"""

from __future__ import annotations

import base64
import hmac
from datetime import datetime
from hashlib import sha256

from .models import ArtifactRetention, StrictModel, utc_now


class UploadGrantError(ValueError):
    """A signed upload grant is malformed, tampered with, or expired."""


class UploadClaims(StrictModel):
    """The signed contents of an upload grant."""

    tenant_id: str
    storage_key: str
    filename: str
    media_type: str
    max_bytes: int
    retention: ArtifactRetention
    owner_id: str | None = None
    producer_kind: str | None = None
    expires_at: datetime


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _signature(secret: str, payload: bytes) -> bytes:
    return hmac.new(secret.encode("utf-8"), payload, sha256).digest()


def sign_upload_grant(secret: str, claims: UploadClaims) -> str:
    """Return a URL-safe ``<payload>.<signature>`` grant token."""

    if not secret:
        raise ValueError("upload signing secret must not be empty")
    payload = claims.model_dump_json().encode("utf-8")
    return f"{_b64encode(payload)}.{_b64encode(_signature(secret, payload))}"


def verify_upload_grant(secret: str, token: str, *, now: datetime | None = None) -> UploadClaims:
    """Validate a grant token's signature and expiry and return its claims."""

    if not secret:
        raise ValueError("upload signing secret must not be empty")
    try:
        payload_part, signature_part = token.split(".", 1)
        payload = _b64decode(payload_part)
        signature = _b64decode(signature_part)
    except (ValueError, TypeError) as exc:
        raise UploadGrantError("malformed upload token") from exc
    if not hmac.compare_digest(_signature(secret, payload), signature):
        raise UploadGrantError("invalid upload token signature")
    try:
        claims = UploadClaims.model_validate_json(payload)
    except ValueError as exc:
        raise UploadGrantError("invalid upload token claims") from exc
    if claims.expires_at < (now or utc_now()):
        raise UploadGrantError("upload token has expired")
    return claims
