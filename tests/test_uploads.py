from __future__ import annotations

from datetime import timedelta

import pytest

from doc_processing.models import PDF, ArtifactRetention, utc_now
from doc_processing.storage import LocalArtifactStorage, S3ArtifactStorage
from doc_processing.uploads import (
    UploadClaims,
    UploadGrantError,
    sign_upload_grant,
    verify_upload_grant,
)

SECRET = "unit-upload-secret"


def _claims(**overrides) -> UploadClaims:
    base = dict(
        tenant_id="tenant-1",
        storage_key="sources/background/t/j/document.pdf",
        filename="document.pdf",
        media_type=PDF,
        max_bytes=1_000_000,
        retention=ArtifactRetention.BACKGROUND,
        expires_at=utc_now() + timedelta(minutes=10),
    )
    base.update(overrides)
    return UploadClaims(**base)


def test_sign_and_verify_roundtrip() -> None:
    token = sign_upload_grant(SECRET, _claims(producer_kind="chat", owner_id="s1",
                                              retention=ArtifactRetention.SESSION_BOUND))
    claims = verify_upload_grant(SECRET, token)
    assert claims.storage_key == "sources/background/t/j/document.pdf"
    assert claims.producer_kind == "chat"
    assert claims.retention is ArtifactRetention.SESSION_BOUND


def test_tampered_payload_is_rejected() -> None:
    token = sign_upload_grant(SECRET, _claims())
    payload, signature = token.split(".", 1)
    forged = payload[:-1] + ("A" if payload[-1] != "A" else "B") + "." + signature
    with pytest.raises(UploadGrantError):
        verify_upload_grant(SECRET, forged)


def test_wrong_secret_is_rejected() -> None:
    token = sign_upload_grant(SECRET, _claims())
    with pytest.raises(UploadGrantError):
        verify_upload_grant("another-secret", token)


def test_expired_grant_is_rejected() -> None:
    token = sign_upload_grant(SECRET, _claims(expires_at=utc_now() - timedelta(seconds=1)))
    with pytest.raises(UploadGrantError, match="expired"):
        verify_upload_grant(SECRET, token)


def test_malformed_token_is_rejected() -> None:
    with pytest.raises(UploadGrantError):
        verify_upload_grant(SECRET, "not-a-valid-token")


# -- storage presign ---------------------------------------------------------


@pytest.mark.asyncio
async def test_local_storage_has_no_direct_presign(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    result = await storage.presign_put(
        "sources/x/document.pdf", content_type=PDF, max_bytes=100, expires_in=60
    )
    assert result is None


class _FakeS3Client:
    def generate_presigned_url(self, operation, *, Params, ExpiresIn):  # noqa: N803
        assert operation == "put_object"
        assert ExpiresIn == 60
        return f"https://s3.example/{Params['Bucket']}/{Params['Key']}?sig=abc"


@pytest.mark.asyncio
async def test_s3_storage_presigns_direct_put() -> None:
    storage = S3ArtifactStorage("bucket", client=_FakeS3Client())
    url = await storage.presign_put(
        "sources/x/document.pdf", content_type=PDF, max_bytes=100, expires_in=60
    )
    assert url == "https://s3.example/bucket/sources/x/document.pdf?sig=abc"
