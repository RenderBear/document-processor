"""Artifact storage contracts for local development and S3-compatible systems."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from botocore.exceptions import ClientError

from .models import StoredArtifact


class ArtifactStorage(Protocol):
    async def put_bytes(self, key: str, content: bytes, content_type: str) -> StoredArtifact: ...

    async def get_bytes(self, key: str) -> bytes: ...

    async def exists(self, key: str) -> bool: ...

    async def delete(self, key: str) -> None: ...

    async def verify(self, key: str, *, size_bytes: int, checksum_sha256: str) -> bool: ...

    async def size(self, key: str) -> int | None: ...

    def uri_for(self, key: str) -> str: ...

    async def presign_put(
        self, key: str, *, content_type: str, max_bytes: int, expires_in: int
    ) -> str | None: ...


def _validated_key(key: str) -> PurePosixPath:
    path = PurePosixPath(key)
    if not path.parts or path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise ValueError("artifact key must be a non-empty relative path without traversal")
    return path


class LocalArtifactStorage:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root.joinpath(*_validated_key(key).parts)

    async def put_bytes(self, key: str, content: bytes, content_type: str) -> StoredArtifact:
        path = self._path(key)

        def write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.tmp")
            temporary.write_bytes(content)
            temporary.replace(path)

        await asyncio.to_thread(write)
        return StoredArtifact(
            key=key,
            uri=path.as_uri(),
            content_type=content_type,
            size_bytes=len(content),
        )

    async def get_bytes(self, key: str) -> bytes:
        return await asyncio.to_thread(self._path(key).read_bytes)

    async def exists(self, key: str) -> bool:
        return await asyncio.to_thread(self._path(key).is_file)

    async def delete(self, key: str) -> None:
        path = self._path(key)

        def remove() -> None:
            path.unlink(missing_ok=True)
            parent = path.parent
            while parent != self.root:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent

        await asyncio.to_thread(remove)

    async def verify(self, key: str, *, size_bytes: int, checksum_sha256: str) -> bool:
        path = self._path(key)

        def inspect() -> bool:
            if not path.is_file() or path.stat().st_size != size_bytes:
                return False
            digest = hashlib.sha256()
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
            return digest.hexdigest() == checksum_sha256

        return await asyncio.to_thread(inspect)

    async def size(self, key: str) -> int | None:
        path = self._path(key)

        def stat() -> int | None:
            return path.stat().st_size if path.is_file() else None

        return await asyncio.to_thread(stat)

    def uri_for(self, key: str) -> str:
        return self._path(key).as_uri()

    async def presign_put(
        self, key: str, *, content_type: str, max_bytes: int, expires_in: int
    ) -> str | None:
        # The local filesystem has no direct upload endpoint; the API serves a
        # backend receiver instead. Returning None signals that fallback.
        _validated_key(key)
        return None


class S3ArtifactStorage:
    """Boto3-backed storage, including SeaweedFS's S3 gateway."""

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        client: Any | None = None,
        **client_options: Any,
    ) -> None:
        self.bucket = bucket
        if client is None:
            import boto3

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region_name,
                **client_options,
            )
        self.client = client

    async def put_bytes(self, key: str, content: bytes, content_type: str) -> StoredArtifact:
        normalized = str(_validated_key(key))
        checksum = hashlib.sha256(content).hexdigest()
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=normalized,
            Body=content,
            ContentType=content_type,
            Metadata={"sha256": checksum},
        )
        return StoredArtifact(
            key=normalized,
            uri=f"s3://{self.bucket}/{normalized}",
            content_type=content_type,
            size_bytes=len(content),
        )

    async def get_bytes(self, key: str) -> bytes:
        normalized = str(_validated_key(key))
        response = await asyncio.to_thread(
            self.client.get_object,
            Bucket=self.bucket,
            Key=normalized,
        )
        return await asyncio.to_thread(response["Body"].read)

    async def exists(self, key: str) -> bool:
        normalized = str(_validated_key(key))
        try:
            await asyncio.to_thread(
                self.client.head_object,
                Bucket=self.bucket,
                Key=normalized,
            )
            return True
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    async def delete(self, key: str) -> None:
        normalized = str(_validated_key(key))
        await asyncio.to_thread(
            self.client.delete_object,
            Bucket=self.bucket,
            Key=normalized,
        )

    async def verify(self, key: str, *, size_bytes: int, checksum_sha256: str) -> bool:
        normalized = str(_validated_key(key))
        try:
            response = await asyncio.to_thread(
                self.client.get_object,
                Bucket=self.bucket,
                Key=normalized,
            )
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

        def inspect() -> bool:
            body = response["Body"]
            digest = hashlib.sha256()
            received = 0
            while chunk := body.read(1024 * 1024):
                received += len(chunk)
                digest.update(chunk)
            return received == size_bytes and digest.hexdigest() == checksum_sha256

        return await asyncio.to_thread(inspect)

    async def size(self, key: str) -> int | None:
        normalized = str(_validated_key(key))
        try:
            response = await asyncio.to_thread(
                self.client.head_object,
                Bucket=self.bucket,
                Key=normalized,
            )
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = exc.response.get("Error", {}).get("Code")
            if status == 404 or code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise
        return response["ContentLength"]

    def uri_for(self, key: str) -> str:
        return f"s3://{self.bucket}/{str(_validated_key(key))}"

    async def presign_put(
        self, key: str, *, content_type: str, max_bytes: int, expires_in: int
    ) -> str | None:
        normalized = str(_validated_key(key))
        # Sign only bucket and key: the client PUTs the bytes directly and the
        # backend verifies size and structure afterwards. Content-Length is not
        # bound into the signature, so oversize is caught at job submission.
        return await asyncio.to_thread(
            self.client.generate_presigned_url,
            "put_object",
            Params={"Bucket": self.bucket, "Key": normalized},
            ExpiresIn=expires_in,
        )
