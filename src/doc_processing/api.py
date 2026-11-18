"""FastAPI control plane for durable document ingestion and job observation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import Annotated
from urllib.parse import urlsplit
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

import httpx
from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .artifact_policy import ArtifactLifecyclePolicy, ArtifactPolicyError
from .config import OCRServiceConfig, load_config
from .dispatch import DispatchBroker, JetStreamDispatchBroker
from .events import JobEvent
from .job_store import InMemoryJobStore, JobStore, PostgresJobStore
from .models import (
    DOCX,
    PDF,
    PPTX,
    REGION_PATTERN,
    ArtifactLifecycle,
    DataClassification,
    DocumentReference,
    Job,
    JobPriority,
    JobState,
    ProducerKind,
    utc_now,
)
from .uploads import (
    UploadClaims,
    UploadGrantError,
    sign_upload_grant,
    verify_upload_grant,
)
from .orchestrator import OCRService
from .policy import (
    BuiltinPolicyEngine,
    PolicyContractError,
    PolicyEngine,
    PolicyEvaluationError,
    PolicySpec,
    load_policy,
)
from .storage import ArtifactStorage, LocalArtifactStorage, S3ArtifactStorage
from .webhooks import WebhookDispatcher

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MEDIA_BY_EXTENSION = {".pdf": PDF, ".docx": DOCX, ".pptx": PPTX}
PRIORITY_BY_NAME = {
    "bulk": JobPriority.BULK,
    "standard": JobPriority.STANDARD,
    "workflow": JobPriority.WORKFLOW,
    "interactive": JobPriority.INTERACTIVE,
}
TERMINAL_EVENT_TYPES = {
    "document.job.succeeded",
    "document.job.failed",
    "document.job.rejected",
    "document.job.cancelled",
}
UPLOAD_GRANT_TTL_SECONDS = 3600


class UploadRequest(BaseModel):
    """Metadata for a presigned upload grant; the file itself is uploaded next."""

    model_config = ConfigDict(extra="forbid")
    filename: str = Field(min_length=1, max_length=1024)
    tenant_id: str = Field(default="default", min_length=1, max_length=128)
    producer_kind: str | None = None
    artifact_session_id: str | None = Field(default=None, max_length=128)


def _media_type_for_filename(filename: str) -> tuple[str, str]:
    extension = Path((filename or "").replace("\\", "/").rsplit("/", 1)[-1]).suffix.lower()
    media_type = MEDIA_BY_EXTENSION.get(extension)
    if media_type is None:
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            "only PDF, DOCX, and PPTX uploads are supported",
        )
    return extension, media_type


def _normalize_producer(value: str | None) -> ProducerKind | None:
    if value is None:
        return None
    try:
        return ProducerKind(value.lower())
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "producer_kind must be one of: " + ", ".join(item.value for item in ProducerKind),
        ) from exc


def _normalize_classification(value: str | None) -> DataClassification | None:
    if value is None:
        return None
    try:
        return DataClassification(value.lower())
    except ValueError as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "data_classification must be one of: "
            + ", ".join(item.value for item in DataClassification),
        ) from exc


def _normalize_region(value: str | None) -> str | None:
    if value is None:
        return None
    region = value.strip().lower()
    if not re.fullmatch(REGION_PATTERN, region):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "home_region must be a lowercase region code",
        )
    return region


def _upload_byte_ceiling(config: OCRServiceConfig) -> int:
    return max(path.limits.max_document_bytes for path in config.enabled_paths)


async def _document_from_upload_file(
    request: Request,
    file: UploadFile,
    tenant_id: str,
    producer_kind: str | None,
    artifact_session_id: str | None,
    page_count: int,
) -> tuple[DocumentReference, str, ProducerKind | None, str]:
    """Proxy path: buffer, validate, and store a bounded upload through the API."""

    filename = file.filename or ""
    extension, expected_media_type = _media_type_for_filename(filename)
    media_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    if media_type != expected_media_type:
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            f"content type must be {expected_media_type} for {extension}",
        )
    normalized_producer = _normalize_producer(producer_kind)
    try:
        lifecycle = request.app.state.artifact_policy.authorize(
            normalized_producer, session_id=artifact_session_id
        )
    except ArtifactPolicyError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc

    storage = request.app.state.storage
    content = await _read_bounded(file, _upload_byte_ceiling(request.app.state.config))
    _validate_signature(content, extension)
    artifact_key = (
        request.app.state.artifact_policy.source_key(tenant_id, uuid4().hex, lifecycle)
        + f"/document{extension}"
    )
    checksum_sha256 = hashlib.sha256(content).hexdigest()
    artifact = None
    try:
        artifact = await storage.put_bytes(artifact_key, content, expected_media_type)
        verified = await storage.verify(
            artifact.key, size_bytes=len(content), checksum_sha256=checksum_sha256
        )
    except Exception as exc:
        if artifact is not None:
            with suppress(Exception):
                await storage.delete(artifact.key)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "object storage is unavailable"
        ) from exc
    if not verified:
        await storage.delete(artifact.key)
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "object storage did not durably verify the upload",
        )
    document = DocumentReference(
        uri=artifact.uri,
        storage_key=artifact.key,
        filename=_safe_display_filename(filename, extension),
        media_type=expected_media_type,
        size_bytes=len(content),
        page_count=page_count,
        checksum_sha256=checksum_sha256,
        lifecycle=lifecycle,
    )
    return document, tenant_id, normalized_producer, artifact.key


async def _document_from_grant(
    request: Request,
    upload_id: str,
    page_count: int,
) -> tuple[DocumentReference, str, ProducerKind | None, str]:
    """Presigned path: reference bytes already uploaded directly to storage."""

    secret = request.app.state.upload_secret
    if not secret:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "signed uploads are not configured"
        )
    try:
        claims = verify_upload_grant(secret, upload_id)
    except UploadGrantError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc

    storage = request.app.state.storage
    actual_size = await storage.size(claims.storage_key)
    if actual_size is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "referenced upload has not completed"
        )
    if actual_size == 0:
        with suppress(Exception):
            await storage.delete(claims.storage_key)
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "uploaded object is empty")
    if actual_size > claims.max_bytes:
        with suppress(Exception):
            await storage.delete(claims.storage_key)
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE, "uploaded object exceeds the permitted size"
        )
    document = DocumentReference(
        uri=storage.uri_for(claims.storage_key),
        storage_key=claims.storage_key,
        filename=claims.filename,
        media_type=claims.media_type,
        size_bytes=actual_size,
        page_count=page_count,
        checksum_sha256=None,
        lifecycle=ArtifactLifecycle(retention=claims.retention, owner_id=claims.owner_id),
    )
    return document, claims.tenant_id, _normalize_producer(claims.producer_kind), claims.storage_key


def _storage_from_environment() -> ArtifactStorage:
    backend = os.getenv("ARTIFACT_STORAGE_BACKEND", "local").lower()
    if backend == "local":
        return LocalArtifactStorage(os.getenv("LOCAL_ARTIFACT_ROOT", ".data/artifacts"))
    if backend == "s3":
        bucket = os.getenv("S3_BUCKET")
        if not bucket:
            raise RuntimeError("S3_BUCKET is required when ARTIFACT_STORAGE_BACKEND=s3")
        return S3ArtifactStorage(
            bucket,
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            region_name=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
        )
    raise RuntimeError(f"unsupported ARTIFACT_STORAGE_BACKEND: {backend}")


def _job_store_from_environment() -> JobStore:
    backend = os.getenv("JOB_STORE_BACKEND", "memory").lower()
    if backend == "memory":
        return InMemoryJobStore()
    if backend == "postgres":
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is required when JOB_STORE_BACKEND=postgres")
        return PostgresJobStore(database_url)
    raise RuntimeError(f"unsupported JOB_STORE_BACKEND: {backend}")


def _dispatch_from_environment() -> DispatchBroker | None:
    backend = os.getenv("DISPATCH_BACKEND", "memory").lower()
    if backend == "memory":
        return None
    if backend == "jetstream":
        servers = [
            item.strip()
            for item in os.getenv("NATS_URL", "nats://127.0.0.1:4222").split(",")
            if item.strip()
        ]
        return JetStreamDispatchBroker(
            servers,
            stream=os.getenv("JETSTREAM_STREAM", "DOCUMENT_PROCESSING"),
            subject=os.getenv("JETSTREAM_SUBJECT", "document.jobs.process"),
            durable=os.getenv("JETSTREAM_DURABLE", "document-processors"),
            replicas=int(os.getenv("JETSTREAM_REPLICAS", "1")),
            ack_wait_seconds=float(os.getenv("JETSTREAM_ACK_WAIT_SECONDS", "120")),
            max_deliver=int(os.getenv("JETSTREAM_MAX_DELIVER", "5")),
            max_age_seconds=float(os.getenv("JETSTREAM_MAX_AGE_SECONDS", "604800")),
            max_bytes=int(os.getenv("JETSTREAM_MAX_BYTES", str(10 * 1024 * 1024 * 1024))),
            max_messages=int(os.getenv("JETSTREAM_MAX_MESSAGES", "1000000")),
        )
    raise RuntimeError(f"unsupported DISPATCH_BACKEND: {backend}")


def _safe_display_filename(filename: str | None, extension: str) -> str:
    candidate = (filename or "").replace("\\", "/").rsplit("/", 1)[-1].strip()
    return candidate or f"document{extension}"


def _validate_signature(content: bytes, extension: str) -> None:
    if extension == ".pdf":
        if not content.startswith(b"%PDF-"):
            raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, "invalid PDF signature")
        return

    expected_member = "word/document.xml" if extension == ".docx" else "ppt/presentation.xml"
    try:
        with ZipFile(BytesIO(content)) as archive:
            if expected_member not in archive.namelist():
                raise HTTPException(
                    status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                    f"upload is not a valid {extension[1:].upper()} package",
                )
    except BadZipFile as exc:
        raise HTTPException(
            status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            f"upload is not a valid {extension[1:].upper()} package",
        ) from exc


async def _read_bounded(upload: UploadFile, limit: int) -> bytes:
    chunks: list[bytes] = []
    received = 0
    while chunk := await upload.read(min(1024 * 1024, limit + 1 - received)):
        received += len(chunk)
        if received > limit:
            raise HTTPException(
                status.HTTP_413_CONTENT_TOO_LARGE,
                f"upload exceeds the configured {limit}-byte ceiling",
            )
        chunks.append(chunk)
    if not chunks:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "upload must not be empty")
    return b"".join(chunks)


def _event_payload(event: JobEvent) -> str:
    data = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
    return f"id: {event.event_id}\nevent: {event.event_type}\ndata: {data}\n\n"


def _job_urls(request: Request, job: Job) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "state": job.state.value,
        "status_url": str(request.url_for("get_job", job_id=job.job_id)),
        "events_url": str(request.url_for("get_job_events", job_id=job.job_id)),
        "event_stream_url": str(request.url_for("stream_job_events", job_id=job.job_id)),
        "result_url": str(request.url_for("get_job_result", job_id=job.job_id)),
    }


def _webhook_allowed_hosts_from_environment() -> set[str]:
    return {
        host.strip().lower()
        for host in os.getenv("WEBHOOK_ALLOWED_HOSTS", "").split(",")
        if host.strip()
    }


def _validated_webhook_url(value: str, allowed_hosts: set[str]) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "webhook_url must be an absolute HTTP(S) URL without credentials or a fragment",
        )
    if parsed.hostname.lower() not in allowed_hosts:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "webhook_url host is not approved",
        )
    return value


def create_app(
    *,
    config: OCRServiceConfig | None = None,
    storage: ArtifactStorage | None = None,
    job_store: JobStore | None = None,
    policy: PolicySpec | None = None,
    policy_engine: PolicyEngine | None = None,
    webhook_secret: str | None = None,
    webhook_allowed_hosts: set[str] | None = None,
    webhook_transport: httpx.AsyncBaseTransport | None = None,
    webhook_poll_interval: float = 0.1,
    upload_secret: str | None = None,
    artifact_policy: ArtifactLifecyclePolicy | None = None,
    dispatch_broker: DispatchBroker | None = None,
    service_role: str | None = None,
) -> FastAPI:
    """Create an app; injectable dependencies keep API tests hermetic."""

    if policy is not None and policy_engine is not None:
        raise ValueError("provide policy or policy_engine, not both")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        resolved_config = config or load_config(
            os.getenv("OCR_CONFIG_PATH", str(PROJECT_ROOT / "configs" / "ocr-paths.yaml"))
        )
        resolved_storage = storage or _storage_from_environment()
        resolved_job_store = job_store or _job_store_from_environment()
        resolved_role = (service_role or os.getenv("SERVICE_ROLE", "all")).lower()
        if resolved_role not in {"all", "api", "worker"}:
            raise RuntimeError("SERVICE_ROLE must be one of: all, api, worker")
        resolved_dispatch = dispatch_broker or _dispatch_from_environment()
        resolved_policy_engine = policy_engine
        if resolved_policy_engine is None:
            resolved_policy = policy or load_policy(
                os.getenv("POLICY_CONFIG_PATH", str(PROJECT_ROOT / "configs" / "policy.yaml"))
            )
            resolved_policy_engine = BuiltinPolicyEngine(resolved_policy)
        service = OCRService(
            resolved_config,
            resolved_storage,
            job_store=resolved_job_store,
            policy_engine=resolved_policy_engine,
            dispatch_broker=resolved_dispatch,
            publish_dispatch=resolved_role in {"all", "api"},
            consume_dispatch=resolved_role in {"all", "worker"},
        )
        app.state.config = resolved_config
        app.state.storage = resolved_storage
        app.state.job_store = resolved_job_store
        app.state.policy_engine = resolved_policy_engine
        app.state.service = service
        app.state.artifact_policy = artifact_policy or ArtifactLifecyclePolicy()
        resolved_webhook_secret = webhook_secret or os.getenv("WEBHOOK_SIGNING_SECRET")
        dispatcher = None
        if resolved_webhook_secret and resolved_role in {"all", "api"}:
            dispatcher = WebhookDispatcher(
                resolved_job_store,
                resolved_webhook_secret,
                transport=webhook_transport,
                poll_interval=webhook_poll_interval,
            )
        app.state.webhook_dispatcher = dispatcher
        app.state.webhook_secret = resolved_webhook_secret
        app.state.upload_secret = upload_secret or os.getenv("UPLOAD_SIGNING_SECRET")
        app.state.webhook_allowed_hosts = (
            {host.lower() for host in webhook_allowed_hosts}
            if webhook_allowed_hosts is not None
            else _webhook_allowed_hosts_from_environment()
        )
        app.state.ready = False
        await service.start()
        if dispatcher is not None:
            await dispatcher.start()
        app.state.ready = True
        try:
            yield
        finally:
            app.state.ready = False
            if dispatcher is not None:
                await dispatcher.close()
            await service.stop()

    app = FastAPI(title="Document Processing Service", version="0.1.0", lifespan=lifespan)

    def service_for(request: Request) -> OCRService:
        return request.app.state.service

    async def known_job(request: Request, job_id: str) -> Job:
        try:
            return await service_for(request).get_job(job_id)
        except KeyError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from exc

    @app.get("/healthz", tags=["operations"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["operations"])
    async def readiness(request: Request) -> dict[str, str]:
        if not getattr(request.app.state, "ready", False):
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "service is not ready")
        return {"status": "ready"}

    @app.post("/v1/uploads", status_code=status.HTTP_201_CREATED, name="create_upload", tags=["uploads"])
    async def create_upload(request: Request, body: UploadRequest) -> dict[str, object]:
        secret = request.app.state.upload_secret
        if not secret:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "signed uploads are not configured"
            )
        extension, media_type = _media_type_for_filename(body.filename)
        normalized_producer = _normalize_producer(body.producer_kind)
        try:
            lifecycle = request.app.state.artifact_policy.authorize(
                normalized_producer, session_id=body.artifact_session_id
            )
        except ArtifactPolicyError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc

        storage_key = (
            request.app.state.artifact_policy.source_key(body.tenant_id, uuid4().hex, lifecycle)
            + f"/document{extension}"
        )
        max_bytes = _upload_byte_ceiling(request.app.state.config)
        expires_at = utc_now() + timedelta(seconds=UPLOAD_GRANT_TTL_SECONDS)
        claims = UploadClaims(
            tenant_id=body.tenant_id,
            storage_key=storage_key,
            filename=_safe_display_filename(body.filename, extension),
            media_type=media_type,
            max_bytes=max_bytes,
            retention=lifecycle.retention,
            owner_id=lifecycle.owner_id,
            producer_kind=normalized_producer.value if normalized_producer else None,
            expires_at=expires_at,
        )
        token = sign_upload_grant(secret, claims)
        direct_url = await request.app.state.storage.presign_put(
            storage_key,
            content_type=media_type,
            max_bytes=max_bytes,
            expires_in=UPLOAD_GRANT_TTL_SECONDS,
        )
        upload_url = direct_url or str(request.url_for("receive_upload", token=token))
        return {
            "upload_id": token,
            "storage_key": storage_key,
            "max_bytes": max_bytes,
            "upload": {
                "method": "PUT",
                "url": upload_url,
                "headers": {},
                "expires_at": expires_at.isoformat(),
            },
            "submit_url": str(request.url_for("create_job")),
        }

    @app.put(
        "/v1/uploads/{token}",
        status_code=status.HTTP_204_NO_CONTENT,
        name="receive_upload",
        tags=["uploads"],
    )
    async def receive_upload(request: Request, token: str) -> Response:
        # Local-development receiver for backends whose storage cannot presign a
        # direct object-store PUT. A real S3-compatible store bypasses this.
        secret = request.app.state.upload_secret
        if not secret:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "signed uploads are not configured"
            )
        try:
            claims = verify_upload_grant(secret, token)
        except UploadGrantError as exc:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, str(exc)) from exc
        chunks: list[bytes] = []
        received = 0
        async for chunk in request.stream():
            received += len(chunk)
            if received > claims.max_bytes:
                raise HTTPException(
                    status.HTTP_413_CONTENT_TOO_LARGE,
                    f"upload exceeds the granted {claims.max_bytes}-byte ceiling",
                )
            chunks.append(chunk)
        content = b"".join(chunks)
        if not content:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "upload must not be empty")
        _validate_signature(content, Path(claims.filename).suffix.lower())
        await request.app.state.storage.put_bytes(
            claims.storage_key, content, claims.media_type
        )
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.post("/v1/jobs", status_code=status.HTTP_202_ACCEPTED, name="create_job", tags=["jobs"])
    async def create_job(
        request: Request,
        file: Annotated[UploadFile | None, File()] = None,
        upload_id: Annotated[str | None, Form()] = None,
        tenant_id: Annotated[str, Form(min_length=1, max_length=128)] = "default",
        priority: Annotated[str, Form()] = "standard",
        producer_kind: Annotated[str | None, Form()] = None,
        artifact_session_id: Annotated[str | None, Form(max_length=128)] = None,
        page_count: Annotated[int, Form(ge=1)] = 1,
        data_classification: Annotated[str | None, Form()] = None,
        home_region: Annotated[str | None, Form(max_length=63)] = None,
        webhook_url: Annotated[str | None, Form(max_length=2048)] = None,
    ) -> dict[str, object]:
        if (file is None) == (upload_id is None):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "provide exactly one of file or upload_id",
            )
        normalized_classification = _normalize_classification(data_classification)
        normalized_region = _normalize_region(home_region)
        normalized_priority = priority.lower()
        if normalized_priority not in PRIORITY_BY_NAME:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"priority must be one of: {', '.join(PRIORITY_BY_NAME)}",
            )
        normalized_webhook_url = None
        if webhook_url is not None:
            if not request.app.state.webhook_secret:
                raise HTTPException(
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    "webhook delivery is not configured",
                )
            normalized_webhook_url = _validated_webhook_url(
                webhook_url, request.app.state.webhook_allowed_hosts
            )

        if file is not None:
            document, submit_tenant, submit_producer, cleanup_key = await _document_from_upload_file(
                request, file, tenant_id, producer_kind, artifact_session_id, page_count
            )
        else:
            assert upload_id is not None
            document, submit_tenant, submit_producer, cleanup_key = await _document_from_grant(
                request, upload_id, page_count
            )

        try:
            job = await service_for(request).submit(
                document,
                tenant_id=submit_tenant,
                priority=PRIORITY_BY_NAME[normalized_priority],
                producer_kind=submit_producer,
                data_classification=normalized_classification,
                home_region=normalized_region,
                webhook_url=normalized_webhook_url,
            )
        except (PolicyEvaluationError, PolicyContractError) as exc:
            with suppress(Exception):
                await request.app.state.storage.delete(cleanup_key)
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "policy engine is unavailable",
            ) from exc
        except Exception:
            with suppress(Exception):
                await request.app.state.storage.delete(cleanup_key)
            raise
        return _job_urls(request, job)

    @app.delete(
        "/v1/artifact-sessions/{session_id}",
        status_code=status.HTTP_204_NO_CONTENT,
        tags=["artifacts"],
    )
    async def close_artifact_session(
        request: Request,
        session_id: str,
        tenant_id: Annotated[str, Query(min_length=1, max_length=128)] = "default",
    ) -> Response:
        await service_for(request).release_session_artifacts(tenant_id, session_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    @app.get("/v1/jobs/{job_id}", name="get_job", tags=["jobs"])
    async def get_job(request: Request, job_id: str) -> Job:
        return await known_job(request, job_id)

    @app.get("/v1/jobs/{job_id}/events", name="get_job_events", tags=["events"])
    async def get_job_events(
        request: Request,
        job_id: str,
        after_version: Annotated[int, Query(ge=0)] = 0,
    ) -> dict[str, list[JobEvent]]:
        await known_job(request, job_id)
        events = await service_for(request).get_events(job_id, after_version=after_version)
        return {"events": events}

    @app.get(
        "/v1/jobs/{job_id}/events/stream", name="stream_job_events", tags=["events"]
    )
    async def stream_job_events(
        request: Request,
        job_id: str,
        after_version: Annotated[int, Query(ge=0)] = 0,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        await known_job(request, job_id)
        history = await service_for(request).get_events(job_id)
        if last_event_id:
            matching = next((event for event in history if event.event_id == last_event_id), None)
            if matching is None:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    "Last-Event-ID is not available in the retained event history",
                )
            after_version = max(after_version, matching.job_version)

        async def generate() -> AsyncIterator[str]:
            cursor = after_version
            bus = service_for(request).events
            async with bus.stream() as queue:
                backfill = await service_for(request).get_events(
                    job_id, after_version=cursor
                )
                for event in backfill:
                    cursor = event.job_version
                    yield _event_payload(event)
                    if event.event_type in TERMINAL_EVENT_TYPES:
                        return

                current = await known_job(request, job_id)
                if current.state in {
                    JobState.SUCCEEDED,
                    JobState.FAILED,
                    JobState.REJECTED,
                    JobState.CANCELLED,
                }:
                    return

                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=15)
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    if event.job_id != job_id or event.job_version <= cursor:
                        continue
                    cursor = event.job_version
                    yield _event_payload(event)
                    if event.event_type in TERMINAL_EVENT_TYPES:
                        return

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.get("/v1/jobs/{job_id}/result", name="get_job_result", tags=["jobs"])
    async def get_job_result(request: Request, job_id: str) -> Response:
        job = await known_job(request, job_id)
        if job.artifacts_released_at is not None:
            raise HTTPException(status.HTTP_410_GONE, "job artifacts have been released")
        if job.state is not JobState.SUCCEEDED:
            raise HTTPException(status.HTTP_409_CONFLICT, "result is not available")
        if job.result is None:
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "result artifact is missing")
        markdown = await request.app.state.storage.get_bytes(job.result.key)
        return Response(markdown, media_type="text/markdown; charset=utf-8")

    @app.get("/v1/ocr-paths", tags=["configuration"])
    async def get_ocr_paths(request: Request) -> dict[str, object]:
        configured: OCRServiceConfig = request.app.state.config
        return {
            "schema_version": configured.schema_version,
            "paths": [path.model_dump(mode="json") for path in configured.paths],
            "runtime_intent": {
                "declared": "YAML defines capabilities, hard limits, rate limits, and timeouts",
                "discovered": "adapter health and available concurrency are checked at dispatch",
                "v0_1_scope": "runtime observations are process-local and are not durable",
            },
        }

    return app
