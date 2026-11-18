import asyncio
from io import BytesIO

import pytest
from botocore.exceptions import ClientError

from doc_processing.config import OCRServiceConfig
from doc_processing.events import InProcessEventBus
from doc_processing.models import (
    DOCX,
    PDF,
    AttemptState,
    DocumentReference,
    JobPriority,
    JobState,
)
from doc_processing.ocr import AdapterStatus, OCRResult
from doc_processing.orchestrator import OCRService
from doc_processing.storage import LocalArtifactStorage, S3ArtifactStorage


def path(
    path_id: str,
    adapter: str,
    *,
    routing_priority: int,
    latency: float,
    timeout: float,
    fallbacks: list[str] | None = None,
    max_concurrency: int = 1,
    max_pages: int = 100,
) -> dict:
    return {
        "path_id": path_id,
        "revision": 1,
        "adapter": adapter,
        "routing_priority": routing_priority,
        "fallback_path_ids": fallbacks or [],
        "capabilities": {"media_types": [PDF, DOCX]},
        "limits": {
            "max_concurrency": max_concurrency,
            "request_timeout_seconds": timeout,
            "max_document_bytes": 1_000_000,
            "max_pages": max_pages,
        },
        "mock": {"latency_seconds": latency},
    }


def document(filename: str = "sample.pdf", *, pages: int = 2) -> DocumentReference:
    return DocumentReference(
        uri=f"s3://documents/{filename}",
        filename=filename,
        media_type=PDF,
        size_bytes=1024,
        page_count=pages,
    )


@pytest.mark.asyncio
async def test_timeout_falls_back_and_persists_markdown(tmp_path) -> None:
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 2, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                path(
                    "external",
                    "mock_external",
                    routing_priority=100,
                    latency=0.05,
                    timeout=0.01,
                    fallbacks=["internal"],
                ),
                path(
                    "internal",
                    "mock_internal",
                    routing_priority=50,
                    latency=0.001,
                    timeout=0.1,
                ),
            ],
        }
    )
    storage = LocalArtifactStorage(tmp_path)
    bus = InProcessEventBus()

    async with OCRService(config, storage, event_bus=bus) as service:
        submitted = await service.submit(document(), tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.SUCCEEDED
    assert result.selected_path_id == "internal"
    assert [attempt.state for attempt in result.attempts] == [
        AttemptState.TIMED_OUT,
        AttemptState.SUCCEEDED,
    ]
    assert result.result is not None
    assert b"mock-internal-ocr" in await storage.get_bytes(result.result.key)
    assert [event.job_version for event in bus.history] == list(range(1, len(bus.history) + 1))
    assert "document.job.retrying" in [event.event_type for event in bus.history]


@pytest.mark.asyncio
async def test_declared_hard_limits_reject_document(tmp_path) -> None:
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "paths": [
                path(
                    "small-path",
                    "mock_internal",
                    routing_priority=1,
                    latency=0.0,
                    timeout=0.1,
                    max_pages=2,
                )
            ],
        }
    )

    async with OCRService(config, LocalArtifactStorage(tmp_path)) as service:
        submitted = await service.submit(document(pages=3), tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.REJECTED
    assert result.error_code == "input.unsupported"
    assert result.attempts == []


class RecordingAdapter:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.active = 0
        self.max_active = 0

    async def status(self) -> AdapterStatus:
        return AdapterStatus()

    async def process(
        self, doc: DocumentReference, *, job_id: str, attempt_id: str
    ) -> OCRResult:
        self.order.append(doc.filename)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.005)
        self.active -= 1
        return OCRResult(
            markdown=f"# {doc.filename}",
            path_id="only-path",
            engine="recording",
            source_uri=doc.uri,
        )


@pytest.mark.asyncio
async def test_priority_queue_and_path_concurrency(tmp_path) -> None:
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 3, "capacity_poll_interval_seconds": 0.001},
            "paths": [
                path(
                    "only-path",
                    "mock_internal",
                    routing_priority=1,
                    latency=0.0,
                    timeout=1.0,
                    max_concurrency=1,
                )
            ],
        }
    )
    order: list[str] = []
    adapter = RecordingAdapter(order)
    service = OCRService(
        config,
        LocalArtifactStorage(tmp_path),
        adapters={"only-path": adapter},
    )
    low = await service.submit(document("bulk.pdf"), tenant_id="tenant", priority=JobPriority.BULK)
    high = await service.submit(
        document("interactive.pdf"), tenant_id="tenant", priority=JobPriority.INTERACTIVE
    )
    normal = await service.submit(
        document("standard.pdf"), tenant_id="tenant", priority=JobPriority.STANDARD
    )

    await service.start()
    await asyncio.gather(
        service.wait_for_job(low.job_id, timeout=1),
        service.wait_for_job(high.job_id, timeout=1),
        service.wait_for_job(normal.job_id, timeout=1),
    )
    await service.stop()

    assert order == ["interactive.pdf", "standard.pdf", "bulk.pdf"]
    assert adapter.max_active == 1


class FailingHeadClient:
    def __init__(self, error: ClientError) -> None:
        self.error = error

    def head_object(self, **_: str) -> None:
        raise self.error


class MemoryS3Client:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str, dict[str, str]]] = {}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        ContentType: str,
        Metadata: dict[str, str],
    ) -> None:
        self.objects[(Bucket, Key)] = (Body, ContentType, Metadata)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, BytesIO]:
        body, _, _ = self.objects[(Bucket, Key)]
        return {"Body": BytesIO(body)}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        body, _, metadata = self.objects[(Bucket, Key)]
        return {"ContentLength": len(body), "Metadata": metadata}

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        self.objects.pop((Bucket, Key), None)


def client_error(code: str, status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": code, "Message": "test error"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "HeadObject",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "status"),
    [("404", 404), ("NoSuchKey", 400), ("NotFound", 404)],
)
async def test_s3_exists_returns_false_only_for_absence(code: str, status: int) -> None:
    storage = S3ArtifactStorage(
        "documents",
        client=FailingHeadClient(client_error(code, status)),
    )

    assert await storage.exists("missing/document.pdf") is False


@pytest.mark.asyncio
async def test_s3_exists_reraises_non_absence_client_error() -> None:
    error = client_error("AccessDenied", 403)
    storage = S3ArtifactStorage("documents", client=FailingHeadClient(error))

    with pytest.raises(ClientError) as raised:
        await storage.exists("private/document.pdf")

    assert raised.value is error


@pytest.mark.asyncio
async def test_s3_put_and_get_preserve_artifact_contract() -> None:
    client = MemoryS3Client()
    storage = S3ArtifactStorage("documents", client=client)

    artifact = await storage.put_bytes(
        "results/job-1/result.md",
        b"# OCR result",
        "text/markdown",
    )

    assert artifact.key == "results/job-1/result.md"
    assert artifact.uri == "s3://documents/results/job-1/result.md"
    assert artifact.content_type == "text/markdown"
    assert artifact.size_bytes == 12
    assert await storage.get_bytes(artifact.key) == b"# OCR result"
    assert client.objects[("documents", artifact.key)][1] == "text/markdown"
    assert await storage.verify(
        artifact.key,
        size_bytes=12,
        checksum_sha256="c65a5fe71b09b50c69010e7d6375ebac6d67ce6c3c3933b53a4aa2c1e0e7b264",
    )
