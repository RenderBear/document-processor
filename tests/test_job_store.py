from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from doc_processing.config import OCRServiceConfig
from doc_processing.events import JobEvent
from doc_processing.job_store import (
    ConcurrentJobUpdate,
    InMemoryJobStore,
    PostgresJobStore,
    _job_payload,
)
from doc_processing.models import DispatchCommand, PDF, DocumentReference, Job, JobState, utc_now
from doc_processing.orchestrator import OCRService
from doc_processing.storage import LocalArtifactStorage
from doc_processing.webhooks import WebhookDispatcher


def document() -> DocumentReference:
    return DocumentReference(
        uri="s3://documents/source.pdf",
        filename="source.pdf",
        media_type=PDF,
        size_bytes=128,
        page_count=1,
    )


def config(*, latency: float = 0.0) -> OCRServiceConfig:
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "paths": [
                {
                    "path_id": "internal",
                    "revision": 1,
                    "adapter": "mock_internal",
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 1_000,
                        "max_pages": 10,
                    },
                    "mock": {"latency_seconds": latency},
                }
            ],
        }
    )


@pytest.mark.asyncio
async def test_memory_store_commits_job_and_event_together() -> None:
    store = InMemoryJobStore()
    job = Job(tenant_id="tenant-1", document=document())
    await store.create_job(job)

    job.state = JobState.QUEUED
    job.version = 1
    event = JobEvent(
        event_type="document.job.queued",
        job_id=job.job_id,
        job_version=job.version,
        tenant_id=job.tenant_id,
        data={"state": JobState.QUEUED.value},
    )
    await store.save_job(job, event)

    assert (await store.get_job(job.job_id)).state is JobState.QUEUED
    assert [saved.event_id for saved in await store.list_events(job.job_id)] == [
        event.event_id
    ]


@pytest.mark.asyncio
async def test_memory_store_atomically_accepts_events_and_dispatch_claim_check() -> None:
    store = InMemoryJobStore()
    job = Job(tenant_id="tenant-1", document=document(), state=JobState.QUEUED, version=2)
    events = tuple(
        JobEvent(
            event_type=f"document.job.{state.value}",
            job_id=job.job_id,
            job_version=version,
            tenant_id=job.tenant_id,
            data={"state": state.value},
        )
        for version, state in ((1, JobState.ACCEPTED), (2, JobState.QUEUED))
    )
    command = DispatchCommand(job_id=job.job_id)

    await store.create_job(job, events=events, dispatch=command)

    assert [event.job_version for event in await store.list_events(job.job_id)] == [1, 2]
    pending = await store.list_pending_dispatches()
    assert pending == [command]
    assert "document" not in command.model_dump()


@pytest.mark.asyncio
async def test_memory_acceptance_validation_does_not_leave_a_partial_job() -> None:
    store = InMemoryJobStore()
    job = Job(tenant_id="tenant-1", document=document(), state=JobState.QUEUED, version=2)
    invalid_event = JobEvent(
        event_type="document.job.queued",
        job_id="another-job",
        job_version=1,
        tenant_id=job.tenant_id,
    )

    with pytest.raises(ValueError, match="event identity"):
        await store.create_job(job, events=(invalid_event,))

    with pytest.raises(KeyError, match="unknown job"):
        await store.get_job(job.job_id)


@pytest.mark.asyncio
async def test_memory_store_rejects_stale_snapshots() -> None:
    store = InMemoryJobStore()
    original = Job(tenant_id="tenant-1", document=document())
    await store.create_job(original)
    current = original.model_copy(deep=True)
    current.state = JobState.QUEUED
    current.version = 1
    await store.save_job(
        current,
        JobEvent(
            event_type="document.job.queued",
            job_id=current.job_id,
            job_version=1,
            tenant_id=current.tenant_id,
        ),
    )

    with pytest.raises(ConcurrentJobUpdate, match="expected 0"):
        await store.save_job(original)


@pytest.mark.asyncio
async def test_memory_store_orders_and_retries_webhook_events() -> None:
    store = InMemoryJobStore()
    job = Job(tenant_id="tenant-1", document=document())
    await store.create_job(job, webhook_url="https://backend.example/webhook")
    for version, state in ((1, JobState.QUEUED), (2, JobState.PROCESSING)):
        job.state = state
        job.version = version
        await store.save_job(
            job,
            JobEvent(
                event_id=f"evt-{version}",
                event_type=f"document.job.{state.value}",
                job_id=job.job_id,
                job_version=version,
                tenant_id=job.tenant_id,
                data={"state": state.value},
            ),
        )

    pending = await store.list_pending_webhooks()
    assert [delivery.event.event_id for delivery in pending] == ["evt-1"]

    await store.record_webhook_attempt(
        "evt-1",
        state="pending",
        next_attempt_at=utc_now() + timedelta(minutes=1),
        error="offline",
    )
    assert await store.list_pending_webhooks() == []

    await store.record_webhook_attempt(
        "evt-1", state="delivered", next_attempt_at=None, error=None
    )
    pending = await store.list_pending_webhooks()
    assert [delivery.event.event_id for delivery in pending] == ["evt-2"]


@pytest.mark.asyncio
async def test_webhook_dispatcher_retries_transient_failures_in_order() -> None:
    store = InMemoryJobStore()
    job = Job(tenant_id="tenant-1", document=document())
    await store.create_job(job, webhook_url="https://backend.example/webhook")
    for version, state in ((1, JobState.QUEUED), (2, JobState.PROCESSING)):
        job.state = state
        job.version = version
        await store.save_job(
            job,
            JobEvent(
                event_id=f"evt-{version}",
                event_type=f"document.job.{state.value}",
                job_id=job.job_id,
                job_version=version,
                tenant_id=job.tenant_id,
                data={"state": state.value},
            ),
        )

    received_versions: list[int] = []

    def backend(request: httpx.Request) -> httpx.Response:
        version = json.loads(request.content)["job_version"]
        received_versions.append(version)
        return httpx.Response(503 if len(received_versions) == 1 else 204)

    dispatcher = WebhookDispatcher(
        store,
        "unit-test-secret",
        transport=httpx.MockTransport(backend),
        retry_base=0,
    )
    try:
        await dispatcher.dispatch_available()
        await dispatcher.dispatch_available()
        await dispatcher.dispatch_available()
    finally:
        await dispatcher.close()

    assert received_versions == [1, 1, 2]


@pytest.mark.asyncio
async def test_service_recovers_an_accepted_job_from_shared_store(tmp_path: Path) -> None:
    store = InMemoryJobStore()
    accepted = Job(tenant_id="tenant-1", document=document(), version=1)
    accepted_event = JobEvent(
        event_type="document.job.accepted",
        job_id=accepted.job_id,
        job_version=1,
        tenant_id="tenant-1",
        data={"state": "accepted"},
    )
    await store.create_job(accepted, events=(accepted_event,))

    async with OCRService(
        config(),
        LocalArtifactStorage(tmp_path),
        job_store=store,
    ) as service:
        result = await service.wait_for_job(accepted.job_id, timeout=1)

    assert result.state is JobState.SUCCEEDED
    assert [event.job_version for event in await store.list_events(accepted.job_id)] == [
        1,
        2,
        3,
        4,
    ]


@pytest.mark.asyncio
async def test_service_retries_an_interrupted_attempt_after_restart(tmp_path: Path) -> None:
    store = InMemoryJobStore()
    first = OCRService(
        config(latency=10.0),
        LocalArtifactStorage(tmp_path),
        job_store=store,
    )
    await first.start()
    submitted = await first.submit(document(), tenant_id="tenant-1")
    for _ in range(100):
        interrupted = await first.get_job(submitted.job_id)
        if interrupted.state is JobState.PROCESSING and interrupted.attempts:
            break
        await asyncio.sleep(0.001)
    else:
        raise AssertionError("job did not begin processing")
    await first.stop()

    async with OCRService(
        config(),
        LocalArtifactStorage(tmp_path),
        job_store=store,
    ) as recovered:
        result = await recovered.wait_for_job(submitted.job_id, timeout=1)

    assert result.state is JobState.SUCCEEDED
    assert [attempt.state.value for attempt in result.attempts] == ["failed", "succeeded"]
    assert result.attempts[0].error_code == "process.restarted"


class SchemaPool:
    def __init__(self, tables: set[str]) -> None:
        self.tables = tables

    async def fetchval(self, query: str, relation: str) -> str | None:
        assert "to_regclass" in query
        table = relation.removeprefix("public.")
        return relation if table in self.tables else None


@pytest.mark.asyncio
async def test_postgres_store_requires_applied_schema() -> None:
    store = PostgresJobStore(
        "postgresql://unused",
        pool=SchemaPool({"document_job"}),
    )

    with pytest.raises(RuntimeError, match="missing job_event"):
        await store.start()


@pytest.mark.asyncio
async def test_postgres_store_accepts_complete_schema() -> None:
    store = PostgresJobStore(
        "postgresql://unused",
        pool=SchemaPool(
            {
                "document_job",
                "job_event",
                "job_webhook_subscription",
                "webhook_delivery",
                "dispatch_outbox",
            }
        ),
    )

    await store.start()


def test_postgres_migration_keeps_payloads_as_references() -> None:
    migration = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(Path("migrations").glob("*.sql"))
    )

    assert "CREATE TABLE IF NOT EXISTS document_job" in migration
    assert "CREATE TABLE IF NOT EXISTS job_event" in migration
    assert "CREATE TABLE IF NOT EXISTS job_webhook_subscription" in migration
    assert "CREATE TABLE IF NOT EXISTS webhook_delivery" in migration
    assert "CREATE TABLE IF NOT EXISTS dispatch_outbox" in migration
    assert "BYTEA" not in migration.upper()


def test_postgres_job_payload_excludes_computed_fields() -> None:
    payload = json.loads(_job_payload(Job(tenant_id="tenant-1", document=document())))

    assert "terminal" not in payload
