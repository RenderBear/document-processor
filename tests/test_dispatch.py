from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from doc_processing.dispatch import JetStreamDispatchBroker, OutboxPublisher
from doc_processing.events import JobEvent
from doc_processing.job_store import InMemoryJobStore
from doc_processing.models import DispatchCommand, Job, JobState, PDF, DocumentReference


def queued_job() -> tuple[Job, tuple[JobEvent, ...], DispatchCommand]:
    document = DocumentReference(
        uri="s3://documents/sources/job.pdf",
        filename="job.pdf",
        media_type=PDF,
        size_bytes=100,
        page_count=1,
    )
    job = Job(tenant_id="tenant-1", document=document, state=JobState.QUEUED, version=2)
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
    return job, events, DispatchCommand(job_id=job.job_id)


class RecoveringBroker:
    def __init__(self) -> None:
        self.available = False
        self.published: list[DispatchCommand] = []

    async def publish(self, command: DispatchCommand) -> None:
        if not self.available:
            raise ConnectionError("JetStream unavailable")
        self.published.append(command)


@pytest.mark.asyncio
async def test_outbox_keeps_accepted_command_until_broker_recovers() -> None:
    store = InMemoryJobStore()
    job, events, command = queued_job()
    await store.create_job(job, events=events, dispatch=command)
    broker = RecoveringBroker()
    publisher = OutboxPublisher(store, broker, poll_interval=0.001)
    await publisher.start()
    try:
        await asyncio.sleep(0.01)
        assert (await store.get_job(job.job_id)).state is JobState.QUEUED
        assert await store.list_pending_dispatches() == [command]

        broker.available = True
        publisher.notify()
        for _ in range(100):
            if not await store.list_pending_dispatches():
                break
            await asyncio.sleep(0.001)
    finally:
        await publisher.close()

    assert broker.published == [command]
    assert await store.list_pending_dispatches() == []


class FakeMessage:
    def __init__(self, command: DispatchCommand, deliveries: int) -> None:
        self.data = command.model_dump_json().encode()
        self.metadata = SimpleNamespace(num_delivered=deliveries)
        self.acked = False
        self.nak_delay: float | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, *, delay: float) -> None:
        self.nak_delay = delay

    async def in_progress(self) -> None:
        return None


class FakeJetStream:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, dict[str, str]]] = []

    async def publish(self, subject: str, data: bytes, *, headers: dict[str, str]) -> None:
        self.published.append((subject, data, headers))


@pytest.mark.asyncio
async def test_jetstream_acknowledges_only_success_and_quarantines_exhaustion() -> None:
    broker = JetStreamDispatchBroker(["nats://unused"], max_deliver=2)
    command = DispatchCommand(job_id="job-1")
    js = FakeJetStream()
    successful = FakeMessage(command, deliveries=1)

    async def succeed(_: DispatchCommand) -> None:
        return None

    await broker._handle_message(js, successful, succeed)
    assert successful.acked
    assert successful.nak_delay is None

    exhausted = FakeMessage(command, deliveries=2)

    async def fail(_: DispatchCommand) -> None:
        raise RuntimeError("processor failed")

    await broker._handle_message(js, exhausted, fail)
    assert exhausted.acked
    assert js.published[0][0] == "document.jobs.process.quarantine"
    assert js.published[0][2]["X-Original-Command-Id"] == command.command_id


@pytest.mark.asyncio
async def test_jetstream_quarantines_poison_payload_after_bounded_delivery() -> None:
    broker = JetStreamDispatchBroker(["nats://unused"], max_deliver=2)
    message = FakeMessage(DispatchCommand(job_id="unused"), deliveries=2)
    message.data = b"not-json"
    js = FakeJetStream()

    async def must_not_run(_: DispatchCommand) -> None:
        raise AssertionError("invalid commands must not reach processors")

    await broker._handle_message(js, message, must_not_run)

    assert message.acked
    assert js.published[0][0] == "document.jobs.process.quarantine"
    assert js.published[0][1] == b"not-json"
