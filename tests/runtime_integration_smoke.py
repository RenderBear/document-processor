"""Smoke contract executed inside the Compose app container."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from uuid import uuid4

from doc_processing.events import JobEvent
from doc_processing.job_store import ConcurrentJobUpdate, PostgresJobStore
from doc_processing.models import (
    PDF,
    Attempt,
    AttemptState,
    DocumentReference,
    Job,
    JobState,
)

BASE_URL = "http://127.0.0.1:8000"
STATE_PATH = Path("/tmp/doc-processing-runtime-contract.json")
SOURCE_SENTINEL = b"source-payload-must-remain-in-object-storage"
INTERRUPTED_PATH_ID = "external-mock"
RECOVERY_ACCEPTED_TENANT = "contract-recovery-accepted"
RECOVERY_PROCESSING_TENANT = "contract-recovery-processing"


def get_json(path: str) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=10) as response:
        return json.load(response)


def get_bytes(path: str) -> bytes:
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=10) as response:
        return response.read()


def _pdf_with_marker(marker: bytes) -> bytes:
    """A valid single-page PDF carrying ``marker`` as a comment.

    Preflight now parses uploads, so the contract document must be structurally
    valid while still embedding the sentinel in the raw source bytes.
    """

    from io import BytesIO

    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = BytesIO()
    writer.write(buffer)
    data = buffer.getvalue()
    # Append after %%EOF so the marker never shifts the xref byte offsets.
    return data + b"\n%" + marker + b"\n"


def submit_document() -> str:
    boundary = f"contract-{uuid4().hex}"
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="contract.pdf"\r\n'
        "Content-Type: application/pdf\r\n\r\n"
    ).encode() + _pdf_with_marker(SOURCE_SENTINEL) + f"\r\n--{boundary}--\r\n".encode()
    request = urllib.request.Request(
        f"{BASE_URL}/v1/jobs",
        data=body,
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 202:
            raise AssertionError(f"submission returned {response.status}")
        return json.load(response)["job_id"]


def await_terminal(job_id: str) -> dict:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        job = get_json(f"/v1/jobs/{job_id}")
        if job["terminal"]:
            return job
        time.sleep(0.1)
    raise AssertionError("job did not reach a terminal state")


def document() -> DocumentReference:
    return DocumentReference(
        uri="s3://doc-processing/contracts/source.pdf",
        filename="source.pdf",
        media_type=PDF,
        size_bytes=128,
        page_count=1,
    )


async def verify_postgres_transactions() -> None:
    store = PostgresJobStore(os.environ["DATABASE_URL"])
    await store.start()
    try:
        committed = Job(tenant_id="contract-transaction", document=document())
        stale = committed.model_copy(deep=True)
        await store.create_job(committed)
        committed.state = JobState.QUEUED
        committed.version = 1
        first_event = JobEvent(
            event_id=f"evt_contract_{uuid4().hex}",
            event_type="document.job.queued",
            job_id=committed.job_id,
            job_version=1,
            tenant_id=committed.tenant_id,
        )
        await store.save_job(committed, first_event)
        assert (await store.get_job(committed.job_id)).version == 1
        assert [event.job_version for event in await store.list_events(committed.job_id)] == [1]

        try:
            await store.save_job(stale)
        except ConcurrentJobUpdate:
            pass
        else:
            raise AssertionError("PostgreSQL accepted a stale job snapshot")

        rolled_back = Job(tenant_id="contract-rollback", document=document())
        await store.create_job(rolled_back)
        rolled_back.state = JobState.QUEUED
        rolled_back.version = 1
        duplicate_event = JobEvent(
            event_id=first_event.event_id,
            event_type="document.job.queued",
            job_id=rolled_back.job_id,
            job_version=1,
            tenant_id=rolled_back.tenant_id,
        )
        try:
            await store.save_job(rolled_back, duplicate_event)
        except ConcurrentJobUpdate:
            pass
        else:
            raise AssertionError("duplicate event did not abort the transaction")

        persisted = await store.get_job(rolled_back.job_id)
        assert persisted.state is JobState.ACCEPTED
        assert persisted.version == 0
        assert await store.list_events(rolled_back.job_id) == []
    finally:
        await store.close()


def _seed_event(job: Job, event_type: str) -> JobEvent:
    return JobEvent(
        event_id=f"evt_contract_{uuid4().hex}",
        event_type=event_type,
        job_id=job.job_id,
        job_version=job.version,
        tenant_id=job.tenant_id,
        data={"state": job.state.value},
    )


async def seed_unfinished_work() -> dict[str, str]:
    """Leave unfinished jobs in PostgreSQL for the next process to recover.

    A running service scans for recoverable jobs only during startup, so rows
    written here stay untouched until the restart under test. This covers the
    promise that one restarted service requeues unfinished work, including
    abandoning an attempt that was still running when the process stopped.
    """

    store = PostgresJobStore(os.environ["DATABASE_URL"])
    await store.start()
    try:
        # Durable acceptance always records an accepted event; seed it that way
        # so recovery re-dispatches the job through preflight to completion.
        accepted = Job(tenant_id=RECOVERY_ACCEPTED_TENANT, document=document(), version=1)
        await store.create_job(
            accepted, events=(_seed_event(accepted, "document.job.accepted"),)
        )

        processing = Job(tenant_id=RECOVERY_PROCESSING_TENANT, document=document())
        await store.create_job(processing)
        processing.state = JobState.QUEUED
        processing.version = 1
        await store.save_job(processing, _seed_event(processing, "document.job.queued"))
        processing.state = JobState.PROCESSING
        processing.version = 2
        processing.selected_path_id = INTERRUPTED_PATH_ID
        processing.attempts = [Attempt(path_id=INTERRUPTED_PATH_ID, path_revision=1)]
        await store.save_job(processing, _seed_event(processing, "document.job.processing"))

        assert (await store.get_job(accepted.job_id)).state is JobState.ACCEPTED
        interrupted = await store.get_job(processing.job_id)
        assert interrupted.state is JobState.PROCESSING
        assert [attempt.state for attempt in interrupted.attempts] == [AttemptState.RUNNING]
        return {"accepted": accepted.job_id, "processing": processing.job_id}
    finally:
        await store.close()


def verify_recovered_unfinished_work(recovery: dict[str, str]) -> None:
    accepted = await_terminal(recovery["accepted"])
    assert accepted["state"] == "succeeded"
    accepted_events = get_json(f"/v1/jobs/{recovery['accepted']}/events")["events"]
    assert [event["job_version"] for event in accepted_events] == list(
        range(1, len(accepted_events) + 1)
    )
    assert accepted_events[0]["event_type"] == "document.job.accepted"
    assert accepted_events[1]["event_type"] == "document.job.queued"
    assert accepted_events[-1]["event_type"] == "document.job.succeeded"

    processing = await_terminal(recovery["processing"])
    assert processing["state"] == "succeeded"
    attempts = processing["attempts"]
    assert len(attempts) >= 2
    assert attempts[0]["path_id"] == INTERRUPTED_PATH_ID
    assert attempts[0]["state"] == "failed"
    assert attempts[0]["error_code"] == "process.restarted"
    assert attempts[0]["finished_at"] is not None
    assert attempts[-1]["state"] == "succeeded"

    events = get_json(f"/v1/jobs/{recovery['processing']}/events")["events"]
    assert [event["job_version"] for event in events] == list(range(1, len(events) + 1))
    retrying = [event for event in events if event["event_type"] == "document.job.retrying"]
    assert len(retrying) == 1
    assert retrying[0]["data"]["error_code"] == "process.restarted"
    assert events[-1]["event_type"] == "document.job.succeeded"
    selected_path = processing["selected_path_id"]
    expected_engine = {
        "external-mock": b"mock-external-ocr",
        "internal-mock": b"mock-internal-ocr",
    }[selected_path]
    assert expected_engine in get_bytes(f"/v1/jobs/{recovery['processing']}/result")


def submit_phase() -> None:
    asyncio.run(verify_postgres_transactions())
    job_id = submit_document()
    job = await_terminal(job_id)
    assert job["state"] == "succeeded"
    events = get_json(f"/v1/jobs/{job_id}/events")["events"]
    assert [event["job_version"] for event in events] == list(range(1, len(events) + 1))
    assert events[-1]["event_type"] == "document.job.succeeded"
    assert SOURCE_SENTINEL.decode() not in json.dumps({"job": job, "events": events})
    assert b"mock-external-ocr" in get_bytes(f"/v1/jobs/{job_id}/result")
    recovery = asyncio.run(seed_unfinished_work())
    STATE_PATH.write_text(
        json.dumps({"job_id": job_id, "recovery": recovery}), encoding="utf-8"
    )


def verify_phase() -> None:
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    job_id = state["job_id"]
    job = get_json(f"/v1/jobs/{job_id}")
    events = get_json(f"/v1/jobs/{job_id}/events")["events"]
    result = get_bytes(f"/v1/jobs/{job_id}/result")
    assert job["state"] == "succeeded"
    assert events[-1]["event_type"] == "document.job.succeeded"
    assert [event["job_version"] for event in events] == list(range(1, len(events) + 1))
    assert b"mock-external-ocr" in result
    verify_recovered_unfinished_work(state["recovery"])


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "submit":
        submit_phase()
    elif phase == "verify":
        verify_phase()
    else:
        raise SystemExit("usage: runtime_integration_smoke.py submit|verify")
