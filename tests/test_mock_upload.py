from __future__ import annotations

import asyncio
import hashlib
import json
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from doc_processing.api import create_app as create_processor_app
from doc_processing.storage import LocalArtifactStorage
from doc_processing.webhooks import (
    DELIVERY_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    webhook_signature,
)
from mock.app import app, create_app
from mock.relay import JobRelay
from mock.store import SQLiteProjectionStore
from mock.upload import DATA_DIR, fixture_files, parse_args, upload_paths

from pdf_fixtures import valid_pdf

WEBHOOK_SECRET = "mock-relay-test-secret"


def send_webhook(
    client,
    *,
    job_id: str,
    version: int,
    state: str,
    secret: str = WEBHOOK_SECRET,
):
    event_id = f"evt_{job_id}_{version}"
    body = json.dumps(
        {
            "event_id": event_id,
            "event_type": f"document.job.{state}",
            "job_id": job_id,
            "job_version": version,
            "tenant_id": "mock-ui",
            "data": {"state": state},
        },
        separators=(",", ":"),
    ).encode()
    timestamp = str(int(time.time()))
    return client.post(
        "/webhooks/processor",
        content=body,
        headers={
            DELIVERY_HEADER: event_id,
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: webhook_signature(secret, timestamp, body),
            "Content-Type": "application/json",
        },
    )


def test_upload_accepts_single_file() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/upload",
            files={"file": ("document.txt", b"hello", "text/plain")},
        )

    assert response.status_code == 200
    assert response.json() == {
        "mode": "single",
        "count": 1,
        "files": [
            {
                "filename": "document.txt",
                "content_type": "text/plain",
                "size_bytes": 5,
                "sha256": hashlib.sha256(b"hello").hexdigest(),
            }
        ],
    }


def test_upload_accepts_batch() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/upload",
            files=[
                ("files", ("one.txt", b"one", "text/plain")),
                ("files", ("two.txt", b"two", "text/plain")),
            ],
        )

    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "batch"
    assert body["count"] == 2
    assert [item["filename"] for item in body["files"]] == ["one.txt", "two.txt"]


def test_upload_rejects_empty_request() -> None:
    with TestClient(app) as client:
        response = client.post("/upload")

    assert response.status_code == 400


def test_mock_root_serves_upload_ui() -> None:
    with TestClient(app) as client:
        response = client.get("/")
        health = client.get("/healthz")
        script = client.get("/static/app.js")
        styles = client.get("/static/styles.css")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert health.json() == {"status": "ok"}
    assert "x-mock-instance" in health.headers
    assert response.headers["content-type"].startswith("text/html")
    assert "Local mock interface" in response.text
    assert "Event relay active" not in response.text
    assert "Backend event relay" not in response.text
    assert "POST /jobs + SSE" not in response.text
    assert "Upload files" in response.text
    assert "Ready to send" in response.text
    assert "Processing queue" in response.text
    assert "Document transfer" in response.text
    assert "Submission and processing history" not in response.text
    assert ">Agent<" in response.text
    assert "Clear queue" in response.text
    assert 'id="queue-list"' in response.text
    assert 'id="status-list"' not in response.text
    assert 'id="upload-button"' in response.text
    assert 'id="context-id"' in response.text
    assert script.status_code == 200
    assert styles.status_code == 200
    assert 'request.open("POST", "/jobs")' in script.text
    assert "new EventSource(job.events_url)" in script.text
    assert 'formData.append("producer_kind", item.producerKind)' in script.text
    assert 'formData.append("context_id", item.contextId)' in script.text
    # The processing queue is producer-agnostic: restore requests all scopes and
    # the queue is ordered newest-first.
    assert 'fetch("/jobs", { headers: { Accept: "application/json" } })' in script.text
    assert "(b.order ?? 0) - (a.order ?? 0)" in script.text
    assert "document-processing.active-kind" in script.text
    assert "no backend job was created" in script.text
    assert "Job acceptance is unknown" in script.text
    assert 'history.className = "job-history"' in script.text
    assert 'actions.className = "job-actions"' in script.text
    assert 'jobId.className = "job-id"' in script.text
    assert '["uploading", "processing"].includes(item.state)' in script.text
    assert 'loader.className = "status-loader"' in script.text
    assert 'document.querySelector(".status-pane")?.remove()' in script.text
    assert "if (!item.terminal) card.append(track)" in script.text
    assert "selectedItems = [...selectedItems, ...newItems]" in script.text
    assert "[hidden]" in styles.text
    assert 'value="agent"' in response.text


def test_mock_backend_relays_normalized_processor_webhooks(tmp_path) -> None:
    processor_requests: list[tuple[str, str]] = []

    def processor(request: httpx.Request) -> httpx.Response:
        processor_requests.append((request.method, request.url.path))
        if request.method == "POST" and request.url.path == "/v1/jobs":
            return httpx.Response(
                202,
                json={
                    "job_id": "job_relay",
                    "state": "queued",
                    "status_url": "http://processor/v1/jobs/job_relay",
                    "events_url": "http://processor/v1/jobs/job_relay/events",
                    "event_stream_url": "http://processor/v1/jobs/job_relay/events/stream",
                    "result_url": "http://processor/v1/jobs/job_relay/result",
                },
            )
        raise AssertionError(f"unexpected processor request: {request.method} {request.url}")

    relay_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=tmp_path / "relay.sqlite3",
    )
    with TestClient(relay_app) as client:
        accepted = client.post(
            "/jobs",
            files={"file": ("document.pdf", b"%PDF-1.7\nmock", "application/pdf")},
            data={"producer_kind": "rag_ingestion", "context_id": "kb-alpha"},
        )
        assert accepted.status_code == 202
        accepted_body = accepted.json()
        assert accepted_body["state"] == "queued"
        assert accepted_body["producer_kind"] == "rag_ingestion"
        assert accepted_body["context_id"] == "kb-alpha"
        assert accepted_body["persistent"] is True
        assert accepted_body["events_url"].endswith("/jobs/job_relay/events")
        assert "status_url" not in accepted_body
        assert "event_stream_url" not in accepted_body

        assert send_webhook(client, job_id="job_relay", version=1, state="accepted").status_code == 204
        assert send_webhook(client, job_id="job_relay", version=2, state="queued").status_code == 204
        assert send_webhook(client, job_id="job_relay", version=3, state="processing").status_code == 204
        assert send_webhook(client, job_id="job_relay", version=4, state="succeeded").status_code == 204
        snapshot = client.get("/jobs/job_relay").json()

        stream = client.get("/jobs/job_relay/events")
        resumed = client.get(
            "/jobs/job_relay/events", headers={"Last-Event-ID": "1"}
        )
        persistent_job_ids = [
            job["job_id"]
            for job in client.get(
                "/jobs",
                params={"producer_kind": "rag_ingestion", "context_id": "kb-alpha"},
            ).json()["jobs"]
        ]
        other_context = client.get(
            "/jobs",
            params={"producer_kind": "rag_ingestion", "context_id": "kb-beta"},
        ).json()["jobs"]

    states = [
        json.loads(line.removeprefix("data: "))["state"]
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    ]
    resumed_states = [
        json.loads(line.removeprefix("data: "))["state"]
        for line in resumed.text.splitlines()
        if line.startswith("data: ")
    ]
    assert states == ["queued", "processing", "completed"]
    assert resumed_states == ["processing", "completed"]
    assert snapshot["state"] == "completed"
    assert persistent_job_ids == ["job_relay"]
    assert other_context == []
    assert ("POST", "/v1/jobs") in processor_requests
    assert not any(path.endswith("/events/stream") for _, path in processor_requests)


def test_mock_backend_preserves_processor_rejection(tmp_path) -> None:
    def processor(_: httpx.Request) -> httpx.Response:
        return httpx.Response(415, json={"detail": "unsupported document"})

    relay_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=tmp_path / "relay.sqlite3",
    )
    with TestClient(relay_app) as client:
        response = client.post(
            "/jobs",
            files={"file": ("notes.txt", b"notes", "text/plain")},
        )

    assert response.status_code == 415
    assert response.json()["detail"] == "document processor: unsupported document"


def test_mock_backend_does_not_create_job_when_processor_returns_503(tmp_path) -> None:
    def processor(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "temporarily unavailable"})

    relay_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=tmp_path / "relay.sqlite3",
    )
    with TestClient(relay_app) as client:
        response = client.post(
            "/jobs",
            files={"file": ("notes.txt", b"notes", "text/plain")},
            data={"producer_kind": "rag_ingestion", "context_id": "kb-main"},
        )
        jobs = client.get(
            "/jobs",
            params={"producer_kind": "rag_ingestion", "context_id": "kb-main"},
        ).json()["jobs"]

    assert response.status_code == 503
    assert response.json()["detail"] == "document processor: temporarily unavailable"
    assert jobs == []


@pytest.mark.asyncio
async def test_mock_backend_relays_real_processor_events(tmp_path) -> None:
    class MutableASGITransport(httpx.AsyncBaseTransport):
        app = None

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            assert self.app is not None
            return await httpx.ASGITransport(app=self.app).handle_async_request(request)

    processor_transport = MutableASGITransport()
    relay_app = create_app(
        processor_url="http://processor",
        processor_transport=processor_transport,
        webhook_secret=WEBHOOK_SECRET,
        public_webhook_url="http://backend/webhooks/processor",
        projection_path=tmp_path / "relay.sqlite3",
    )
    processor_app = create_processor_app(
        storage=LocalArtifactStorage(tmp_path / "processor-artifacts"),
        webhook_secret=WEBHOOK_SECRET,
        webhook_allowed_hosts={"backend"},
        webhook_transport=httpx.ASGITransport(app=relay_app),
        webhook_poll_interval=0.001,
    )
    processor_transport.app = processor_app
    async with relay_app.router.lifespan_context(relay_app):
        async with processor_app.router.lifespan_context(processor_app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=relay_app),
                base_url="http://backend",
            ) as client:
                accepted = await client.post(
                    "/jobs",
                    files={
                        "file": (
                            "document.pdf",
                            valid_pdf(marker="real-relay-test"),
                            "application/pdf",
                        )
                    },
                    data={"producer_kind": "agent"},
                )
                assert accepted.status_code == 202
                job_id = accepted.json()["job_id"]

                deadline = asyncio.get_running_loop().time() + 8
                while asyncio.get_running_loop().time() < deadline:
                    snapshot = (await client.get(f"/jobs/{job_id}")).json()
                    if snapshot["terminal"]:
                        break
                    await asyncio.sleep(0.05)
                else:
                    raise AssertionError("real processor relay did not become terminal")

                stream = await client.get(f"/jobs/{job_id}/events")

    states = [
        json.loads(line.removeprefix("data: "))["state"]
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    ]
    assert states == ["queued", "processing", "completed"]
    assert snapshot["state"] == "completed"


def test_non_chat_jobs_restore_while_chat_jobs_remain_ephemeral(tmp_path) -> None:
    job_ids = iter(("job_rag", "job_chat"))

    def processor(_: httpx.Request) -> httpx.Response:
        job_id = next(job_ids)
        return httpx.Response(202, json={"job_id": job_id, "state": "queued"})

    database = tmp_path / "relay.sqlite3"
    first_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=database,
    )
    with TestClient(first_app) as client:
        rag = client.post(
            "/jobs",
            files={"file": ("rag.pdf", b"%PDF-1.7\nrag", "application/pdf")},
            data={"producer_kind": "rag_ingestion", "context_id": "kb-main"},
        )
        chat = client.post(
            "/jobs",
            files={"file": ("chat.pdf", b"%PDF-1.7\nchat", "application/pdf")},
            data={"producer_kind": "chat", "context_id": "chat-session-one"},
        )
        assert rag.status_code == chat.status_code == 202
        assert send_webhook(client, job_id="job_rag", version=1, state="processing").status_code == 204
        assert send_webhook(client, job_id="job_chat", version=1, state="processing").status_code == 204

    restored_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=database,
    )
    with TestClient(restored_app) as client:
        restored = client.get(
            "/jobs",
            params={"producer_kind": "rag_ingestion", "context_id": "kb-main"},
        ).json()["jobs"]
        assert [job["job_id"] for job in restored] == ["job_rag"]
        assert restored[0]["state"] == "processing"
        assert restored[0]["context_id"] == "kb-main"
        assert client.get(
            "/jobs", params={"producer_kind": "rag_ingestion", "context_id": "kb-other"}
        ).json()["jobs"] == []
        assert client.get("/jobs/job_chat").status_code == 404
        assert send_webhook(client, job_id="job_rag", version=2, state="succeeded").status_code == 204
        stream = client.get("/jobs/job_rag/events")

    states = [
        json.loads(line.removeprefix("data: "))["state"]
        for line in stream.text.splitlines()
        if line.startswith("data: ")
    ]
    assert states == ["queued", "processing", "completed"]


def test_job_listing_separates_categories_and_contexts(tmp_path) -> None:
    job_ids = iter(("rag_a", "rag_b", "knowledge_a"))

    def processor(_: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"job_id": next(job_ids), "state": "queued"})

    relay_app = create_app(
        processor_url="http://processor",
        processor_transport=httpx.MockTransport(processor),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=tmp_path / "relay.sqlite3",
    )
    submissions = (
        ("rag_ingestion", "kb-a"),
        ("rag_ingestion", "kb-b"),
        ("agent", "agent-a"),
    )
    with TestClient(relay_app) as client:
        for producer_kind, context_id in submissions:
            response = client.post(
                "/jobs",
                files={"file": ("document.pdf", b"%PDF-1.7\nmock", "application/pdf")},
                data={"producer_kind": producer_kind, "context_id": context_id},
            )
            assert response.status_code == 202

        rag_a = client.get(
            "/jobs",
            params={"producer_kind": "rag_ingestion", "context_id": "kb-a"},
        ).json()["jobs"]
        knowledge_a = client.get(
            "/jobs",
            params={"producer_kind": "agent", "context_id": "agent-a"},
        ).json()["jobs"]
        invalid = client.get("/jobs", params={"context_id": "not allowed"})

    assert [job["job_id"] for job in rag_a] == ["rag_a"]
    assert [job["job_id"] for job in knowledge_a] == ["knowledge_a"]
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_legacy_saved_projection_restores_in_default_context(tmp_path) -> None:
    store = SQLiteProjectionStore(tmp_path / "legacy.sqlite3")
    await store.start()
    await store.save(
        {
            "job_id": "legacy-rag",
            "filename": "legacy.pdf",
            "producer_kind": "rag_ingestion",
            "processor_state": "queued",
            "processor_version": 1,
        },
        (
            {
                "sequence": 1,
                "job_id": "legacy-rag",
                "filename": "legacy.pdf",
                "producer_kind": "rag_ingestion",
                "state": "queued",
                "processor_state": "queued",
            },
        ),
    )

    relay = JobRelay(store)
    await relay.start()
    restored = await relay.list_persistent(
        producer_kind="rag_ingestion", context_id="default"
    )

    assert len(restored) == 1
    assert restored[0].context_id == "default"
    assert restored[0].events[0].context_id == "default"


def test_mock_backend_rejects_invalid_webhook_signature(tmp_path) -> None:
    relay_app = create_app(
        processor_transport=httpx.MockTransport(lambda _: httpx.Response(500)),
        webhook_secret=WEBHOOK_SECRET,
        projection_path=tmp_path / "relay.sqlite3",
    )
    with TestClient(relay_app) as client:
        response = client.post(
            "/webhooks/processor",
            content=b"{}",
            headers={TIMESTAMP_HEADER: str(int(time.time())), SIGNATURE_HEADER: "v1=bad"},
        )

    assert response.status_code == 401


def test_fixture_uploader_sends_mock_data_as_batch(tmp_path) -> None:
    first = tmp_path / "first.pdf"
    second = tmp_path / "nested" / "second.docx"
    first.write_bytes(b"%PDF-1.7\nfirst")
    second.parent.mkdir()
    second.write_bytes(b"PK\x03\x04second")
    paths = fixture_files(tmp_path)

    with TestClient(app) as client:
        body = upload_paths(paths, url="/upload", client=client)

    assert body["mode"] == "batch"
    assert body["count"] == len(paths)
    assert [item["filename"] for item in body["files"]] == [path.name for path in paths]


def test_fixture_uploader_sends_one_file_in_single_mode(tmp_path) -> None:
    path = tmp_path / "document.pdf"
    path.write_bytes(b"%PDF-1.7\nsingle")

    with TestClient(app) as client:
        body = upload_paths([path], url="/upload", client=client)

    assert body["mode"] == "single"
    assert body["files"][0]["filename"] == path.name


def test_data_directory_controls_which_files_are_uploaded(tmp_path) -> None:
    first = tmp_path / "first.txt"
    second = tmp_path / "second.txt"
    first.write_text("first")
    second.write_text("second")

    assert fixture_files(tmp_path) == [first, second]
    assert parse_args([]).data_dir == DATA_DIR
    assert parse_args(["--data-dir", str(tmp_path)]).data_dir == tmp_path


def test_fixture_discovery_ignores_loader_and_hidden_files(tmp_path) -> None:
    loader = tmp_path / "load_data.sh"
    hidden = tmp_path / ".hidden.pdf"
    document = tmp_path / "doclaynet" / "document.pdf"
    loader.write_text("#!/bin/sh\n")
    hidden.write_bytes(b"%PDF-hidden")
    document.parent.mkdir()
    document.write_bytes(b"%PDF-document")

    assert fixture_files(tmp_path) == [document]


@pytest.mark.asyncio
async def test_relay_lists_all_producers_when_unscoped() -> None:
    relay = JobRelay()
    await relay.create(
        job_id="j-rag", filename="a.pdf", producer_kind="rag_ingestion",
        context_id="kb", processor_state="queued",
    )
    await relay.create(
        job_id="j-agent", filename="b.pdf", producer_kind="agent",
        context_id="ws", processor_state="queued",
    )
    unscoped = await relay.list_persistent()
    assert {p.job_id for p in unscoped} == {"j-rag", "j-agent"}
    # The backend retains scoped-filter capability even though the UI no longer uses it.
    scoped = await relay.list_persistent(producer_kind="rag_ingestion")
    assert {p.job_id for p in scoped} == {"j-rag"}
