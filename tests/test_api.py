from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest

from doc_processing.api import create_app
from doc_processing.config import OCRServiceConfig
from doc_processing.dispatch import DispatchBroker
from doc_processing.job_store import InMemoryJobStore
from doc_processing.models import DOCX, PDF, PPTX
from doc_processing.storage import LocalArtifactStorage
from doc_processing.webhooks import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_webhook_signature

from pdf_fixtures import valid_pdf


def api_config(*, latency: float = 0.01, max_bytes: int = 1_000_000) -> OCRServiceConfig:
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 2, "capacity_poll_interval_seconds": 0.001},
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 10,
                    "capabilities": {"media_types": [PDF, DOCX, PPTX]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": max_bytes,
                        "max_pages": 10,
                    },
                    "mock": {"latency_seconds": latency},
                }
            ],
        }
    )


@asynccontextmanager
async def api_client(
    tmp_path,
    *,
    latency: float = 0.01,
    max_bytes: int = 1_000_000,
    job_store: InMemoryJobStore | None = None,
    webhook_secret: str | None = None,
    webhook_allowed_hosts: set[str] | None = None,
    webhook_transport: httpx.AsyncBaseTransport | None = None,
    dispatch_broker: DispatchBroker | None = None,
    service_role: str | None = None,
):
    storage = LocalArtifactStorage(tmp_path / "artifacts")
    app = create_app(
        config=api_config(latency=latency, max_bytes=max_bytes),
        storage=storage,
        job_store=job_store,
        webhook_secret=webhook_secret,
        webhook_allowed_hosts=webhook_allowed_hosts,
        webhook_transport=webhook_transport,
        webhook_poll_interval=0.001,
        dispatch_broker=dispatch_broker,
        service_role=service_role,
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, storage


async def submit_pdf(client: httpx.AsyncClient, filename: str = "sample.pdf") -> httpx.Response:
    return await client.post(
        "/v1/jobs",
        files={"file": (filename, valid_pdf(), PDF)},
        data={"tenant_id": "tenant-1", "priority": "interactive", "page_count": "2"},
    )


def office_package(member: str) -> bytes:
    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr(member, "<document/>")
    return output.getvalue()


async def await_terminal(client: httpx.AsyncClient, job_id: str) -> dict:
    for _ in range(100):
        response = await client.get(f"/v1/jobs/{job_id}")
        body = response.json()
        if body["terminal"]:
            return body
        await asyncio.sleep(0.005)
    raise AssertionError("job did not reach a terminal state")


@pytest.mark.asyncio
async def test_submit_status_events_and_markdown_result(tmp_path) -> None:
    async with api_client(tmp_path) as (client, _):
        response = await submit_pdf(client)
        assert response.status_code == 202
        accepted = response.json()
        assert set(accepted) == {
            "job_id",
            "state",
            "status_url",
            "events_url",
            "event_stream_url",
            "result_url",
        }

        job = await await_terminal(client, accepted["job_id"])
        assert job["state"] == "succeeded"
        assert job["selected_path_id"] == "external"
        assert job["document"]["checksum_sha256"]

        events_response = await client.get(f"/v1/jobs/{accepted['job_id']}/events")
        events = events_response.json()["events"]
        assert [event["job_version"] for event in events] == list(range(1, len(events) + 1))
        assert events[-1]["event_type"] == "document.job.succeeded"

        result = await client.get(f"/v1/jobs/{accepted['job_id']}/result")
        assert result.status_code == 200
        assert "mock-external-ocr" in result.text


@pytest.mark.asyncio
async def test_trusted_producer_kind_is_classified_by_policy(tmp_path) -> None:
    async with api_client(tmp_path) as (client, _):
        response = await client.post(
            "/v1/jobs",
            files={"file": ("knowledge.pdf", valid_pdf(), PDF)},
            data={
                "tenant_id": "tenant-1",
                "producer_kind": "agent",
                "priority": "bulk",
                "page_count": "2",
            },
        )
        assert response.status_code == 202
        job = await await_terminal(client, response.json()["job_id"])

    assert job["producer_kind"] == "agent"
    assert job["service_class"] == "knowledge"
    assert job["queue_priority"] == 50
    assert job["priority"] == 10
    assert job["admission_policy"]["policy_id"] == "default-ocr"


@pytest.mark.asyncio
async def test_chat_artifacts_require_session_and_are_deleted_when_it_closes(tmp_path) -> None:
    async with api_client(tmp_path) as (client, storage):
        missing_session = await client.post(
            "/v1/jobs",
            files={"file": ("chat.pdf", valid_pdf(), PDF)},
            data={"producer_kind": "chat"},
        )
        assert missing_session.status_code == 422

        accepted = await client.post(
            "/v1/jobs",
            files={"file": ("chat.pdf", valid_pdf(), PDF)},
            data={
                "tenant_id": "tenant-1",
                "producer_kind": "chat",
                "artifact_session_id": "browser-tab-1",
            },
        )
        job = await await_terminal(client, accepted.json()["job_id"])
        source_key = job["document"]["storage_key"]
        result_key = job["result"]["key"]
        assert job["document"]["lifecycle"] == {
            "retention": "session_bound",
            "owner_id": "browser-tab-1",
        }
        assert await storage.exists(source_key)
        assert await storage.exists(result_key)

        closed = await client.delete(
            "/v1/artifact-sessions/browser-tab-1",
            params={"tenant_id": "tenant-1"},
        )
        assert closed.status_code == 204
        assert not await storage.exists(source_key)
        assert not await storage.exists(result_key)
        assert (await client.get(f"/v1/jobs/{job['job_id']}/result")).status_code == 410


@pytest.mark.asyncio
async def test_storage_keys_do_not_embed_untrusted_tenant_or_session_paths(tmp_path) -> None:
    async with api_client(tmp_path) as (client, _):
        accepted = await client.post(
            "/v1/jobs",
            files={"file": ("chat.pdf", valid_pdf(), PDF)},
            data={
                "tenant_id": "../../tenant/private",
                "producer_kind": "chat",
                "artifact_session_id": "../../browser/session",
            },
        )
        assert accepted.status_code == 202
        job = await await_terminal(client, accepted.json()["job_id"])
        key = job["document"]["storage_key"]
        assert key.startswith("sources/session/")
        assert ".." not in key
        assert "tenant" not in key
        assert "browser" not in key


@pytest.mark.asyncio
async def test_background_artifacts_outlive_unrelated_session_cleanup(tmp_path) -> None:
    async with api_client(tmp_path) as (client, storage):
        accepted = await client.post(
            "/v1/jobs",
            files={"file": ("knowledge.pdf", valid_pdf(), PDF)},
            data={"tenant_id": "tenant-1", "producer_kind": "agent"},
        )
        job = await await_terminal(client, accepted.json()["job_id"])
        source_key = job["document"]["storage_key"]
        assert job["document"]["lifecycle"]["retention"] == "background"

        await client.delete(
            "/v1/artifact-sessions/browser-tab-1",
            params={"tenant_id": "tenant-1"},
        )
        assert await storage.exists(source_key)


class UnavailableBroker:
    subject = "document.jobs.process"

    async def publish(self, command) -> None:
        raise ConnectionError("JetStream unavailable")

    async def consume(self, handler, *, workers: int) -> None:
        raise AssertionError("API role must not consume processor commands")

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_processor_or_broker_outage_does_not_reject_durable_acceptance(tmp_path) -> None:
    store = InMemoryJobStore()
    async with api_client(
        tmp_path,
        job_store=store,
        dispatch_broker=UnavailableBroker(),
        service_role="api",
    ) as (client, _):
        response = await submit_pdf(client)
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        await asyncio.sleep(0.01)
        # Durable acceptance commits the job as accepted; without a reachable
        # broker the preflight/queue transition simply has not run yet.
        assert (await client.get(f"/v1/jobs/{job_id}")).json()["state"] == "accepted"
        pending = await store.list_pending_dispatches()
        assert [command.job_id for command in pending] == [job_id]


@pytest.mark.asyncio
async def test_submit_delivers_ordered_signed_webhooks(tmp_path) -> None:
    secret = "unit-test-webhook-secret"
    received: list[httpx.Request] = []

    def backend(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(204)

    async with api_client(
        tmp_path,
        webhook_secret=secret,
        webhook_allowed_hosts={"backend"},
        webhook_transport=httpx.MockTransport(backend),
    ) as (client, _):
        response = await client.post(
            "/v1/jobs",
            files={"file": ("sample.pdf", valid_pdf(), PDF)},
            data={
                "producer_kind": "rag_ingestion",
                "page_count": "1",
                "webhook_url": "https://backend/webhooks/processor",
            },
        )
        assert response.status_code == 202
        await await_terminal(client, response.json()["job_id"])
        for _ in range(200):
            if len(received) >= 4:
                break
            await asyncio.sleep(0.005)

    payloads = [json.loads(request.content) for request in received]
    assert [payload["job_version"] for payload in payloads] == list(
        range(1, len(payloads) + 1)
    )
    assert payloads[-1]["event_type"] == "document.job.succeeded"
    assert all(
        verify_webhook_signature(
            secret,
            request.headers[TIMESTAMP_HEADER],
            request.content,
            request.headers[SIGNATURE_HEADER],
        )
        for request in received
    )


@pytest.mark.asyncio
async def test_submit_rejects_unapproved_webhook_host(tmp_path) -> None:
    async with api_client(
        tmp_path,
        webhook_secret="unit-test-webhook-secret",
        webhook_allowed_hosts={"backend"},
    ) as (client, _):
        response = await client.post(
            "/v1/jobs",
            files={"file": ("sample.pdf", valid_pdf(), PDF)},
            data={"webhook_url": "https://unapproved.example/webhook"},
        )

    assert response.status_code == 422
    assert response.json()["detail"] == "webhook_url host is not approved"


@pytest.mark.asyncio
async def test_job_and_events_keep_payloads_by_reference(tmp_path) -> None:
    source_sentinel = "source-payload-must-not-escape"
    extracted_sentinel = "This is placeholder Markdown for the v0.1 OCR contract."
    async with api_client(tmp_path) as (client, _):
        response = await client.post(
            "/v1/jobs",
            files={"file": ("sample.pdf", valid_pdf(marker=source_sentinel), PDF)},
            data={"page_count": "1"},
        )
        job_id = response.json()["job_id"]
        job = await await_terminal(client, job_id)
        events = (await client.get(f"/v1/jobs/{job_id}/events")).json()["events"]

    observable_state = json.dumps({"job": job, "events": events})
    assert source_sentinel not in observable_state
    assert extracted_sentinel not in observable_state
    assert job["document"]["uri"]
    assert job["result"]["uri"]


@pytest.mark.asyncio
async def test_generated_artifact_key_ignores_untrusted_filename_path(tmp_path) -> None:
    async with api_client(tmp_path) as (client, storage):
        response = await submit_pdf(client, "../../escape.pdf")
        assert response.status_code == 202
        job = await await_terminal(client, response.json()["job_id"])

        assert job["document"]["filename"] == "escape.pdf"
        assert job["document"]["uri"].startswith((storage.root / "sources").as_uri())
        assert "escape.pdf" not in job["document"]["uri"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filename", "media_type", "content"),
    [
        ("sample.pdf", PDF, valid_pdf()),
        ("sample.docx", DOCX, office_package("word/document.xml")),
        ("sample.pptx", PPTX, office_package("ppt/presentation.xml")),
    ],
)
async def test_supported_document_packages_are_accepted(
    tmp_path, filename: str, media_type: str, content: bytes
) -> None:
    async with api_client(tmp_path) as (client, _):
        response = await client.post(
            "/v1/jobs",
            files={"file": (filename, content, media_type)},
            data={"page_count": "1"},
        )
        assert response.status_code == 202
        job = await await_terminal(client, response.json()["job_id"])
        assert job["state"] == "succeeded"


@pytest.mark.asyncio
async def test_upload_type_signature_and_size_are_enforced(tmp_path) -> None:
    async with api_client(tmp_path, max_bytes=12) as (client, _):
        mismatch = await client.post(
            "/v1/jobs", files={"file": ("sample.pdf", b"%PDF-1.7", "text/plain")}
        )
        assert mismatch.status_code == 415

        invalid_signature = await client.post(
            "/v1/jobs", files={"file": ("sample.pdf", b"not-pdf", PDF)}
        )
        assert invalid_signature.status_code == 415

        oversized = await client.post(
            "/v1/jobs", files={"file": ("sample.pdf", b"%PDF-" + b"x" * 20, PDF)}
        )
        assert oversized.status_code == 413

        unsupported = await client.post(
            "/v1/jobs", files={"file": ("sample.txt", b"hello", "text/plain")}
        )
        assert unsupported.status_code == 415


@pytest.mark.asyncio
async def test_result_is_conflict_until_job_succeeds(tmp_path) -> None:
    async with api_client(tmp_path, latency=0.1) as (client, _):
        response = await submit_pdf(client)
        result = await client.get(f"/v1/jobs/{response.json()['job_id']}/result")
        assert result.status_code == 409


@pytest.mark.asyncio
async def test_sse_backfills_filters_and_reconnects_from_last_event_id(tmp_path) -> None:
    async with api_client(tmp_path) as (client, _):
        first = await submit_pdf(client, "first.pdf")
        second = await submit_pdf(client, "second.pdf")
        first_id = first.json()["job_id"]
        await await_terminal(client, first_id)
        await await_terminal(client, second.json()["job_id"])

        stream = await client.get(f"/v1/jobs/{first_id}/events/stream")
        assert stream.status_code == 200
        event_blocks = [block for block in stream.text.split("\n\n") if block]
        assert event_blocks
        assert all(first_id in block for block in event_blocks)
        last_event_id = next(
            line.removeprefix("id: ")
            for line in event_blocks[-2].splitlines()
            if line.startswith("id: ")
        )

        resumed = await client.get(
            f"/v1/jobs/{first_id}/events/stream",
            headers={"Last-Event-ID": last_event_id},
        )
        assert resumed.status_code == 200
        assert resumed.text.count("event: ") == 1
        assert "document.job.succeeded" in resumed.text


@pytest.mark.asyncio
async def test_operations_and_path_contract_endpoints(tmp_path) -> None:
    async with api_client(tmp_path) as (client, _):
        assert (await client.get("/healthz")).json() == {"status": "ok"}
        assert (await client.get("/readyz")).json() == {"status": "ready"}
        paths = (await client.get("/v1/ocr-paths")).json()
        assert paths["paths"][0]["limits"]["request_timeout_seconds"] == 1.0
        assert "dispatch" in paths["runtime_intent"]["discovered"]

        assert (await client.get("/v1/jobs/missing")).status_code == 404


@pytest.mark.asyncio
async def test_job_status_events_and_result_survive_service_restart(tmp_path) -> None:
    job_store = InMemoryJobStore()
    async with api_client(tmp_path, job_store=job_store) as (client, _):
        submitted = await submit_pdf(client)
        job_id = submitted.json()["job_id"]
        await await_terminal(client, job_id)

    async with api_client(tmp_path, job_store=job_store) as (client, _):
        job = await client.get(f"/v1/jobs/{job_id}")
        events = await client.get(f"/v1/jobs/{job_id}/events")
        result = await client.get(f"/v1/jobs/{job_id}/result")

    assert job.json()["state"] == "succeeded"
    assert events.json()["events"][-1]["event_type"] == "document.job.succeeded"
    assert result.status_code == 200
