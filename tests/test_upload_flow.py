from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import httpx
import pytest

from doc_processing.api import create_app
from doc_processing.config import OCRServiceConfig
from doc_processing.models import PDF
from doc_processing.storage import LocalArtifactStorage

from pdf_fixtures import valid_pdf

UPLOAD_SECRET = "unit-upload-secret"


def _config(*, max_bytes: int = 1_000_000) -> OCRServiceConfig:
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
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": max_bytes,
                        "max_pages": 10,
                    },
                    "mock": {"latency_seconds": 0.01},
                }
            ],
        }
    )


@asynccontextmanager
async def upload_client(tmp_path, *, max_bytes: int = 1_000_000, secret: str | None = UPLOAD_SECRET):
    storage = LocalArtifactStorage(tmp_path / "artifacts")
    app = create_app(config=_config(max_bytes=max_bytes), storage=storage, upload_secret=secret)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, storage


async def _await_terminal(client: httpx.AsyncClient, job_id: str) -> dict:
    for _ in range(200):
        body = (await client.get(f"/v1/jobs/{job_id}")).json()
        if body["terminal"]:
            return body
        await asyncio.sleep(0.005)
    raise AssertionError("job did not reach a terminal state")


@pytest.mark.asyncio
async def test_signed_url_two_step_upload_succeeds(tmp_path) -> None:
    async with upload_client(tmp_path) as (client, _):
        created = await client.post(
            "/v1/uploads",
            json={"filename": "doc.pdf", "tenant_id": "tenant-1", "producer_kind": "agent"},
        )
        assert created.status_code == 201
        grant = created.json()
        assert grant["storage_key"].startswith("sources/background/")

        put = await client.put(grant["upload"]["url"], content=valid_pdf())
        assert put.status_code == 204

        submitted = await client.post(
            "/v1/jobs", data={"upload_id": grant["upload_id"], "page_count": "1"}
        )
        assert submitted.status_code == 202
        job = await _await_terminal(client, submitted.json()["job_id"])
        assert job["state"] == "succeeded"
        assert job["document"]["storage_key"] == grant["storage_key"]
        assert job["producer_kind"] == "agent"


@pytest.mark.asyncio
async def test_uploads_endpoint_requires_configured_secret(tmp_path) -> None:
    async with upload_client(tmp_path, secret=None) as (client, _):
        response = await client.post("/v1/uploads", json={"filename": "doc.pdf"})
        assert response.status_code == 503


@pytest.mark.asyncio
async def test_job_requires_exactly_one_source(tmp_path) -> None:
    async with upload_client(tmp_path) as (client, _):
        neither = await client.post("/v1/jobs", data={"page_count": "1"})
        assert neither.status_code == 422

        both = await client.post(
            "/v1/jobs",
            data={"upload_id": "x.y"},
            files={"file": ("doc.pdf", valid_pdf(), PDF)},
        )
        assert both.status_code == 422


@pytest.mark.asyncio
async def test_job_with_incomplete_upload_is_conflict(tmp_path) -> None:
    async with upload_client(tmp_path) as (client, _):
        created = await client.post("/v1/uploads", json={"filename": "doc.pdf"})
        grant = created.json()
        # Reference the grant without ever uploading the bytes.
        submitted = await client.post(
            "/v1/jobs", data={"upload_id": grant["upload_id"], "page_count": "1"}
        )
        assert submitted.status_code == 409


@pytest.mark.asyncio
async def test_job_with_tampered_grant_is_unauthorized(tmp_path) -> None:
    async with upload_client(tmp_path) as (client, _):
        submitted = await client.post(
            "/v1/jobs", data={"upload_id": "forged.token", "page_count": "1"}
        )
        assert submitted.status_code == 401


@pytest.mark.asyncio
async def test_receiver_enforces_size_ceiling(tmp_path) -> None:
    async with upload_client(tmp_path, max_bytes=64) as (client, _):
        created = await client.post("/v1/uploads", json={"filename": "doc.pdf"})
        grant = created.json()
        oversized = await client.put(grant["upload"]["url"], content=valid_pdf(pages=2))
        assert oversized.status_code == 413
