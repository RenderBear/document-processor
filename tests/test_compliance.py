from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest

from doc_processing.api import create_app
from doc_processing.config import OCRServiceConfig, PathCompliance
from doc_processing.models import PDF, DataClassification, DocumentReference, JobState
from doc_processing.orchestrator import OCRService
from doc_processing.storage import LocalArtifactStorage

from pdf_fixtures import valid_pdf


# -- config -----------------------------------------------------------------


def test_path_compliance_defaults_are_permissive() -> None:
    compliance = PathCompliance()
    assert compliance.allowed_classifications == frozenset(
        {"public", "internal", "confidential", "restricted"}
    )
    assert compliance.regions == frozenset()


def test_path_compliance_rejects_unknown_classification() -> None:
    with pytest.raises(ValueError, match="unsupported data classifications"):
        PathCompliance(allowed_classifications=frozenset({"secret"}))


def test_path_compliance_rejects_malformed_region() -> None:
    with pytest.raises(ValueError):
        PathCompliance(regions=frozenset({"EU West"}))


# -- eligibility gates (integration) ----------------------------------------


def _config() -> OCRServiceConfig:
    def path(path_id, adapter, priority, classes, regions):
        return {
            "path_id": path_id,
            "revision": 1,
            "adapter": adapter,
            "routing_priority": priority,
            "capabilities": {"media_types": [PDF]},
            "compliance": {"allowed_classifications": classes, "regions": regions},
            "limits": {
                "max_concurrency": 2,
                "request_timeout_seconds": 1.0,
                "max_document_bytes": 5_000_000,
                "max_pages": 100,
            },
            "mock": {"latency_seconds": 0.001},
        }

    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 2, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                # Higher priority but externally scoped: public/internal, US only.
                path("external", "mock_external", 100, ["public", "internal"], ["us"]),
                # Lower priority but cleared for everything, EU + US.
                path("internal", "mock_internal", 50,
                     ["public", "internal", "confidential", "restricted"], ["eu", "us"]),
            ],
        }
    )


async def _store(storage: LocalArtifactStorage, content: bytes) -> DocumentReference:
    key = f"sources/background/test/{uuid4().hex}/document"
    artifact = await storage.put_bytes(key, content, PDF)
    return DocumentReference(
        uri=artifact.uri,
        storage_key=artifact.key,
        filename="sample.pdf",
        media_type=PDF,
        size_bytes=len(content),
        page_count=1,
        checksum_sha256=hashlib.sha256(content).hexdigest(),
    )


@pytest.mark.asyncio
async def test_classification_excludes_uncleared_path(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store(storage, valid_pdf(pages=1))
    async with OCRService(_config(), storage) as service:
        submitted = await service.submit(
            document, tenant_id="t", data_classification=DataClassification.CONFIDENTIAL
        )
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.SUCCEEDED
    # The higher-priority external path is not cleared for confidential.
    assert result.selected_path_id == "internal"


@pytest.mark.asyncio
async def test_residency_excludes_out_of_region_path(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store(storage, valid_pdf(pages=1))
    async with OCRService(_config(), storage) as service:
        submitted = await service.submit(
            document, tenant_id="t", data_classification=DataClassification.PUBLIC,
            home_region="eu",
        )
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.SUCCEEDED
    # external serves only us; the eu job must land on the eu-capable path.
    assert result.selected_path_id == "internal"


@pytest.mark.asyncio
async def test_no_cleared_path_is_policy_denied(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 1, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 100,
                    "capabilities": {"media_types": [PDF]},
                    "compliance": {"allowed_classifications": ["public", "internal"]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 5_000_000,
                        "max_pages": 100,
                    },
                    "mock": {"latency_seconds": 0.001},
                }
            ],
        }
    )
    document = await _store(storage, valid_pdf(pages=1))
    async with OCRService(config, storage) as service:
        submitted = await service.submit(
            document, tenant_id="t", data_classification=DataClassification.RESTRICTED
        )
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.REJECTED
    assert result.error_code == "policy.denied"


@pytest.mark.asyncio
async def test_unconstrained_region_gates_nothing(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    # Both paths leave regions empty (unconstrained); an eu job still routes.
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 1, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                {
                    "path_id": "internal",
                    "revision": 1,
                    "adapter": "mock_internal",
                    "routing_priority": 50,
                    "capabilities": {"media_types": [PDF]},
                    "limits": {
                        "max_concurrency": 1,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 5_000_000,
                        "max_pages": 100,
                    },
                    "mock": {"latency_seconds": 0.001},
                }
            ],
        }
    )
    document = await _store(storage, valid_pdf(pages=1))
    async with OCRService(config, storage) as service:
        submitted = await service.submit(document, tenant_id="t", home_region="eu")
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.SUCCEEDED


# -- API validation ---------------------------------------------------------


@asynccontextmanager
async def _api(tmp_path):
    storage = LocalArtifactStorage(tmp_path / "artifacts")
    config = OCRServiceConfig.model_validate(
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
                        "max_document_bytes": 1_000_000,
                        "max_pages": 10,
                    },
                    "mock": {"latency_seconds": 0.01},
                }
            ],
        }
    )
    app = create_app(config=config, storage=storage)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client


@pytest.mark.asyncio
async def test_api_rejects_invalid_classification(tmp_path) -> None:
    async with _api(tmp_path) as client:
        response = await client.post(
            "/v1/jobs",
            files={"file": ("s.pdf", valid_pdf(), PDF)},
            data={"data_classification": "top-secret"},
        )
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_api_rejects_invalid_region(tmp_path) -> None:
    async with _api(tmp_path) as client:
        response = await client.post(
            "/v1/jobs",
            files={"file": ("s.pdf", valid_pdf(), PDF)},
            data={"home_region": "EU WEST"},
        )
        assert response.status_code == 422


@pytest.mark.asyncio
async def test_api_carries_classification_onto_job(tmp_path) -> None:
    async with _api(tmp_path) as client:
        response = await client.post(
            "/v1/jobs",
            files={"file": ("s.pdf", valid_pdf(), PDF)},
            data={"data_classification": "internal", "home_region": "eu"},
        )
        assert response.status_code == 202
        job_id = response.json()["job_id"]
        for _ in range(200):
            body = (await client.get(f"/v1/jobs/{job_id}")).json()
            if body["terminal"]:
                break
        assert body["data_classification"] == "internal"
        assert body["home_region"] == "eu"
