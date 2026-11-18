from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from doc_processing.config import OCRServiceConfig, PathCapabilities
from doc_processing.models import PDF, DocumentReference, JobState
from doc_processing.orchestrator import OCRService
from doc_processing.storage import LocalArtifactStorage

from pdf_fixtures import text_pdf, valid_pdf


# -- capability config -------------------------------------------------------


def test_path_capabilities_default_to_all_document_kinds() -> None:
    caps = PathCapabilities(media_types=frozenset({PDF}))
    assert caps.document_kinds == frozenset({"digital", "scanned"})


def test_path_capabilities_reject_unknown_document_kind() -> None:
    with pytest.raises(ValueError, match="unsupported document kinds"):
        PathCapabilities(media_types=frozenset({PDF}), document_kinds=frozenset({"faxed"}))


# -- profile-driven eligibility ---------------------------------------------


def _config(*, digital_only_priority: int = 100, both_priority: int = 50) -> OCRServiceConfig:
    def path(path_id: str, adapter: str, priority: int, kinds: list[str]) -> dict:
        return {
            "path_id": path_id,
            "revision": 1,
            "adapter": adapter,
            "routing_priority": priority,
            "capabilities": {"media_types": [PDF], "document_kinds": kinds},
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
                path("extract", "mock_external", digital_only_priority, ["digital"]),
                path("ocr", "mock_internal", both_priority, ["digital", "scanned"]),
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
async def test_scanned_document_excludes_digital_only_path(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store(storage, valid_pdf(pages=1))  # blank -> text_layer absent
    async with OCRService(_config(), storage) as service:
        submitted = await service.submit(document, tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.SUCCEEDED
    # The higher-priority digital-only path is ineligible for a scanned document.
    assert result.selected_path_id == "ocr"


@pytest.mark.asyncio
async def test_digital_document_prefers_digital_path(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store(storage, text_pdf("Digital invoice body"))
    async with OCRService(_config(), storage) as service:
        submitted = await service.submit(document, tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.SUCCEEDED
    assert result.selected_path_id == "extract"


@pytest.mark.asyncio
async def test_scanned_document_rejected_when_only_digital_paths_exist(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    config = OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 1, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                {
                    "path_id": "extract",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 100,
                    "capabilities": {"media_types": [PDF], "document_kinds": ["digital"]},
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
        submitted = await service.submit(document, tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=2)
    assert result.state is JobState.REJECTED
    assert result.error_code == "input.unsupported"
