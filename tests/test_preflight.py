from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest

from doc_processing.config import OCRServiceConfig, PreflightConfig
from doc_processing.models import (
    DOCX,
    PDF,
    PPTX,
    DocumentReference,
    JobPriority,
    JobState,
    TextLayer,
)
from doc_processing.orchestrator import OCRService
from doc_processing.preflight import DefaultDocumentPreflight, PreflightRejection
from doc_processing.storage import LocalArtifactStorage

from pdf_fixtures import encrypted_pdf, office_package, text_pdf, valid_pdf


def _reference(content: bytes, *, media_type: str = PDF, page_count: int = 1) -> DocumentReference:
    # page_count here is the (untrusted) client claim; preflight replaces it.
    return DocumentReference(
        uri="s3://documents/sample",
        filename="sample",
        media_type=media_type,
        size_bytes=len(content),
        page_count=page_count,
    )


# -- profiler unit tests ----------------------------------------------------


@pytest.mark.asyncio
async def test_pdf_page_count_is_authoritative() -> None:
    preflight = DefaultDocumentPreflight()
    result = await preflight.run(_reference(valid_pdf(pages=4), page_count=1), valid_pdf(pages=4))
    assert result.page_count == 4
    assert result.profile.profiler_id == "pdf-pypdf"


@pytest.mark.asyncio
async def test_pdf_text_layer_detects_digital_document() -> None:
    content = text_pdf("Invoice total 1234 EUR")
    result = await DefaultDocumentPreflight().run(_reference(content), content)
    assert result.profile.text_layer is TextLayer.PRESENT


@pytest.mark.asyncio
async def test_pdf_without_text_layer_is_marked_absent() -> None:
    content = valid_pdf(pages=1)
    result = await DefaultDocumentPreflight().run(_reference(content), content)
    assert result.profile.text_layer is TextLayer.ABSENT


@pytest.mark.asyncio
async def test_encrypted_pdf_is_rejected() -> None:
    content = encrypted_pdf()
    with pytest.raises(PreflightRejection) as excinfo:
        await DefaultDocumentPreflight().run(_reference(content), content)
    assert excinfo.value.code == "input.encrypted"


@pytest.mark.asyncio
async def test_corrupt_pdf_is_rejected() -> None:
    content = b"%PDF-1.4 not really a pdf"
    with pytest.raises(PreflightRejection) as excinfo:
        await DefaultDocumentPreflight().run(_reference(content), content)
    assert excinfo.value.code == "input.corrupt"


@pytest.mark.asyncio
async def test_empty_document_is_rejected() -> None:
    with pytest.raises(PreflightRejection) as excinfo:
        await DefaultDocumentPreflight().run(_reference(b""), b"")
    assert excinfo.value.code == "input.invalid"


@pytest.mark.asyncio
async def test_page_ceiling_rejects_oversized_document() -> None:
    preflight = DefaultDocumentPreflight(PreflightConfig(max_pages=1))
    content = valid_pdf(pages=3)
    with pytest.raises(PreflightRejection) as excinfo:
        await preflight.run(_reference(content), content)
    assert excinfo.value.code == "input.too_many_pages"


@pytest.mark.asyncio
async def test_pptx_slide_count_and_text_layer() -> None:
    content = office_package(
        "ppt/presentation.xml",
        extra={"ppt/slides/slide1.xml": "<sld/>", "ppt/slides/slide2.xml": "<sld/>"},
    )
    result = await DefaultDocumentPreflight().run(
        _reference(content, media_type=PPTX), content
    )
    assert result.page_count == 2
    assert result.profile.text_layer is TextLayer.PRESENT
    assert result.profile.profiler_id == "office-ooxml"


@pytest.mark.asyncio
async def test_docx_page_count_from_app_properties() -> None:
    content = office_package(
        "word/document.xml",
        extra={"docProps/app.xml": "<Properties><Pages>7</Pages></Properties>"},
    )
    result = await DefaultDocumentPreflight().run(
        _reference(content, media_type=DOCX), content
    )
    assert result.page_count == 7


@pytest.mark.asyncio
async def test_docx_without_page_hint_falls_back_to_one() -> None:
    content = office_package("word/document.xml")
    result = await DefaultDocumentPreflight().run(
        _reference(content, media_type=DOCX), content
    )
    assert result.page_count == 1


# -- orchestrator integration ----------------------------------------------


def _service_config() -> OCRServiceConfig:
    return OCRServiceConfig.model_validate(
        {
            "schema_version": 1,
            "scheduler": {"workers": 2, "capacity_poll_interval_seconds": 0.005},
            "paths": [
                {
                    "path_id": "external",
                    "revision": 1,
                    "adapter": "mock_external",
                    "routing_priority": 100,
                    "capabilities": {"media_types": [PDF, DOCX, PPTX]},
                    "limits": {
                        "max_concurrency": 2,
                        "request_timeout_seconds": 1.0,
                        "max_document_bytes": 5_000_000,
                        "max_pages": 100,
                    },
                    "mock": {"latency_seconds": 0.001},
                }
            ],
        }
    )


async def _store_source(
    storage: LocalArtifactStorage, content: bytes, *, media_type: str = PDF
) -> DocumentReference:
    key = f"sources/background/test/{uuid4().hex}/document"
    artifact = await storage.put_bytes(key, content, media_type)
    return DocumentReference(
        uri=artifact.uri,
        storage_key=artifact.key,
        filename="sample.pdf",
        media_type=media_type,
        size_bytes=len(content),
        page_count=1,  # deliberately wrong; preflight must overwrite it
        checksum_sha256=hashlib.sha256(content).hexdigest(),
    )


@pytest.mark.asyncio
async def test_pipeline_replaces_client_page_count_and_profiles(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store_source(storage, valid_pdf(pages=3))
    async with OCRService(_service_config(), storage) as service:
        submitted = await service.submit(
            document, tenant_id="tenant-1", priority=JobPriority.INTERACTIVE
        )
        assert submitted.state is JobState.ACCEPTED
        result = await service.wait_for_job(submitted.job_id, timeout=2)

    assert result.state is JobState.SUCCEEDED
    assert result.document.page_count == 3
    assert result.document_profile.profiler_id == "pdf-pypdf"
    events = [event.event_type for event in await service.get_events(result.job_id)]
    assert "document.job.validating" in events


@pytest.mark.asyncio
async def test_pipeline_rejects_unparseable_source(tmp_path) -> None:
    storage = LocalArtifactStorage(tmp_path)
    document = await _store_source(storage, b"%PDF-1.4 not really a pdf")
    async with OCRService(_service_config(), storage) as service:
        submitted = await service.submit(document, tenant_id="tenant-1")
        result = await service.wait_for_job(submitted.job_id, timeout=2)

    assert result.state is JobState.REJECTED
    assert result.error_code == "input.corrupt"
    assert result.attempts == []
