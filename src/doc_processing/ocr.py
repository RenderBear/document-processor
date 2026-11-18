"""OCR adapter contract and deterministic mock implementations."""

from __future__ import annotations

import asyncio
from typing import Protocol

from pydantic import Field

from .config import OCRPathConfig
from .models import DocumentReference, OCRResult, StrictModel


class OCRAdapterError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class AdapterStatus(StrictModel):
    healthy: bool = True
    detail: str | None = None
    observed_queue_depth: int | None = Field(default=None, ge=0)


class OCRAdapter(Protocol):
    async def status(self) -> AdapterStatus: ...

    async def process(
        self,
        document: DocumentReference,
        *,
        job_id: str,
        attempt_id: str,
    ) -> OCRResult: ...


class _MockOCR:
    engine_name = "mock"

    def __init__(self, config: OCRPathConfig) -> None:
        self.config = config
        self._healthy = True

    def set_healthy(self, healthy: bool) -> None:
        """Test/operator hook representing dynamically discovered runtime health."""

        self._healthy = healthy

    async def status(self) -> AdapterStatus:
        return AdapterStatus(healthy=self._healthy)

    async def process(
        self,
        document: DocumentReference,
        *,
        job_id: str,
        attempt_id: str,
    ) -> OCRResult:
        if document.media_type not in self.config.capabilities.media_types:
            raise OCRAdapterError("input.unsupported", "media type is not supported by this path")
        await asyncio.sleep(self.config.mock.latency_seconds)
        if self.config.mock.fail_with:
            raise OCRAdapterError(self.config.mock.fail_with, "configured mock failure")
        markdown = (
            f"# Mock OCR: {document.filename}\n\n"
            f"Processed by **{self.engine_name}** on path `{self.config.path_id}`.\n\n"
            "This is placeholder Markdown for the v0.1 OCR contract.\n"
        )
        return OCRResult(
            markdown=markdown,
            path_id=self.config.path_id,
            engine=self.engine_name,
            source_uri=document.uri,
            metadata={
                "pages": document.page_count,
                "mock": True,
                "job_id": job_id,
                "attempt_id": attempt_id,
            },
        )


class MockExternalOCR(_MockOCR):
    engine_name = "mock-external-ocr"


class MockInternalOCR(_MockOCR):
    engine_name = "mock-internal-ocr"


def build_mock_adapter(config: OCRPathConfig) -> OCRAdapter:
    if config.adapter == "mock_external":
        return MockExternalOCR(config)
    if config.adapter == "mock_internal":
        return MockInternalOCR(config)
    raise ValueError(f"no adapter factory for {config.adapter}")
