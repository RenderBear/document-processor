"""Asynchronous document preflight: authoritative facts before scheduling.

Preflight is an orchestration effect, not a policy decision. It reads source
bytes through ``ArtifactStorage`` and produces only bounded decision facts: an
authoritative page count and a :class:`~doc_processing.models.DocumentProfile`.
Extracted document text never leaves this stage; only the presence or absence of
a text layer is retained. A structurally invalid, encrypted, or oversized input
is rejected before it reaches the scheduler.
"""

from __future__ import annotations

import asyncio
import re
from io import BytesIO
from typing import Protocol
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

from .config import PreflightConfig
from .models import (
    DOCX,
    PDF,
    PPTX,
    DocumentProfile,
    DocumentReference,
    FeaturePresence,
    LayoutClass,
    StrictModel,
    TextLayer,
)

_PPTX_SLIDE = re.compile(r"^ppt/slides/slide\d+\.xml$")
_DOCX_PAGES = re.compile(r"<Pages>(\d+)</Pages>")


class PreflightRejection(Exception):
    """A document is unsafe or unusable and must not enter the scheduler."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class PreflightResult(StrictModel):
    """Authoritative facts derived from the source bytes."""

    page_count: int
    profile: DocumentProfile


class DocumentProfiler(Protocol):
    async def run(
        self, document: DocumentReference, content: bytes
    ) -> PreflightResult: ...


class DefaultDocumentPreflight:
    """Bounded profiler for the v0.1 supported media types.

    Parsing is CPU-bound and runs off the event loop. Every parser failure maps
    to a stable reason code so the orchestrator can reject the job explainably.
    """

    def __init__(self, config: PreflightConfig | None = None) -> None:
        self.config = config or PreflightConfig()

    async def run(
        self, document: DocumentReference, content: bytes
    ) -> PreflightResult:
        if not content:
            raise PreflightRejection("input.invalid", "source document is empty")
        media_type = document.media_type
        if media_type == PDF:
            return await asyncio.to_thread(self._profile_pdf, content)
        if media_type in (DOCX, PPTX):
            return await asyncio.to_thread(self._profile_office, content, media_type)
        # The orchestrator only routes supported media types here; treat anything
        # else as a hard rejection rather than a silent pass-through.
        raise PreflightRejection(
            "input.unsupported", f"preflight has no profiler for {media_type}"
        )

    # -- PDF ----------------------------------------------------------------

    def _profile_pdf(self, content: bytes) -> PreflightResult:
        from pypdf import PdfReader
        from pypdf.errors import PyPdfError

        try:
            reader = PdfReader(BytesIO(content), strict=False)
        except (PyPdfError, ValueError, OSError) as exc:
            raise PreflightRejection("input.corrupt", "PDF could not be parsed") from exc

        if reader.is_encrypted:
            try:
                unlocked = reader.decrypt("")
            except PreflightRejection:
                raise
            except Exception as exc:  # pypdf raises varied errors for unsupported schemes
                raise PreflightRejection(
                    "input.encrypted", "PDF encryption is unsupported"
                ) from exc
            if not unlocked:
                raise PreflightRejection(
                    "input.encrypted", "PDF requires a password to open"
                )

        try:
            pages = list(reader.pages)
        except (PyPdfError, ValueError, OSError) as exc:
            raise PreflightRejection("input.corrupt", "PDF page tree is unreadable") from exc

        page_count = len(pages)
        if page_count == 0:
            raise PreflightRejection("input.invalid", "PDF contains no pages")
        if page_count > self.config.max_pages:
            raise PreflightRejection(
                "input.too_many_pages",
                f"PDF has {page_count} pages, above the {self.config.max_pages} preflight ceiling",
            )

        sampled = pages[: self.config.text_sample_pages]
        extracted = 0
        for page in sampled:
            try:
                text = page.extract_text() or ""
            except Exception:
                # A single unreadable page must not fail the whole document; it
                # simply contributes no evidence of a text layer.
                continue
            extracted += len(text.strip())
            if extracted >= self.config.digital_text_threshold_chars:
                break
        text_layer = (
            TextLayer.PRESENT
            if extracted >= self.config.digital_text_threshold_chars
            else TextLayer.ABSENT
        )

        return PreflightResult(
            page_count=page_count,
            profile=DocumentProfile(
                profiler_id="pdf-pypdf",
                revision=1,
                text_layer=text_layer,
                layout_class=LayoutClass.UNKNOWN,
                handwriting=FeaturePresence.UNKNOWN,
                mathematical_notation=FeaturePresence.UNKNOWN,
            ),
        )

    # -- Office (OOXML) -----------------------------------------------------

    def _profile_office(self, content: bytes, media_type: str) -> PreflightResult:
        try:
            with ZipFile(BytesIO(content)) as archive:
                names = archive.namelist()
                if media_type == PPTX:
                    page_count = sum(1 for name in names if _PPTX_SLIDE.match(name))
                else:
                    page_count = self._docx_page_count(archive, names)
        except BadZipFile as exc:
            raise PreflightRejection(
                "input.corrupt", "Office package is not a valid archive"
            ) from exc

        page_count = max(1, page_count)
        if page_count > self.config.max_pages:
            raise PreflightRejection(
                "input.too_many_pages",
                f"document has {page_count} pages, above the {self.config.max_pages} ceiling",
            )
        # OOXML documents carry a native text layer; a rendering engine would be
        # required to classify layout, so it stays unknown at this stage.
        return PreflightResult(
            page_count=page_count,
            profile=DocumentProfile(
                profiler_id="office-ooxml",
                revision=1,
                text_layer=TextLayer.PRESENT,
                layout_class=LayoutClass.UNKNOWN,
            ),
        )

    @staticmethod
    def _docx_page_count(archive: ZipFile, names: list[str]) -> int:
        # Word records a best-effort page total in app.xml when it was last saved
        # by a layout-aware editor. It is advisory, so a missing value falls back
        # to one schedulable unit rather than trusting the client.
        if "docProps/app.xml" not in names:
            return 1
        try:
            raw = archive.read("docProps/app.xml").decode("utf-8", "replace")
        except (KeyError, OSError):
            return 1
        match = _DOCX_PAGES.search(raw)
        if match is None:
            # Some writers namespace the element; fall back to a tolerant parse.
            try:
                root = ElementTree.fromstring(raw)
            except ElementTree.ParseError:
                return 1
            for element in root.iter():
                if element.tag.rsplit("}", 1)[-1] == "Pages" and (element.text or "").isdigit():
                    return int(element.text)
            return 1
        return int(match.group(1))
