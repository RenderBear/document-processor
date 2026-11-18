"""Deterministic document fixtures for preflight-aware tests.

Preflight parses real bytes, so tests that expect a job to progress past
validation must upload structurally valid documents. These builders produce
minimal-but-valid PDFs (with or without a text layer) and OOXML packages.
"""

from __future__ import annotations

from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile


def valid_pdf(*, pages: int = 1, marker: str | None = None) -> bytes:
    """A valid blank PDF with ``pages`` pages and no extractable text layer.

    ``marker`` is embedded as a PDF comment so a test can assert that raw source
    bytes never escape into job or event payloads while the document still parses.
    """

    from pypdf import PdfWriter

    writer = PdfWriter()
    for _ in range(max(1, pages)):
        writer.add_blank_page(width=200, height=200)
    buffer = BytesIO()
    writer.write(buffer)
    data = buffer.getvalue()
    if marker is not None:
        # Append as a trailing comment after %%EOF so the marker never shifts the
        # xref byte offsets; injecting it earlier corrupts the cross-reference
        # table for some marker lengths.
        data = data + b"\n%" + marker.encode("latin-1") + b"\n"
    return data


def text_pdf(text: str = "Hello digital document") -> bytes:
    """A valid single-page PDF whose content stream carries extractable text."""

    stream = f"BT /F1 24 Tf 20 100 Td ({text}) Tj ET".encode("latin-1")
    objects = [
        b"<</Type/Catalog/Pages 2 0 R>>",
        b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
        b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]"
        b"/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>",
        b"<</Length %d>>stream\n%s\nendstream" % (len(stream), stream),
        b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>",
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for index, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj" % index + body + b"endobj\n"
    xref_pos = len(out)
    out += b"xref\n0 %d\n" % (len(objects) + 1)
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += b"%010d 00000 n \n" % offset
    out += b"trailer<</Size %d/Root 1 0 R>>\n" % (len(objects) + 1)
    out += b"startxref\n%d\n%%%%EOF\n" % xref_pos
    return bytes(out)


def encrypted_pdf(*, password: str = "secret") -> bytes:
    """A valid PDF locked with a user password preflight cannot open."""

    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    writer.encrypt(user_password=password)
    buffer = BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def office_package(member: str, *, extra: dict[str, str] | None = None) -> bytes:
    """A minimal OOXML package containing ``member`` and any ``extra`` parts."""

    output = BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr(member, "<document/>")
        for name, content in (extra or {}).items():
            archive.writestr(name, content)
    return output.getvalue()
