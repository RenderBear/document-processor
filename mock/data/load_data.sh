#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
document_count=${1:-100}
output_dir=${2:-"$script_dir/docs"}
archive_url="https://codait-cos-dax.s3.us.cloud-object-storage.appdomain.cloud/dax-doclaynet/1.0.0/DocLayNet_extra.zip"

case "$document_count" in
  ''|*[!0-9]*|0)
    echo "usage: $0 [positive-document-count] [output-directory]" >&2
    exit 2
    ;;
esac

if ! command -v uv >/dev/null 2>&1; then
  echo "uv is required to load the DocLayNet subset." >&2
  exit 1
fi

mkdir -p "$output_dir"

echo "Loading $document_count DocLayNet PDFs into $output_dir"
echo "Source: https://github.com/DS4SD/DocLayNet"
echo "The loader reads individual files from the remote ZIP; it does not download the full archive."

uv run --isolated --no-project --with 'remotezip==0.12.3' python - \
  "$archive_url" "$output_dir" "$document_count" <<'PY'
from __future__ import annotations

import shutil
import sys
from pathlib import Path

from remotezip import RemoteZip


archive_url = sys.argv[1]
output_dir = Path(sys.argv[2]).resolve()
requested_count = int(sys.argv[3])
output_dir.mkdir(parents=True, exist_ok=True)

headers = {"User-Agent": "doc-processing-service-test-data/1.0"}
with RemoteZip(
    archive_url,
    headers=headers,
    initial_buffer_size=16 * 1024 * 1024,
    timeout=120,
) as archive:
    pdf_members = sorted(
        (member for member in archive.infolist() if member.filename.lower().endswith(".pdf")),
        key=lambda member: member.filename,
    )
    if not pdf_members:
        raise SystemExit("DocLayNet archive contains no PDF members")

    selected_count = min(requested_count, len(pdf_members))
    selected = [
        pdf_members[(index * len(pdf_members)) // selected_count]
        for index in range(selected_count)
    ]

    downloaded = 0
    reused = 0
    filenames: list[str] = []
    for index, member in enumerate(selected, start=1):
        target = output_dir / Path(member.filename).name
        filenames.append(target.name)
        if target.is_file() and target.stat().st_size == member.file_size:
            with target.open("rb") as existing:
                if existing.read(5) == b"%PDF-":
                    reused += 1
                    print(f"[{index:>3}/{selected_count}] reuse    {target.name}")
                    continue

        partial = target.with_suffix(".pdf.part")
        try:
            with archive.open(member) as source, partial.open("wb") as destination:
                shutil.copyfileobj(source, destination, length=1024 * 1024)
            with partial.open("rb") as downloaded_file:
                if downloaded_file.read(5) != b"%PDF-":
                    raise RuntimeError(f"downloaded member is not a PDF: {member.filename}")
            partial.replace(target)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise

        downloaded += 1
        print(f"[{index:>3}/{selected_count}] download {target.name}")

(output_dir / "manifest.txt").write_text("\n".join(filenames) + "\n", encoding="utf-8")
(output_dir / "SOURCE.txt").write_text(
    "DocLayNet extra files v1.0.0\n"
    "https://github.com/DS4SD/DocLayNet\n"
    "License: Community Data License Agreement - Permissive 1.0\n",
    encoding="utf-8",
)

print(f"Ready: {selected_count} PDFs ({downloaded} downloaded, {reused} reused)")
print(f"Manifest: {output_dir / 'manifest.txt'}")
PY
