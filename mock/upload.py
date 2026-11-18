"""Upload fixture files from ``mock/data`` to the mock backend."""

from __future__ import annotations

import argparse
import json
import mimetypes
from contextlib import ExitStack
from pathlib import Path
from typing import Protocol

import httpx

DATA_DIR = Path(__file__).with_name("data")
DEFAULT_URL = "http://127.0.0.1:8001/upload"
DOCUMENT_EXTENSIONS = frozenset({".pdf", ".docx", ".pptx", ".txt"})


class UploadClient(Protocol):
    def post(self, url: str, *, files: list[tuple[str, tuple]]) -> httpx.Response: ...


def fixture_files(data_dir: Path = DATA_DIR) -> list[Path]:
    """Return visible fixture files in deterministic order."""

    return sorted(
        path
        for path in data_dir.rglob("*")
        if path.is_file()
        and path.suffix.lower() in DOCUMENT_EXTENSIONS
        and not any(part.startswith(".") for part in path.relative_to(data_dir).parts)
    )


def upload_paths(
    paths: list[Path],
    *,
    url: str = DEFAULT_URL,
    client: UploadClient | None = None,
) -> dict[str, object]:
    """Send paths as a single upload or one multipart batch."""

    if not paths:
        raise ValueError("no files to upload")

    field_name = "file" if len(paths) == 1 else "files"
    with ExitStack() as stack:
        parts = [
            (
                field_name,
                (
                    path.name,
                    stack.enter_context(path.open("rb")),
                    mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                ),
            )
            for path in paths
        ]
        if client is not None:
            response = client.post(url, files=parts)
        else:
            with httpx.Client(timeout=30.0) as http_client:
                response = http_client.post(url, files=parts)

    response.raise_for_status()
    return response.json()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="upload endpoint URL")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DATA_DIR,
        help="directory whose files are uploaded (default: mock/data)",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if not args.data_dir.is_dir():
        raise SystemExit(f"data directory does not exist: {args.data_dir}")
    paths = fixture_files(args.data_dir)
    if not paths:
        raise SystemExit(f"data directory contains no files: {args.data_dir}")
    print(json.dumps(upload_paths(paths, url=args.url), indent=2))


if __name__ == "__main__":
    main()
