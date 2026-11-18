"""ASGI entrypoint and local console runner."""

from __future__ import annotations

import os

import uvicorn

from .api import create_app

app = create_app()


def run() -> None:
    uvicorn.run(
        "doc_processing.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", "8000")),
    )


if __name__ == "__main__":
    run()
