"""Dedicated processor process consuming claim-check commands from JetStream."""

from __future__ import annotations

import asyncio
import os

from .api import (
    PROJECT_ROOT,
    _dispatch_from_environment,
    _job_store_from_environment,
    _storage_from_environment,
)
from .config import load_config
from .orchestrator import OCRService
from .policy import BuiltinPolicyEngine, load_policy


async def serve() -> None:
    dispatch = _dispatch_from_environment()
    if dispatch is None:
        raise RuntimeError("the standalone worker requires DISPATCH_BACKEND=jetstream")
    service = OCRService(
        load_config(os.getenv("OCR_CONFIG_PATH", str(PROJECT_ROOT / "configs/ocr-paths.yaml"))),
        _storage_from_environment(),
        job_store=_job_store_from_environment(),
        policy_engine=BuiltinPolicyEngine(
            load_policy(
                os.getenv("POLICY_CONFIG_PATH", str(PROJECT_ROOT / "configs/policy.yaml"))
            )
        ),
        dispatch_broker=dispatch,
        publish_dispatch=False,
        consume_dispatch=True,
    )
    async with service:
        await asyncio.Event().wait()


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()
