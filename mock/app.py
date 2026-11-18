"""Mock application backend and browser UI for document processing."""

from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import httpx
from fastapi import (
    FastAPI,
    File,
    Form,
    Header,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from doc_processing.events import JobEvent
from doc_processing.webhooks import (
    DELIVERY_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    verify_webhook_signature,
)
from mock.relay import TERMINAL_FRONTEND_STATES, JobRelay, encode_sse
from mock.store import SQLiteProjectionStore

STATIC_DIR = Path(__file__).with_name("static")
DEFAULT_PROCESSOR_URL = "http://127.0.0.1:8000"
DEFAULT_PROJECTION_PATH = ".data/mock-jobs.sqlite3"
PRIORITY_BY_PRODUCER = {
    "chat": "interactive",
    "workflow": "workflow",
    "rag_ingestion": "standard",
    "agent": "bulk",
}
CONTEXT_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,99}")


def normalize_context_id(value: str) -> str:
    normalized = value.strip()
    if CONTEXT_ID_PATTERN.fullmatch(normalized) is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            "context_id must be 1-100 letters, numbers, dots, underscores, colons, or hyphens",
        )
    return normalized


def create_app(
    *,
    processor_url: str | None = None,
    processor_transport: httpx.AsyncBaseTransport | None = None,
    webhook_secret: str | None = None,
    public_webhook_url: str | None = None,
    projection_path: Path | str = DEFAULT_PROJECTION_PATH,
) -> FastAPI:
    """Create the mock backend with an injectable processor transport."""

    resolved_processor_url = processor_url or os.getenv(
        "DOCUMENT_PROCESSOR_URL", DEFAULT_PROCESSOR_URL
    )
    resolved_webhook_secret = webhook_secret or os.getenv("WEBHOOK_SIGNING_SECRET")
    resolved_public_webhook_url = public_webhook_url or os.getenv("MOCK_WEBHOOK_URL")
    relay = JobRelay(SQLiteProjectionStore(projection_path))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        timeout = httpx.Timeout(30.0, read=None)
        async with httpx.AsyncClient(
            base_url=resolved_processor_url,
            transport=processor_transport,
            timeout=timeout,
        ) as processor_client:
            app.state.processor_client = processor_client
            app.state.relay = relay
            app.state.webhook_secret = resolved_webhook_secret
            await relay.start()
            yield

    app = FastAPI(title="Mock Document Backend", version="2.0.0", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def upload_ui() -> FileResponse:
        """Serve the browser UI used to exercise document processing."""

        return FileResponse(
            STATIC_DIR / "index.html",
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/healthz", include_in_schema=False)
    async def mock_health() -> JSONResponse:
        """Identify the exact mock process started by the repository launcher."""

        return JSONResponse(
            {"status": "ok"},
            headers={
                "Cache-Control": "no-store",
                "X-Mock-Instance": os.getenv("MOCK_INSTANCE_TOKEN", ""),
            },
        )

    @app.post("/jobs", status_code=status.HTTP_202_ACCEPTED)
    async def create_job(
        request: Request,
        file: Annotated[UploadFile, File()],
        page_count: Annotated[int, Form(ge=1)] = 1,
        producer_kind: Annotated[str, Form()] = "chat",
        context_id: Annotated[str, Form(max_length=100)] = "default",
    ) -> dict[str, object]:
        """Forward a document and register this backend's webhook endpoint."""

        filename = file.filename or "document"
        content = await file.read()
        media_type = file.content_type or mimetypes.guess_type(filename)[0]
        media_type = media_type or "application/octet-stream"
        normalized_producer = producer_kind.lower()
        if normalized_producer not in PRIORITY_BY_PRODUCER:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                "producer_kind must be one of: " + ", ".join(PRIORITY_BY_PRODUCER),
            )
        normalized_context_id = normalize_context_id(context_id)
        if not request.app.state.webhook_secret:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "processor webhook signing is not configured",
            )
        webhook_url = resolved_public_webhook_url or str(
            request.url_for("receive_processor_webhook")
        )
        client: httpx.AsyncClient = request.app.state.processor_client
        try:
            response = await client.post(
                "/v1/jobs",
                files={"file": (filename, content, media_type)},
                data={
                    "tenant_id": "mock-ui",
                    "producer_kind": normalized_producer,
                    "priority": PRIORITY_BY_PRODUCER[normalized_producer],
                    "page_count": str(page_count),
                    "webhook_url": webhook_url,
                    "artifact_session_id": (
                        normalized_context_id if normalized_producer == "chat" else ""
                    ),
                },
            )
        except httpx.RequestError as exc:
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                "document processor is unavailable",
            ) from exc

        if response.is_error:
            try:
                processor_detail = response.json().get("detail", response.text)
            except json.JSONDecodeError:
                processor_detail = response.text
            raise HTTPException(response.status_code, f"document processor: {processor_detail}")

        payload = response.json()
        projection = await relay.create(
            job_id=payload["job_id"],
            filename=filename,
            producer_kind=normalized_producer,
            context_id=normalized_context_id,
            processor_state=payload["state"],
        )

        return {
            **projection.snapshot(),
            "events_url": str(
                request.url_for("stream_frontend_job_events", job_id=projection.job_id)
            ),
        }

    @app.post("/artifact-sessions/{session_id}/close", status_code=204)
    async def close_artifact_session(request: Request, session_id: str) -> Response:
        """Best-effort browser signal; processor cleanup remains idempotent."""

        normalized_session_id = normalize_context_id(session_id)
        client: httpx.AsyncClient = request.app.state.processor_client
        try:
            response = await client.delete(
                f"/v1/artifact-sessions/{normalized_session_id}",
                params={"tenant_id": "mock-ui"},
            )
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError):
            # A browser lifecycle signal cannot provide reliable response handling.
            # Production callers can retry the idempotent processor endpoint.
            pass
        return Response(status_code=204)

    @app.post("/webhooks/processor", name="receive_processor_webhook", status_code=204)
    async def receive_processor_webhook(
        request: Request,
        delivery_id: Annotated[str | None, Header(alias=DELIVERY_HEADER)] = None,
        timestamp: Annotated[str | None, Header(alias=TIMESTAMP_HEADER)] = None,
        signature: Annotated[str | None, Header(alias=SIGNATURE_HEADER)] = None,
    ) -> Response:
        secret = request.app.state.webhook_secret
        if not secret:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                "processor webhook signing is not configured",
            )
        body = await request.body()
        if (
            timestamp is None
            or signature is None
            or not verify_webhook_signature(secret, timestamp, body, signature)
        ):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid webhook signature")
        try:
            event = JobEvent.model_validate_json(body)
        except ValueError as exc:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, "invalid webhook payload"
            ) from exc
        if delivery_id != event.event_id:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "webhook id mismatch")
        processor_state = event.data.get("state")
        if not isinstance(processor_state, str):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, "webhook state is missing"
            )
        try:
            await relay.publish(event.job_id, processor_state, event.job_version)
        except KeyError as exc:
            # The earliest job events can race the submission response. A
            # retryable response preserves them until the projection exists.
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "job projection is not ready"
            ) from exc
        return Response(status_code=204)

    @app.get("/jobs")
    async def list_jobs(
        request: Request,
        producer_kind: Annotated[str | None, Query()] = None,
        context_id: Annotated[str | None, Query(max_length=100)] = None,
    ) -> dict[str, object]:
        normalized_producer = producer_kind.lower() if producer_kind else None
        if (
            normalized_producer is not None
            and normalized_producer not in PRIORITY_BY_PRODUCER
        ):
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT,
                "producer_kind must be one of: " + ", ".join(PRIORITY_BY_PRODUCER),
            )
        normalized_context_id = (
            normalize_context_id(context_id) if context_id is not None else None
        )
        jobs = []
        for projection in await relay.list_persistent(
            producer_kind=normalized_producer,
            context_id=normalized_context_id,
        ):
            jobs.append(
                {
                    **projection.snapshot(),
                    "events_url": str(
                        request.url_for(
                            "stream_frontend_job_events", job_id=projection.job_id
                        )
                    ),
                }
            )
        return {"jobs": jobs}

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> dict[str, object]:
        try:
            projection = await relay.get(job_id)
        except KeyError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from exc
        return projection.snapshot()

    @app.get("/jobs/{job_id}/events", name="stream_frontend_job_events")
    async def stream_job_events(
        request: Request,
        job_id: str,
        last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
    ) -> StreamingResponse:
        try:
            after_sequence = int(last_event_id) if last_event_id else 0
        except ValueError as exc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid Last-Event-ID") from exc

        try:
            await relay.get(job_id)
        except KeyError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found") from exc

        async def generate() -> AsyncIterator[str]:
            async with relay.subscribe(
                job_id, after_sequence=after_sequence
            ) as (backfill, queue):
                for event in backfill:
                    yield encode_sse(event)
                    if event.state in TERMINAL_FRONTEND_STATES:
                        return

                projection = await relay.get(job_id)
                if projection.terminal:
                    return

                while True:
                    if await request.is_disconnected():
                        return
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=15)
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    yield encode_sse(event)
                    if event.state in TERMINAL_FRONTEND_STATES:
                        return

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/upload")
    async def upload(
        file: Annotated[UploadFile | None, File()] = None,
        files: Annotated[list[UploadFile] | None, File()] = None,
    ) -> dict[str, object]:
        """Retain the original multipart echo endpoint for client fixtures."""

        uploads = ([file] if file is not None else []) + (files or [])
        if not uploads:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="provide a 'file' field or one or more 'files' fields",
            )

        received = []
        for item in uploads:
            content = await item.read()
            received.append(
                {
                    "filename": item.filename,
                    "content_type": item.content_type,
                    "size_bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )

        return {
            "mode": "single" if len(received) == 1 else "batch",
            "count": len(received),
            "files": received,
        }

    return app


app = create_app()
