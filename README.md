# Document processing service

An asynchronous OCR orchestration service. It accepts PDF, DOCX, and PPTX documents from several
producers, schedules them fairly, routes each one to one of `N` OCR paths, and returns Markdown.

This repository is the runnable v0.1 slice. Both OCR paths are deterministic mocks.

<p align="center">
  <img src="docs/assets/flow.svg" width="880" alt="Documents enter a fair queue, a YAML policy decides which job goes first and which OCR path runs it, and Markdown comes back.">
</p>

**Design, rationale, and the target v1 architecture: [`docs/spec.md`](docs/spec.md).**

| Document | Covers |
|---|---|
| [`docs/spec.md`](docs/spec.md) | Target v1 architecture and rollout |
| [`docs/runtime-architecture.md`](docs/runtime-architecture.md) | What the v0.1 slice actually does, boundary by boundary |
| [`docs/policy.md`](docs/policy.md) | Scheduling and routing policy contract |
| [`docs/mock-event-relay.md`](docs/mock-event-relay.md) | Mock UI and webhook-to-SSE relay |

## Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync --extra dev
uv run uvicorn doc_processing.main:app --reload
```

Runs on port 8000 with an in-memory job store and artifacts in `.data/artifacts`.
Interactive API docs are at `http://localhost:8000/docs`.

## Full stack (Docker)

API, processor, PostgreSQL, NATS JetStream, and SeaweedFS:

```bash
./start.sh                 # main stack
./start.sh --with-mock     # main stack + mock UI on port 8001
./stop.sh
```

| Port | Service |
|---|---|
| 8000 | Document API |
| 8001 | Mock UI (`--with-mock`) |
| 5432 | PostgreSQL |
| 4222 / 8222 | NATS client / monitoring |
| 8333 | SeaweedFS S3 endpoint |

The Compose credentials are for local development only.

## Submit a job

```bash
curl -i http://localhost:8000/v1/jobs \
  -F 'file=@./sample.pdf;type=application/pdf' \
  -F 'tenant_id=tenant-demo' \
  -F 'producer_kind=agent'
```

| Field | Notes |
|---|---|
| `file` or `upload_id` | Exactly one is required |
| `tenant_id` | Defaults to `default`; with `upload_id` it comes from the grant |
| `producer_kind` | `chat`, `agent`, `rag_ingestion`, `workflow`; selects the service class |
| `artifact_session_id` | Required for `chat`; closing the session deletes its artifacts |
| `priority` | `bulk`, `standard`, `workflow`, `interactive`; used only without `producer_kind` |
| `page_count` | Optional hint; preflight replaces it with the real count |
| `data_classification`, `home_region` | Optional hard routing gates |
| `webhook_url` | Optional; host must be in `WEBHOOK_ALLOWED_HOSTS` |

## Follow a job

The `202` response returns these URLs:

```bash
curl http://localhost:8000/v1/jobs/JOB_ID                    # status
curl http://localhost:8000/v1/jobs/JOB_ID/events             # ordered events
curl -N http://localhost:8000/v1/jobs/JOB_ID/events/stream   # SSE
curl http://localhost:8000/v1/jobs/JOB_ID/result             # Markdown; 409 until succeeded
```

States: `accepted → validating → queued → processing → succeeded | failed | rejected | cancelled`.

Resume events with `Last-Event-ID` or `?after_version=N`.

## Large uploads (two-step)

Keeps the file body off the API. Requires `UPLOAD_SIGNING_SECRET`.

```bash
# 1. Get a signed upload grant
curl -s http://localhost:8000/v1/uploads \
  -H 'content-type: application/json' \
  -d '{"filename":"sample.pdf","tenant_id":"tenant-demo","producer_kind":"agent"}'

# 2. PUT the bytes to the returned upload.url
curl -X PUT --data-binary @./sample.pdf '<upload.url>'

# 3. Create the job from the grant
curl -i http://localhost:8000/v1/jobs -F 'upload_id=<upload_id>'
```

## Other endpoints

```text
GET    /healthz
GET    /readyz
GET    /v1/ocr-paths
DELETE /v1/artifact-sessions/{session_id}?tenant_id=...
```

## Mock UI

`./start.sh --with-mock` is the easy way. To run it by hand with reload:

```bash
# terminal 1: processor
export WEBHOOK_SIGNING_SECRET=any-local-secret
export WEBHOOK_ALLOWED_HOSTS=127.0.0.1,localhost
uv run uvicorn doc_processing.main:app --reload --port 8000
```

```bash
# terminal 2: mock backend + UI (same secret)
export WEBHOOK_SIGNING_SECRET=any-local-secret
uv run uvicorn mock.app:app --reload --port 8001
```

Open `http://localhost:8001`, pick a source (Chat, RAG, Agent, Workflow), and upload a document.

Upload the fixture files in `mock/data` from the command line:

```bash
uv run --extra dev python -m mock.upload
uv run --extra dev python -m mock.upload --data-dir /path/to/fixtures
```

## Configuration

| Variable | Values | Default |
|---|---|---|
| `JOB_STORE_BACKEND` | `memory`, `postgres` | `memory` |
| `DATABASE_URL` | PostgreSQL URL | required for `postgres` |
| `ARTIFACT_STORAGE_BACKEND` | `local`, `s3` | `local` |
| `LOCAL_ARTIFACT_ROOT` | directory | `.data/artifacts` |
| `S3_BUCKET`, `S3_ENDPOINT_URL` | existing bucket, endpoint | omit the endpoint for AWS S3 |
| `DISPATCH_BACKEND` | `memory`, `jetstream` | `memory` |
| `NATS_URL` | NATS URL | `nats://127.0.0.1:4222` |
| `SERVICE_ROLE` | `all`, `api`, `worker` | `all` |
| `OCR_CONFIG_PATH` | YAML file | `configs/ocr-paths.yaml` |
| `POLICY_CONFIG_PATH` | YAML file | `configs/policy.yaml` |
| `UPLOAD_SIGNING_SECRET` | secret | unset (two-step upload off) |
| `WEBHOOK_SIGNING_SECRET`, `WEBHOOK_ALLOWED_HOSTS` | secret, host list | unset (webhooks off) |
| `JETSTREAM_*` | stream name, replicas, limits | see `_dispatch_from_environment` in [`api.py`](src/doc_processing/api.py) |

S3 credentials come from the standard AWS credential chain. Do not add a `.env` file to the repository.

PostgreSQL outside Compose:

```bash
for f in migrations/*.sql; do psql "$DATABASE_URL" -f "$f"; done
JOB_STORE_BACKEND=postgres uv run uvicorn doc_processing.main:app --reload
```

## OCR paths and policy

- [`configs/ocr-paths.yaml`](configs/ocr-paths.yaml): one entry per OCR path, with its adapter,
  capabilities, hard limits, fallbacks, and compliance gates. Invalid files fail at startup.
- [`configs/policy.yaml`](configs/policy.yaml): maps producers to service classes, sets the
  scheduling algorithm (`weighted_fair_v1` or `rank_v1`), routing objectives, and failure rules.
- To replace the built-in policy engine, pass a `PolicyEngine` to `OCRService` or `create_app`.
  See [`docs/policy.md`](docs/policy.md).

## Tests

```bash
uv run --extra dev python -m pytest              # default suite
uv run --extra dev python -m pytest -m perf -s   # opt-in performance suite, prints measurements
```

```bash
sh scripts/verify-runtime-integrations.sh   # integration run against Compose (needs Docker)
```

## Limitations

- OCR is mocked; output does not depend on the source document.
- No authentication. `tenant_id`, `producer_kind`, and compliance fields are trusted as sent.
- Scheduling, rate limits, and capacity are process-local: run one processor replica.
- Live SSE and webhook delivery are single-replica.
- The memory job store loses everything on exit.
- No malware scanning, tenant quotas, general cancellation API, or session expiry sweeper.

Details: [`docs/runtime-architecture.md`](docs/runtime-architecture.md).
