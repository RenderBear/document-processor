# Mock application event relay

## Purpose

The mock application demonstrates the product-facing integration without making the browser a
direct document-processor client. The document processor remains authoritative for job state. The
mock backend forwards documents with its webhook URL, verifies signed processor lifecycle events,
projects them into frontend states, and exposes its own server-sent event (SSE) stream to the
browser.

The repository launcher keeps this demonstration opt-in. Plain `./start.sh` starts the document
API, processor, JetStream, and persistence dependencies; `./start.sh --with-mock` additionally starts the
host mock backend, configures the container-reachable callback host, and shares a runtime-generated
signing secret. It refuses to reuse an unmanaged listener on the mock port and accepts readiness
only from the per-launch instance it started. `./stop.sh` stops a mock process only when it matches
the PID recorded by that launcher. The browser UI must not present a static relay-health claim:
serving the page proves only that the mock backend is reachable, not that the processor or webhook
path is healthy.

Processor webhook subscriptions are recorded with the accepted job. Retained lifecycle events act
as the durable outbox: delivery is ordered per job, signed over the exact body and timestamp,
retried with bounded exponential backoff for transient failures, and recorded independently from
OCR success. Callback hosts must be explicitly allowed by processor configuration. Receivers
deduplicate by processor event ID and monotonically increasing job version.

## State relay contract

The backend normalizes processor states for the frontend:

| Processor state | Frontend state |
|---|---|
| `accepted`, `queued` | `queued` |
| `processing` | `processing` |
| `succeeded` | `completed` |
| `failed`, `rejected`, `cancelled` | `failed` |

The browser submits each file, a producer kind, and a bounded `context_id` to `POST /jobs` on the
mock backend. `chat` receives a new context for each browser session and remains an ephemeral
projection. `rag_ingestion`, `agent`, and `workflow` projections and normalized
event histories are persisted by the mock backend and listed again when the page reopens. Their
contexts identify a knowledge base, agent, or workflow destination; they are routing and display
scopes, not authorization credentials. Production ingress must derive both tenant and permitted
context from authenticated identity rather than trusting browser fields.

`GET /jobs` accepts `producer_kind` and `context_id` filters, but the page presents a single
producer-agnostic processing queue that mirrors the one shared scheduler queue: it requests all
persisted projections, renders them together with this session's uploads newest-first, and opens
live streams for every unfinished job. The producer and context selection only tags new uploads;
changing it re-targets the compose area without re-scoping the queue. The backend still partitions
persistence by producer kind and context and still honors the scope filters when they are supplied.
Existing saved projections that predate `context_id` restore into the `default` context.

Relay events have a monotonically increasing integer sequence and resume with `Last-Event-ID`.
The response contains the normalized initial state, category, context, and a backend-owned
`events_url`; the browser never receives processor endpoints or webhook credentials. On browser
close, the page sends a best-effort request through the mock backend to close its chat artifact
session; that idempotently cancels unfinished chat work and releases chat source/result objects.
Closing the browser never closes the background artifact lifecycle used by RAG ingestion,
agent, or workflow work. SQLite is the mock backend's local persistence; the
processor's canonical job, event, and artifact lifecycle state remains independent.
