#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
mock_pid_file="$repository_root/.data/mock-server.pid"
mock_log_file="$repository_root/.data/mock-server.log"
with_mock=false

usage() {
  cat <<'EOF'
Usage: ./start.sh [--with-mock]

Starts the document API, processor, PostgreSQL, JetStream, and SeaweedFS,
applying local PostgreSQL migrations before the document services start.
  --with-mock  Also start the mock browser backend at http://127.0.0.1:8001
EOF
}

case "${1:-}" in
  "") ;;
  --with-mock) with_mock=true ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown option: $1" >&2
    usage >&2
    exit 2
    ;;
esac

if [ "$#" -gt 1 ]; then
  echo "Only one option may be supplied." >&2
  usage >&2
  exit 2
fi

compose() {
  docker compose \
    --project-directory "$repository_root" \
    --file "$repository_root/docker-compose.yml" \
    "$@"
}

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found on PATH." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose is required. Install the Docker Compose plugin and try again." >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "The Docker daemon is not available. Start Docker and try again." >&2
  exit 1
fi

if [ "$with_mock" = "true" ]; then
  if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required to start the mock browser backend." >&2
    exit 1
  fi

  if [ -z "${WEBHOOK_SIGNING_SECRET:-}" ]; then
    WEBHOOK_SIGNING_SECRET=$(uv run python -c 'import secrets; print(secrets.token_hex(32))')
  fi
  case ",${WEBHOOK_ALLOWED_HOSTS:-}," in
    *,host.docker.internal,*) ;;
    ,,) WEBHOOK_ALLOWED_HOSTS=host.docker.internal ;;
    *) WEBHOOK_ALLOWED_HOSTS="${WEBHOOK_ALLOWED_HOSTS},host.docker.internal" ;;
  esac
  export WEBHOOK_SIGNING_SECRET WEBHOOK_ALLOWED_HOSTS
fi

echo "Starting PostgreSQL, JetStream, and SeaweedFS..."
if ! compose up --detach --wait postgres nats seaweedfs; then
  echo "The persistence dependencies did not become healthy. Current service status:" >&2
  compose ps >&2 || true
  echo "Run ./stop.sh when you are done inspecting the failure." >&2
  exit 1
fi

echo "Applying PostgreSQL migrations..."
for migration in "$repository_root"/migrations/*.sql; do
  if ! compose exec -T postgres \
    psql -v ON_ERROR_STOP=1 --single-transaction \
      -U doc-processing -d doc-processing -f - <"$migration"; then
    echo "Migration failed: $migration" >&2
    echo "The document services were not started." >&2
    exit 1
  fi
done

echo "Starting the document API and processor..."
if ! compose up --build --detach --wait; then
  echo "The stack did not become healthy. Current service status:" >&2
  compose ps >&2 || true
  echo "Run ./stop.sh when you are done inspecting the failure." >&2
  exit 1
fi

compose ps
echo
echo "Document API: http://127.0.0.1:8000"
echo "Readiness:    http://127.0.0.1:8000/readyz"

if [ "$with_mock" = "true" ]; then
  mkdir -p "$repository_root/.data"
  if [ -f "$mock_pid_file" ]; then
    previous_pid=$(sed -n '1p' "$mock_pid_file")
    case "$previous_pid" in
      ''|*[!0-9]*) ;;
      *)
        previous_command=$(ps -p "$previous_pid" -o command= 2>/dev/null || true)
        case "$previous_command" in
          *mock.app:app*)
            echo "Stopping previously managed mock browser backend..."
            kill "$previous_pid" 2>/dev/null || true
            stop_attempt=0
            while kill -0 "$previous_pid" 2>/dev/null && [ "$stop_attempt" -lt 20 ]; do
              stop_attempt=$((stop_attempt + 1))
              sleep 0.1
            done
            if kill -0 "$previous_pid" 2>/dev/null; then
              echo "The previous mock browser backend did not stop." >&2
              exit 1
            fi
            ;;
        esac
        ;;
    esac
    rm -f "$mock_pid_file"
  fi

  if uv run python -c \
    "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8001/healthz', timeout=1)" \
    >/dev/null 2>&1; then
    echo "Port 8001 is already in use. The launcher will not reuse or stop an unmanaged process." >&2
    echo "Stop the existing process, then run ./start.sh --with-mock again." >&2
    exit 1
  fi

  mock_instance_token=$(uv run python -c 'import secrets; print(secrets.token_hex(16))')
  echo "Starting the mock browser backend..."
  nohup env \
    DOCUMENT_PROCESSOR_URL=http://127.0.0.1:8000 \
    MOCK_WEBHOOK_URL=http://host.docker.internal:8001/webhooks/processor \
    MOCK_INSTANCE_TOKEN="$mock_instance_token" \
    "$repository_root/.venv/bin/uvicorn" mock.app:app \
      --app-dir "$repository_root" --host 127.0.0.1 --port 8001 \
    >"$mock_log_file" 2>&1 </dev/null &
  mock_pid=$!
  echo "$mock_pid" >"$mock_pid_file"

  mock_ready=false
  attempt=0
  while [ "$attempt" -lt 20 ]; do
    if ! kill -0 "$mock_pid" 2>/dev/null; then
      break
    fi
    if MOCK_INSTANCE_TOKEN="$mock_instance_token" uv run python -c \
      'import os, urllib.request; response = urllib.request.urlopen("http://127.0.0.1:8001/healthz", timeout=1); raise SystemExit(response.headers.get("X-Mock-Instance") != os.environ["MOCK_INSTANCE_TOKEN"])' \
      >/dev/null 2>&1; then
      mock_ready=true
      break
    fi
    attempt=$((attempt + 1))
    sleep 0.5
  done

  if [ "$mock_ready" != "true" ]; then
    echo "The mock browser backend did not become ready. See $mock_log_file" >&2
    tail -20 "$mock_log_file" >&2 || true
    kill "$mock_pid" 2>/dev/null || true
    rm -f "$mock_pid_file"
    exit 1
  fi
  echo "Mock UI:      http://127.0.0.1:8001"
  echo "Mock log:     $mock_log_file"
fi

echo "Stop with:    ./stop.sh"
