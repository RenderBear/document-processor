#!/bin/sh
set -eu

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
mock_pid_file="$repository_root/.data/mock-server.pid"

if [ -f "$mock_pid_file" ]; then
  remove_mock_pid=true
  mock_pid=$(sed -n '1p' "$mock_pid_file")
  case "$mock_pid" in
    ''|*[!0-9]*) mock_command="" ;;
    *) mock_command=$(ps -p "$mock_pid" -o command= 2>/dev/null || true) ;;
  esac
  case "$mock_command" in
    *mock.app:app*)
      echo "Stopping the mock browser backend..."
      kill "$mock_pid" 2>/dev/null || true
      stop_attempt=0
      while kill -0 "$mock_pid" 2>/dev/null && [ "$stop_attempt" -lt 20 ]; do
        stop_attempt=$((stop_attempt + 1))
        sleep 0.1
      done
      if kill -0 "$mock_pid" 2>/dev/null; then
        echo "The mock browser backend did not stop; preserving its process record." >&2
        remove_mock_pid=false
      fi
      ;;
    *)
      echo "Ignoring stale mock process record."
      ;;
  esac
  if [ "$remove_mock_pid" = "true" ]; then
    rm -f "$mock_pid_file"
  fi
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found on PATH." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose is required. Install the Docker Compose plugin and try again." >&2
  exit 1
fi

echo "Stopping the document API, PostgreSQL, and SeaweedFS..."
docker compose \
  --project-directory "$repository_root" \
  --file "$repository_root/docker-compose.yml" \
  down --remove-orphans

echo "Stack stopped. PostgreSQL and SeaweedFS data volumes were preserved."
