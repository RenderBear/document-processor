#!/bin/sh
set -eu

repository_root=$(git rev-parse --show-toplevel)
project_name="doc-processing-contract-$$"

compose() {
  docker compose \
    --project-directory "$repository_root" \
    --project-name "$project_name" \
    --file "$repository_root/docker-compose.yml" \
    --file "$repository_root/docker-compose.integration.yml" \
    "$@"
}

cleanup() {
  compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

compose up --build --detach --wait
compose exec -T app python - submit < "$repository_root/tests/runtime_integration_smoke.py"
compose restart processor app >/dev/null

attempt=0
until compose exec -T app python -c \
  "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/readyz', timeout=2)" \
  >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 30 ]; then
    echo "runtime integration app did not become ready after restart" >&2
    exit 1
  fi
  sleep 1
done

compose exec -T app python - verify < "$repository_root/tests/runtime_integration_smoke.py"
