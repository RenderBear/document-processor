from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_start_help_documents_mock_flag() -> None:
    result = subprocess.run(
        ["sh", str(ROOT / "start.sh"), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "--with-mock" in result.stdout


def test_start_script_makes_mock_runtime_opt_in() -> None:
    script = (ROOT / "start.sh").read_text()

    assert 'file "$repository_root/docker-compose.yml"' in script
    assert "compose up --build --detach --wait" in script
    assert "compose up --detach --wait postgres nats seaweedfs" in script
    assert 'migrations/*.sql' in script
    assert "psql -v ON_ERROR_STOP=1 --single-transaction" in script
    assert "with_mock=false" in script
    assert "--with-mock" in script
    assert 'if [ "$with_mock" = "true" ]; then' in script
    assert '"$repository_root/.venv/bin/uvicorn" mock.app:app' in script
    assert '--app-dir "$repository_root"' in script
    assert "host.docker.internal:8001/webhooks/processor" in script
    assert "MOCK_INSTANCE_TOKEN" in script
    assert "127.0.0.1:8001/healthz" in script
    assert "Port 8001 is already in use" in script
    assert "</dev/null" in script
    assert ".data/mock-server.pid" in script
    assert "docker-compose.integration.yml" not in script


def test_stop_script_preserves_persistent_data() -> None:
    script = (ROOT / "stop.sh").read_text()

    assert "down --remove-orphans" in script
    assert "--volumes" not in script
    assert "mock.app:app" in script
    assert ".data/mock-server.pid" in script
