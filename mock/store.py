"""SQLite persistence for non-chat frontend job projections."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StoredProjection:
    payload: dict[str, object]
    events: tuple[dict[str, object], ...]


class SQLiteProjectionStore:
    """Persist normalized UI state without storing document payloads."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()

    async def start(self) -> None:
        await asyncio.to_thread(self._start_sync)

    async def save(
        self,
        payload: dict[str, object],
        events: tuple[dict[str, object], ...],
    ) -> None:
        await asyncio.to_thread(self._save_sync, payload, events)

    async def load(self) -> list[StoredProjection]:
        return await asyncio.to_thread(self._load_sync)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _start_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS frontend_job (
                    job_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS frontend_event (
                    job_id TEXT NOT NULL REFERENCES frontend_job(job_id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (job_id, sequence)
                );
                """
            )

    def _save_sync(
        self,
        payload: dict[str, object],
        events: tuple[dict[str, object], ...],
    ) -> None:
        job_id = str(payload["job_id"])
        serialized_payload = json.dumps(payload, separators=(",", ":"))
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                INSERT INTO frontend_job (job_id, payload) VALUES (?, ?)
                ON CONFLICT(job_id) DO UPDATE SET payload = excluded.payload
                """,
                (job_id, serialized_payload),
            )
            connection.executemany(
                """
                INSERT INTO frontend_event (job_id, sequence, payload) VALUES (?, ?, ?)
                ON CONFLICT(job_id, sequence) DO UPDATE SET payload = excluded.payload
                """,
                (
                    (
                        job_id,
                        int(event["sequence"]),
                        json.dumps(event, separators=(",", ":")),
                    )
                    for event in events
                ),
            )

    def _load_sync(self) -> list[StoredProjection]:
        with self._lock, self._connect() as connection:
            jobs = connection.execute(
                "SELECT job_id, payload FROM frontend_job ORDER BY rowid"
            ).fetchall()
            restored = []
            for job_id, raw_payload in jobs:
                rows = connection.execute(
                    """
                    SELECT payload FROM frontend_event
                    WHERE job_id = ? ORDER BY sequence
                    """,
                    (job_id,),
                ).fetchall()
                restored.append(
                    StoredProjection(
                        payload=json.loads(raw_payload),
                        events=tuple(json.loads(row[0]) for row in rows),
                    )
                )
            return restored
