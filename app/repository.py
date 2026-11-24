"""Small SQL operations kept separate from HTTP concerns."""

import json
import sqlite3
from datetime import datetime, timezone

from app.database import connect
from app.validation import PaymentEvent


class EventConflict(ValueError):
    """An existing receipt ID was reused for different event content."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def insert_event(database_path: str, event: PaymentEvent) -> bool:
    """Return False only when SQLite's primary key says we saw this event before."""
    connection = connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        payload = json.dumps(event, sort_keys=True, ensure_ascii=True)
        cursor = connection.execute(
            "INSERT INTO events (event_id, payload, status, occurred_at, created_at) VALUES (?, ?, 'received', ?, ?) "
            "ON CONFLICT(event_id) DO NOTHING",
            (event["event_id"], payload, event["occurred_at"], now()),
        )
        inserted = cursor.rowcount == 1
        if not inserted:
            row = connection.execute("SELECT payload FROM events WHERE event_id = ?", (event["event_id"],)).fetchone()
            stored = json.loads(row["payload"])
            incoming = json.loads(payload)
            # Pre-migration receipts may have no gateway occurrence time to compare.
            if isinstance(stored, dict) and "occurred_at" not in stored:
                incoming.pop("occurred_at")
            if json.dumps(stored, sort_keys=True) != json.dumps(incoming, sort_keys=True):
                raise EventConflict("event_id already belongs to different content")
            connection.execute(
                "INSERT INTO counters(name, value) VALUES ('duplicates_skipped', 1) "
                "ON CONFLICT(name) DO UPDATE SET value = value + 1"
            )
        connection.commit()
        return inserted
    finally:
        connection.close()


def get_event(database_path: str, event_id: str) -> sqlite3.Row | None:
    connection = connect(database_path)
    try:
        return connection.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
    finally:
        connection.close()


def due_events(database_path: str, current_time: str, limit: int = 100) -> list[str]:
    connection = connect(database_path)
    try:
        return [row["event_id"] for row in connection.execute(
            "SELECT event_id FROM events WHERE status IN ('received', 'failed') "
            "AND (status = 'received' OR next_attempt_at IS NULL OR next_attempt_at <= ?) "
            "ORDER BY created_at, event_id LIMIT ?",
            (current_time, limit),
        )]
    finally:
        connection.close()


def dead_letters(database_path: str, limit: int = 100, offset: int = 0) -> list[sqlite3.Row]:
    connection = connect(database_path)
    try:
        return connection.execute(
            "SELECT * FROM dead_letters ORDER BY dead_lettered_at DESC, event_id LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    finally:
        connection.close()


def metrics(database_path: str) -> dict[str, int]:
    connection = connect(database_path)
    try:
        connection.execute("BEGIN")
        counts: dict[str, int] = dict(connection.execute("SELECT status, COUNT(*) FROM events GROUP BY status").fetchall())
        duplicate_row = connection.execute("SELECT value FROM counters WHERE name = 'duplicates_skipped'").fetchone()
        return {
            "received": sum(counts.values()),
            "duplicates_skipped": duplicate_row[0] if duplicate_row else 0,
            "processed": counts.get("processed", 0),
            "failed": counts.get("failed", 0),
            "dead_lettered": counts.get("dead_lettered", 0),
        }
    finally:
        connection.close()
