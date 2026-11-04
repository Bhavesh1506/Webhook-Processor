"""Small SQL operations kept separate from HTTP concerns."""

import json
import sqlite3
from datetime import datetime, timezone

from app.database import connect


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def insert_event(database_path: str, event: dict) -> bool:
    """Return False only when SQLite's primary key says we saw this event before."""
    connection = connect(database_path)
    try:
        connection.execute(
            "INSERT INTO events (event_id, payload, status, created_at) VALUES (?, ?, 'received', ?)",
            (event["event_id"], json.dumps(event, separators=(",", ":")), now()),
        )
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        connection.close()


def get_event(database_path: str, event_id: str) -> sqlite3.Row | None:
    connection = connect(database_path)
    try:
        return connection.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
    finally:
        connection.close()


def due_events(database_path: str, current_time: str) -> list[sqlite3.Row]:
    connection = connect(database_path)
    try:
        return connection.execute(
            "SELECT * FROM events WHERE status IN ('received', 'failed') "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY created_at",
            (current_time,),
        ).fetchall()
    finally:
        connection.close()


def dead_letters(database_path: str) -> list[sqlite3.Row]:
    connection = connect(database_path)
    try:
        return connection.execute("SELECT * FROM dead_letters ORDER BY dead_lettered_at DESC").fetchall()
    finally:
        connection.close()


def metrics(database_path: str) -> dict[str, int]:
    connection = connect(database_path)
    try:
        counts = dict(connection.execute("SELECT status, COUNT(*) FROM events GROUP BY status").fetchall())
        received_total = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        duplicates = connection.execute("SELECT COUNT(*) FROM events WHERE event_id LIKE 'duplicate:%'").fetchone()[0]
        # Duplicate requests are not events, so their count is recorded by the app in a tiny meta table below.
        connection.execute("CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        duplicate_row = connection.execute("SELECT value FROM counters WHERE name = 'duplicates_skipped'").fetchone()
        return {
            "received": received_total,
            "duplicates_skipped": duplicate_row[0] if duplicate_row else duplicates,
            "processed": counts.get("processed", 0),
            "failed": counts.get("failed", 0),
            "dead_lettered": counts.get("dead_lettered", 0),
        }
    finally:
        connection.close()


def increment_duplicate_counter(database_path: str) -> None:
    connection = connect(database_path)
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        connection.execute(
            "INSERT INTO counters(name, value) VALUES ('duplicates_skipped', 1) "
            "ON CONFLICT(name) DO UPDATE SET value = value + 1"
        )
    finally:
        connection.close()
