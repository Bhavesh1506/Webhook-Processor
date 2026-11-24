"""SQLite connection and schema helpers."""

import sqlite3
from pathlib import Path
from time import monotonic, sleep


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT NOT NULL PRIMARY KEY,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT NOT NULL PRIMARY KEY,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('paid', 'failed', 'refunded')),
    last_event_occurred_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letters (
    event_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT NOT NULL,
    created_at TEXT NOT NULL,
    dead_lettered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


def connect(database_path: str) -> sqlite3.Connection:
    """Open one short-lived connection so requests do not share cursors."""
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path, timeout=5, isolation_level=None)
    connection.row_factory = sqlite3.Row
    return connection


def is_busy(error: sqlite3.OperationalError) -> bool:
    return (getattr(error, "sqlite_errorcode", 0) & 0xFF) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)


def initialize(database_path: str) -> None:
    connection = connect(database_path)
    try:
        # Inspect and migrate under the same writer lock across startups.
        # Concurrent first opens can collide while changing the journal mode.
        deadline = monotonic() + 5
        while True:
            try:
                mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if mode.lower() != "wal":
                    raise RuntimeError("database must support WAL mode")
                break
            except sqlite3.OperationalError as error:
                if not is_busy(error) or monotonic() >= deadline:
                    raise
                sleep(0.05)
        connection.execute("BEGIN IMMEDIATE")
        for statement in SCHEMA.split(";"):
            if statement.strip():
                connection.execute(statement)
        _migrate_legacy_events(connection)
        _migrate_legacy_orders(connection)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS events_pending ON events(created_at, event_id) "
            "WHERE status IN ('received', 'failed')"
        )
        connection.commit()
    finally:
        connection.close()


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _migrate_legacy_events(connection: sqlite3.Connection) -> None:
    """Rebuild only old tables; fail rather than discard a legacy event with no ID."""
    columns = _columns(connection, "events")
    if "occurred_at" in columns:
        return
    invalid_count = connection.execute(
        "SELECT COUNT(*) FROM events WHERE event_id IS NULL OR trim(event_id) = ''"
    ).fetchone()[0]
    if invalid_count:
        raise RuntimeError("cannot migrate events containing empty event IDs; repair the data first")

    connection.execute(
        "CREATE TABLE events_new ("
        "event_id TEXT NOT NULL PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL, "
        "occurred_at TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT, "
        "last_error TEXT, created_at TEXT NOT NULL, processed_at TEXT)"
    )
    # Legacy rows did not contain gateway occurrence times, so created_at is the best preserved fallback.
    connection.execute(
        "INSERT INTO events_new(event_id, payload, status, occurred_at, attempts, next_attempt_at, last_error, created_at, processed_at) "
        "SELECT event_id, payload, status, created_at, attempts, next_attempt_at, last_error, created_at, processed_at FROM events"
    )
    connection.execute("DROP TABLE events")
    connection.execute("ALTER TABLE events_new RENAME TO events")


def _migrate_legacy_orders(connection: sqlite3.Connection) -> None:
    """Keep existing orders while adding the timestamp needed for safe event ordering."""
    columns = _columns(connection, "orders")
    if "last_event_occurred_at" in columns:
        return
    invalid_count = connection.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id IS NULL OR trim(order_id) = ''"
    ).fetchone()[0]
    if invalid_count:
        raise RuntimeError("cannot migrate orders containing empty order IDs; repair the data first")

    connection.execute(
        "CREATE TABLE orders_new (order_id TEXT NOT NULL PRIMARY KEY, amount INTEGER NOT NULL, "
        "status TEXT NOT NULL CHECK(status IN ('paid', 'failed', 'refunded')), "
        "last_event_occurred_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    connection.execute(
        "INSERT INTO orders_new(order_id, amount, status, last_event_occurred_at, updated_at) "
        "SELECT order_id, amount, status, paid_at, paid_at FROM orders"
    )
    connection.execute("DROP TABLE orders")
    connection.execute("ALTER TABLE orders_new RENAME TO orders")
