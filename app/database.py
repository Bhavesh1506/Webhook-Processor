"""SQLite connection and schema helpers."""

import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL,
    paid_at TEXT NOT NULL
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
    # WAL lets a reader continue while a writer is committing.
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def initialize(database_path: str) -> None:
    connection = connect(database_path)
    try:
        connection.executescript(SCHEMA)
    finally:
        connection.close()
