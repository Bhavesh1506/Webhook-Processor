"""Transactional order processing and retry scheduling."""

import json
from datetime import datetime, timedelta, timezone

from app.database import connect
from app.repository import due_events, now

MAX_ATTEMPTS = 3


def process_event(database_path: str, event_id: str, should_fail=lambda _event: False) -> str:
    """Apply the order update and event status in one transaction or neither."""
    connection = connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        if row is None or row["status"] in ("processed", "dead_lettered"):
            connection.rollback()
            return row["status"] if row else "missing"

        event = json.loads(row["payload"])
        try:
            if should_fail(event):
                raise RuntimeError("simulated processing failure")
            # The event idempotency key protects this write from duplicate deliveries.
            connection.execute(
                "INSERT INTO orders(order_id, amount, status, paid_at) VALUES (?, ?, 'paid', ?) "
                "ON CONFLICT(order_id) DO UPDATE SET amount = excluded.amount, status = 'paid', paid_at = excluded.paid_at",
                (event["order_id"], event["amount"], now()),
            )
            connection.execute(
                "UPDATE events SET status = 'processed', processed_at = ?, last_error = NULL WHERE event_id = ?",
                (now(), event_id),
            )
            connection.commit()
            return "processed"
        except Exception as error:
            attempts = row["attempts"] + 1
            if attempts >= MAX_ATTEMPTS:
                connection.execute(
                    "INSERT OR REPLACE INTO dead_letters(event_id, payload, attempts, last_error, created_at, dead_lettered_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (event_id, row["payload"], attempts, str(error), row["created_at"], now()),
                )
                connection.execute(
                    "UPDATE events SET status = 'dead_lettered', attempts = ?, last_error = ? WHERE event_id = ?",
                    (attempts, str(error), event_id),
                )
                connection.commit()
                return "dead_lettered"

            delay_seconds = 2 ** (attempts - 1)
            retry_at = (datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).isoformat()
            connection.execute(
                "UPDATE events SET status = 'failed', attempts = ?, next_attempt_at = ?, last_error = ? WHERE event_id = ?",
                (attempts, retry_at, str(error), event_id),
            )
            connection.commit()
            return "failed"
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def retry_due_events(database_path: str, should_fail=lambda _event: False) -> list[str]:
    """Resume work left as received after a crash, plus scheduled failed work."""
    return [process_event(database_path, row["event_id"], should_fail) for row in due_events(database_path, now())]
