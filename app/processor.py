"""Transactional order processing and retry scheduling."""

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from typing import Callable

from app.database import connect
from app.repository import due_events, now
from app.validation import PaymentEvent, validate_event

MAX_ATTEMPTS = 3
logger = logging.getLogger(__name__)


VALID_TRANSITIONS = {
    "failed": {"paid", "refunded"},
    "paid": {"refunded"},
    "refunded": set(),
}

EVENT_STATES = {
    "payment.captured": "paid",
    "payment.failed": "failed",
    "payment.refunded": "refunded",
}


def process_event(
    database_path: str,
    event_id: str,
    should_fail: Callable[[PaymentEvent], bool] = lambda _event: False,
    current_time: str | None = None,
) -> str:
    """Apply the order update and event status in one transaction or neither."""
    connection = connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        attempt_time = current_time or now()
        row = connection.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        if row is None or row["status"] in ("processed", "dead_lettered"):
            connection.rollback()
            if row is None:
                return "missing"
            return "already_processed" if row["status"] == "processed" else "dead_lettered"
        if row["status"] == "failed" and row["next_attempt_at"] and row["next_attempt_at"] > attempt_time:
            # Check after BEGIN IMMEDIATE: another worker cannot sneak in an early retry.
            connection.rollback()
            return "not_due"

        # Keep the writer lock while undoing a failed business operation.
        connection.execute("SAVEPOINT payment_effect")
        try:
            payload = json.loads(row["payload"])
            if not isinstance(payload, dict):
                raise ValueError("stored payment payload is not a JSON object")
            payload["occurred_at"] = row["occurred_at"]
            event = validate_event(payload)
            if should_fail(event):
                raise RuntimeError("simulated processing failure")
            _apply_payment_event(connection, event)
            connection.execute(
                "UPDATE events SET status = 'processed', processed_at = ?, last_error = NULL, next_attempt_at = NULL WHERE event_id = ?",
                (now(), event_id),
            )
        except sqlite3.OperationalError:
            # Infrastructure errors leave the receipt eligible for a later poll.
            raise
        except Exception as error:
            connection.execute("ROLLBACK TO payment_effect")
            connection.execute("RELEASE payment_effect")
            logger.warning("Payment event processing failed (%s)", type(error).__name__)
            attempts = row["attempts"] + 1
            if attempts >= MAX_ATTEMPTS:
                connection.execute(
                    "INSERT OR REPLACE INTO dead_letters(event_id, payload, attempts, last_error, created_at, dead_lettered_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (event_id, row["payload"], attempts, str(error), row["created_at"], now()),
                )
                connection.execute(
                    "UPDATE events SET status = 'dead_lettered', attempts = ?, last_error = ?, next_attempt_at = NULL WHERE event_id = ?",
                    (attempts, str(error), event_id),
                )
                connection.commit()
                return "dead_lettered"

            delay_seconds = 2 ** (attempts - 1)
            retry_at = (datetime.fromisoformat(current_time or now()) + timedelta(seconds=delay_seconds)).isoformat()
            connection.execute(
                "UPDATE events SET status = 'failed', attempts = ?, next_attempt_at = ?, last_error = ? WHERE event_id = ?",
                (attempts, retry_at, str(error), event_id),
            )
            connection.commit()
            return "failed"
        connection.commit()
        return "processed"
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def retry_due_events(
    database_path: str,
    should_fail: Callable[[PaymentEvent], bool] = lambda _event: False,
    current_time: str | None = None,
    *,
    stopping: Callable[[], bool] = lambda: False,
) -> list[str]:
    """Resume work left as received after a crash, plus scheduled failed work."""
    results = []
    for event_id in due_events(database_path, current_time or now()):
        if stopping():
            break
        try:
            results.append(process_event(database_path, event_id, should_fail, current_time))
        except Exception:
            logger.exception("Worker could not process a stored payment event")
            results.append("worker_error")
    return results


def _apply_payment_event(connection: sqlite3.Connection, event: PaymentEvent) -> None:
    """Apply only forward, time-ordered states for one order inside the event transaction."""
    target_state = EVENT_STATES[event["type"]]
    existing = connection.execute("SELECT * FROM orders WHERE order_id = ?", (event["order_id"],)).fetchone()
    if existing is None:
        connection.execute(
            "INSERT INTO orders(order_id, amount, status, last_event_occurred_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (event["order_id"], event["amount"], target_state, event["occurred_at"], now()),
        )
        return

    event_time = datetime.fromisoformat(event["occurred_at"])
    previous_time = datetime.fromisoformat(existing["last_event_occurred_at"])
    if event_time < previous_time:
        return  # A late gateway delivery must not undo a state based on a newer occurrence time.
    if target_state == existing["status"]:
        return  # Different delivery IDs can describe the same business action; keep its first local effect.
    # At equal times, the forward-only transitions give refund > paid > failed.
    if target_state not in VALID_TRANSITIONS[existing["status"]]:
        return  # Keep the recorded state; the event remains auditable in the events table.

    connection.execute(
        "UPDATE orders SET amount = ?, status = ?, last_event_occurred_at = ?, updated_at = ? WHERE order_id = ?",
        (event["amount"], target_state, event["occurred_at"], now(), event["order_id"]),
    )
