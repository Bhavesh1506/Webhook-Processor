"""Validation for the small, gateway-neutral event contract."""

from datetime import datetime, timezone
from typing import TypedDict, cast

SUPPORTED_EVENT_TYPES = {"payment.captured", "payment.failed", "payment.refunded"}


class PaymentEvent(TypedDict):
    event_id: str
    order_id: str
    type: str
    amount: int
    occurred_at: str


def valid_id(value: object, limit: int) -> bool:
    return (
        isinstance(value, str) and 0 < len(value) <= limit
        and value == value.strip() and value.isprintable()
    )


def validate_event(event: object) -> PaymentEvent:
    """Return a normalized event or raise ValueError with a client-safe reason."""
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")

    event_id = event.get("event_id")
    if not valid_id(event_id, 255):
        raise ValueError("event_id must be a printable string of 1–255 characters without surrounding spaces")

    order_id = event.get("order_id")
    if not valid_id(order_id, 64):
        raise ValueError("order_id must be a printable string of 1–64 characters without surrounding spaces")

    amount = event.get("amount")
    # Store money as integer minor units (for example, paise) to avoid float rounding.
    if isinstance(amount, bool) or not isinstance(amount, int) or not 0 < amount <= 2**63 - 1:
        raise ValueError("amount must be a positive SQLite integer in minor units")

    event_type = event.get("type")
    if not isinstance(event_type, str) or event_type not in SUPPORTED_EVENT_TYPES:
        raise ValueError(f"type must be one of: {', '.join(sorted(SUPPORTED_EVENT_TYPES))}")

    occurred_at = event.get("occurred_at")
    if not isinstance(occurred_at, str):
        raise ValueError("occurred_at must be an ISO-8601 timestamp with a timezone")
    try:
        parsed_time = datetime.fromisoformat(occurred_at)
        if parsed_time.tzinfo is None:
            raise ValueError("missing timezone")
        utc_time = parsed_time.astimezone(timezone.utc).isoformat()
    except (ValueError, OverflowError) as error:
        raise ValueError("occurred_at must be an ISO-8601 timestamp with a timezone") from error

    # One UTC representation makes SQLite's text timestamp comparison reliable.
    normalized = dict(event)
    normalized["occurred_at"] = utc_time
    return cast(PaymentEvent, normalized)
