"""Validation for the small, gateway-neutral event contract."""

from datetime import datetime, timezone

SUPPORTED_EVENT_TYPES = {"payment.captured", "payment.failed", "payment.refunded"}


def validate_event(event: object) -> dict:
    """Return a normalized event or raise ValueError with a client-safe reason."""
    if not isinstance(event, dict):
        raise ValueError("event must be a JSON object")

    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip() or event_id != event_id.strip():
        raise ValueError("event_id must be a non-empty string without surrounding spaces")

    order_id = event.get("order_id")
    if not isinstance(order_id, str) or not order_id.strip() or order_id != order_id.strip() or len(order_id) > 64:
        raise ValueError("order_id must be a non-empty string of at most 64 characters")

    amount = event.get("amount")
    # Store money as integer minor units (for example, paise) to avoid float rounding.
    if isinstance(amount, bool) or not isinstance(amount, int) or amount <= 0:
        raise ValueError("amount must be a positive integer in minor units")

    event_type = event.get("type")
    if event_type not in SUPPORTED_EVENT_TYPES:
        raise ValueError(f"type must be one of: {', '.join(sorted(SUPPORTED_EVENT_TYPES))}")

    occurred_at = event.get("occurred_at")
    if not isinstance(occurred_at, str):
        raise ValueError("occurred_at must be an ISO-8601 timestamp with a timezone")
    try:
        parsed_time = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("occurred_at must be an ISO-8601 timestamp with a timezone") from error
    if parsed_time.tzinfo is None:
        raise ValueError("occurred_at must include a timezone")

    # One UTC representation makes SQLite's text timestamp comparison reliable.
    normalized = dict(event)
    normalized["occurred_at"] = parsed_time.astimezone(timezone.utc).isoformat()
    return normalized
