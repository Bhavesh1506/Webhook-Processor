"""Webhook authentication helpers."""

import hashlib
import hmac


def is_valid_signature(raw_body: bytes, supplied_signature: str | None, secret: str) -> bool:
    """Compare digests safely so signature bytes are never parsed or altered."""
    if not supplied_signature or len(supplied_signature) != 64 or not supplied_signature.isascii():
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, supplied_signature)
