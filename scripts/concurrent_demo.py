"""Send ten identical signed deliveries to a running local webhook service."""

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import cast
from uuid import uuid4


def request(url: str, body: bytes, signature: str) -> dict[str, str]:
    webhook = urllib.request.Request(
        f"{url}/webhook", body, method="POST", headers={"Content-Type": "application/json", "X-Signature": signature}
    )
    with urllib.request.urlopen(webhook, timeout=10) as response:
        return cast(dict[str, str], json.loads(response.read()))


def metrics(url: str, operator_token: str) -> dict[str, int]:
    query = urllib.request.Request(f"{url}/metrics", headers={"Authorization": f"Bearer {operator_token}"})
    with urllib.request.urlopen(query, timeout=10) as response:
        return cast(dict[str, int], json.loads(response.read()))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--secret", default=os.getenv("WEBHOOK_SECRET"))
    arguments = parser.parse_args()
    operator_token = os.getenv("OPERATOR_TOKEN")
    if not arguments.secret or not operator_token:
        parser.error("set WEBHOOK_SECRET and OPERATOR_TOKEN in this terminal")

    event = {
        "event_id": f"demo-{uuid4()}", "type": "payment.captured", "amount": 500,
        "order_id": f"demo-order-{uuid4()}", "occurred_at": datetime.now(timezone.utc).isoformat(),
    }
    body = json.dumps(event, separators=(",", ":")).encode()
    signature = hmac.new(arguments.secret.encode(), body, hashlib.sha256).hexdigest()
    before = metrics(arguments.url, operator_token)
    with ThreadPoolExecutor(max_workers=10) as pool:
        outcomes = list(pool.map(lambda _: request(arguments.url, body, signature)["status"], range(10)))

    for _ in range(20):
        after = metrics(arguments.url, operator_token)
        if after["processed"] == before["processed"] + 1:
            break
        time.sleep(0.1)
    else:
        after = metrics(arguments.url, operator_token)

    passed = outcomes.count("accepted") == 1 and outcomes.count("already received") == 9 and after["processed"] == before["processed"] + 1
    print({"accepted": outcomes.count("accepted"), "duplicates_skipped": outcomes.count("already received"), "processed": after["processed"] - before["processed"]})
    if not passed:
        raise SystemExit("Demo failed: inspect the service logs and metrics above.")
