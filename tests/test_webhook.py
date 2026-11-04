import hashlib
import hmac
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.processor import process_event, retry_due_events
from app.repository import get_event

SECRET = "test-secret"
T0 = "2026-11-04T12:00:00+00:00"


def signed_headers(event):
    body = json.dumps(event, separators=(",", ":")).encode()
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return body, {"X-Signature": signature, "content-type": "application/json"}


@pytest.fixture
def database_path():
    directory = Path("tests/.test-data")
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory / f"{uuid4()}.db")


@pytest.fixture
def client(database_path):
    # Long polling makes HTTP tests deterministic; worker behaviour is tested directly below.
    app = create_app(database_path, SECRET, worker_poll_seconds=3600)
    with TestClient(app) as test_client:
        yield test_client


def post_event(client, payload):
    body, headers = signed_headers(payload)
    return client.post("/webhook", content=body, headers=headers)


def event(event_id="evt_1", **changes):
    payload = {
        "event_id": event_id,
        "type": "payment.captured",
        "amount": 500,
        "order_id": "order_1",
        "occurred_at": "2026-11-04T12:00:00Z",
    }
    payload.update(changes)
    return payload


def order(database_path):
    connection = sqlite3.connect(database_path)
    row = connection.execute("SELECT order_id, amount, status FROM orders WHERE order_id = 'order_1'").fetchone()
    connection.close()
    return row


def test_new_event_is_durable_then_processed_by_worker(client, database_path):
    response = post_event(client, event())
    assert response.status_code == 202
    assert response.json() == {"status": "accepted"}
    assert get_event(database_path, "evt_1")["status"] == "received"
    assert process_event(database_path, "evt_1", current_time=T0) == "processed"
    assert order(database_path) == ("order_1", 500, "paid")


def test_duplicate_awaiting_processing_is_acknowledged_and_skipped(client, database_path):
    post_event(client, event())
    response = post_event(client, event())
    assert response.status_code == 202
    assert response.json() == {"status": "already received"}
    assert client.get("/metrics").json()["duplicates_skipped"] == 1
    assert get_event(database_path, "evt_1")["status"] == "received"


def test_bad_signature_is_rejected(client):
    assert client.post("/webhook", json=event(), headers={"X-Signature": "wrong"}).status_code == 401


@pytest.mark.parametrize("bad_event_id", ["", "  ", None])
def test_empty_or_null_event_id_is_rejected(client, bad_event_id):
    assert post_event(client, event(event_id=bad_event_id)).status_code == 422


def test_invalid_type_amount_and_order_id_are_rejected(client):
    assert post_event(client, event(type="payment.pending")).status_code == 422
    assert post_event(client, event(amount=0)).status_code == 422
    assert post_event(client, event(order_id=" ")).status_code == 422


def test_payment_state_transitions_and_out_of_order_event(client, database_path):
    post_event(client, event("captured", occurred_at="2026-11-04T12:00:00Z"))
    process_event(database_path, "captured", current_time=T0)
    post_event(client, event("refunded", type="payment.refunded", occurred_at="2026-11-04T14:00:00Z"))
    process_event(database_path, "refunded", current_time="2026-11-04T14:00:00+00:00")
    post_event(client, event("late-failed", type="payment.failed", occurred_at="2026-11-04T13:00:00Z"))
    process_event(database_path, "late-failed", current_time="2026-11-04T15:00:00+00:00")
    assert order(database_path) == ("order_1", 500, "refunded")


def test_different_event_ids_for_same_action_do_not_change_order(client, database_path):
    post_event(client, event("capture-one", amount=500))
    process_event(database_path, "capture-one", current_time=T0)
    post_event(client, event("capture-two", amount=999, occurred_at="2026-11-04T13:00:00Z"))
    process_event(database_path, "capture-two", current_time="2026-11-04T13:00:00+00:00")
    assert order(database_path) == ("order_1", 500, "paid")


def test_received_event_survives_restart_and_is_recovered(database_path):
    first_app = create_app(database_path, SECRET, worker_poll_seconds=3600)
    with TestClient(first_app) as first_client:
        post_event(first_client, event())
    second_app = create_app(database_path, SECRET, worker_poll_seconds=3600)
    with TestClient(second_app):
        # Startup's worker has already found the received event in the persistent database.
        assert get_event(database_path, "evt_1")["status"] == "processed"
    assert order(database_path) == ("order_1", 500, "paid")


def test_multiple_workers_process_one_event_once(client, database_path):
    post_event(client, event())
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: process_event(database_path, "evt_1", current_time=T0), range(5)))
    assert results.count("processed") == 1
    assert results.count("already_processed") == 4
    assert order(database_path) == ("order_1", 500, "paid")


def test_ten_concurrent_duplicate_requests_create_one_event(client, database_path):
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(lambda _: post_event(client, event("evt_same")).json()["status"], range(10)))
    assert results.count("accepted") == 1
    assert results.count("already received") == 9
    assert process_event(database_path, "evt_same", current_time=T0) == "processed"
    assert client.get("/metrics").json()["duplicates_skipped"] == 9
    assert order(database_path) == ("order_1", 500, "paid")


def test_retry_schedule_and_dead_lettering_are_persistent(client, database_path):
    post_event(client, event())
    always_fail = lambda _event: True
    assert process_event(database_path, "evt_1", always_fail, T0) == "failed"
    assert get_event(database_path, "evt_1")["attempts"] == 1
    assert process_event(database_path, "evt_1", always_fail, T0) == "not_due"
    assert process_event(database_path, "evt_1", always_fail, "2026-11-04T12:00:01+00:00") == "failed"
    assert process_event(database_path, "evt_1", always_fail, "2026-11-04T12:00:03+00:00") == "dead_lettered"
    assert get_event(database_path, "evt_1")["attempts"] == 3
    assert len(client.get("/dead-letters").json()) == 1


def test_transient_failure_then_worker_retry_succeeds(client, database_path):
    post_event(client, event())
    fail_once = {"value": True}
    assert process_event(database_path, "evt_1", lambda _: fail_once["value"], T0) == "failed"
    fail_once["value"] = False
    assert retry_due_events(database_path, lambda _: False, "2026-11-04T12:00:01+00:00") == ["processed"]
    assert order(database_path) == ("order_1", 500, "paid")
