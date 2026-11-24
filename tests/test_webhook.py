import hashlib
import hmac
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.database import initialize
from app.main import MAX_BODY_BYTES, create_app
from app.processor import process_event, retry_due_events
from app.repository import get_event

SECRET = "test-secret"
OPERATOR_HEADERS = {"Authorization": "Bearer test-operator-token"}
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
    app = create_app(database_path, SECRET, worker_poll_seconds=None, operator_token="test-operator-token")
    with TestClient(app, headers=OPERATOR_HEADERS) as test_client:
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
    first_app = create_app(database_path, SECRET, worker_poll_seconds=None)
    with TestClient(first_app) as first_client:
        post_event(first_client, event())
    second_app = create_app(database_path, SECRET, worker_poll_seconds=3600)
    with TestClient(second_app):
        deadline = monotonic() + 3
        while get_event(database_path, "evt_1")["status"] != "processed" and monotonic() < deadline:
            sleep(0.01)
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


def test_hmac_authenticates_exact_bytes(client, database_path):
    body, headers = signed_headers(event())
    assert client.post("/webhook", content=body + b" ", headers=headers).status_code == 401
    assert client.post("/webhook", content=body).status_code == 401
    assert client.post("/webhook", content=body, headers={"X-Signature": b"\xff" * 64}).status_code == 401
    assert get_event(database_path, "evt_1") is None
    assert client.post("/webhook", content=body, headers=headers).status_code == 202


@pytest.mark.parametrize("body", [b"{", b"\xff", b'{"type":1,"type":2}', b'{"extra":1e999}'])
def test_signed_malformed_json_returns_400(client, body):
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    assert client.post("/webhook", content=body, headers={"X-Signature": signature}).status_code == 400


@pytest.mark.parametrize("changes", [
    {"type": []}, {"type": {}}, {"event_id": "\ud800"}, {"order_id": "a\x00b"},
    {"amount": 2**63}, {"occurred_at": "0001-01-01T00:00:00+01:00"},
])
def test_signed_invalid_fields_return_422(client, changes):
    assert post_event(client, event(**changes)).status_code == 422


def test_oversize_rejected(client):
    assert client.post("/webhook", content=b"x" * (MAX_BODY_BYTES + 1)).status_code == 413


def test_secret_is_required_at_startup(database_path, monkeypatch):
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"), TestClient(create_app(database_path)):
        pytest.fail("startup should have failed")


@pytest.mark.parametrize("path", ["/metrics", "/dead-letters"])
def test_operator_endpoints_require_separate_token(client, path):
    assert client.get(path, headers={"Authorization": f"Bearer {SECRET}"}).status_code == 401
    assert client.get(path).status_code == 200


def test_reused_id_with_different_content_returns_conflict(client, database_path):
    assert post_event(client, event()).status_code == 202
    assert post_event(client, event(amount=900)).status_code == 409
    assert client.get("/metrics").json()["duplicates_skipped"] == 0
    process_event(database_path, "evt_1")
    assert order(database_path) == ("order_1", 500, "paid")


def test_failed_status_write_rolls_back_order_effect(client, database_path):
    post_event(client, event())
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_processed BEFORE UPDATE ON events WHEN NEW.status = 'processed' "
            "BEGIN SELECT RAISE(ABORT, 'simulated status update failure'); END"
        )
    assert process_event(database_path, "evt_1", current_time=T0) == "failed"
    assert order(database_path) is None
    assert get_event(database_path, "evt_1")["attempts"] == 1


def test_legacy_pending_event_uses_migrated_timestamp(database_path):
    payload = event()
    del payload["occurred_at"]
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "CREATE TABLE events (event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, status TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT, last_error TEXT, "
            "created_at TEXT NOT NULL, processed_at TEXT)"
        )
        connection.execute("INSERT INTO events VALUES (?, ?, 'received', 0, NULL, NULL, ?, NULL)",
                           ("evt_1", json.dumps(payload), T0))
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(initialize, [database_path] * 3))
    with TestClient(create_app(database_path, SECRET, worker_poll_seconds=None)):
        assert process_event(database_path, "evt_1") == "processed"
    assert order(database_path) == ("order_1", 500, "paid")


@pytest.mark.parametrize("actions", [("failed", "refunded", "captured"), ("refunded", "captured", "failed")])
def test_equal_time_refund_wins_in_either_order(client, database_path, actions):
    for action in actions:
        assert post_event(client, event(action, type=f"payment.{action}")).status_code == 202
        assert process_event(database_path, action) == "processed"
    assert order(database_path) == ("order_1", 500, "refunded")


def test_worker_quarantines_bad_stored_json_and_continues(client, database_path):
    assert post_event(client, event("broken")).status_code == 202
    assert post_event(client, event("healthy")).status_code == 202
    with sqlite3.connect(database_path) as connection:
        connection.execute("UPDATE events SET payload = '{' WHERE event_id = 'broken'")
    assert retry_due_events(database_path, current_time=T0) == ["failed", "processed"]
    assert retry_due_events(database_path, current_time="2026-11-04T12:00:01+00:00") == ["failed"]
    assert retry_due_events(database_path, current_time="2026-11-04T12:00:03+00:00") == ["dead_lettered"]
    assert client.get("/dead-letters").json()[0]["event_id"] == "broken"


def test_busy_ingestion_returns_retryable_response(client, database_path, monkeypatch):
    from app import repository

    connect = repository.connect

    def immediate_lock_failure(path):
        connection = connect(path)
        connection.execute("PRAGMA busy_timeout = 0")
        return connection

    monkeypatch.setattr(repository, "connect", immediate_lock_failure)
    writer = sqlite3.connect(database_path)
    try:
        writer.execute("BEGIN IMMEDIATE")
        response = post_event(client, event())
        assert response.status_code == 503
        assert response.headers["Retry-After"] == "1"
    finally:
        writer.close()
    assert get_event(database_path, "evt_1") is None
    assert post_event(client, event()).status_code == 202
