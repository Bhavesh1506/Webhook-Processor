import hashlib
import hmac
import json
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.processor import process_event
from app.repository import get_event

SECRET = "test-secret"


def signed_headers(event):
    body = json.dumps(event, separators=(",", ":")).encode()
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return body, {"X-Signature": signature, "content-type": "application/json"}


@pytest.fixture
def database_path():
    # Avoid Windows' shared temp directory, which can be owned by another account.
    directory = Path("tests/.test-data")
    directory.mkdir(parents=True, exist_ok=True)
    return str(directory / f"{uuid4()}.db")


@pytest.fixture
def client(database_path):
    app = create_app(database_path, SECRET)
    with TestClient(app) as test_client:
        yield test_client


def post_event(client, event):
    body, headers = signed_headers(event)
    return client.post("/webhook", content=body, headers=headers)


def event(event_id="evt_1"):
    return {"event_id": event_id, "type": "payment.captured", "amount": 500, "order_id": "order_1"}


def test_new_event_is_processed(client):
    assert post_event(client, event()).json() == {"status": "ok"}
    assert client.get("/metrics").json()["processed"] == 1


def test_duplicate_is_skipped(client):
    post_event(client, event())
    assert post_event(client, event()).json() == {"status": "already processed"}
    assert client.get("/metrics").json()["duplicates_skipped"] == 1


def test_bad_signature_is_rejected(client):
    response = client.post("/webhook", json=event(), headers={"X-Signature": "wrong"})
    assert response.status_code == 401


def test_failure_retries_then_processes(database_path):
    fail_once = {"value": True}
    app = create_app(database_path, SECRET, lambda _event: fail_once["value"])
    with TestClient(app) as client:
        response = post_event(client, event())
        assert response.json()["status"] == "failed"
        fail_once["value"] = False
        # Calling the worker directly avoids waiting for the one-second backoff in a unit test.
        process_event(database_path, "evt_1", lambda _event: False)
        assert get_event(database_path, "evt_1")["status"] == "processed"


def test_dead_letter_after_max_attempts(database_path):
    app = create_app(database_path, SECRET, lambda _event: True)
    with TestClient(app) as client:
        post_event(client, event())
        process_event(database_path, "evt_1", lambda _event: True)
        process_event(database_path, "evt_1", lambda _event: True)
        assert len(client.get("/dead-letters").json()) == 1


def test_concurrent_duplicates(database_path):
    # SQLite's primary key is the final authority even across independent clients.
    from concurrent.futures import ThreadPoolExecutor
    app = create_app(database_path, SECRET)
    with TestClient(app) as client:
        with ThreadPoolExecutor(max_workers=5) as pool:
            responses = list(pool.map(lambda _n: post_event(client, event("evt_same")).json()["status"], range(5)))
    assert responses.count("ok") == 1
    assert responses.count("already processed") == 4
