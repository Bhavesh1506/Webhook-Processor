"""FastAPI entry point."""

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from app.database import initialize
from app.processor import retry_due_events
from app.repository import dead_letters, increment_duplicate_counter, insert_event, metrics
from app.security import is_valid_signature
from app.validation import validate_event


def create_app(
    database_path: str | None = None,
    webhook_secret: str | None = None,
    should_fail=lambda _event: False,
    worker_poll_seconds: float = 1.0,
) -> FastAPI:
    database_path = database_path or os.getenv("DATABASE_PATH", "data/webhooks.db")
    webhook_secret = webhook_secret or os.getenv("WEBHOOK_SECRET", "development-secret")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        initialize(database_path)
        stop = asyncio.Event()

        async def retry_worker() -> None:
            while not stop.is_set():
                try:
                    # One broken event must not make the restaurant kitchen close for every later order.
                    retry_due_events(database_path, should_fail)
                except Exception:
                    # Processing failures are persisted per event; an unexpected worker error is retried next poll.
                    pass
                try:
                    await asyncio.wait_for(stop.wait(), timeout=worker_poll_seconds)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(retry_worker())
        yield
        stop.set()
        await task

    app = FastAPI(title="Idempotent Payment Webhook Processor", lifespan=lifespan)

    @app.post("/webhook", status_code=202)
    async def webhook(request: Request):
        raw_body = await request.body()
        if not is_valid_signature(raw_body, request.headers.get("X-Signature"), webhook_secret):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            event = json.loads(raw_body)
        except json.JSONDecodeError as error:
            raise HTTPException(status_code=400, detail="body must be valid JSON") from error
        try:
            event = validate_event(event)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        if not insert_event(database_path, event):
            increment_duplicate_counter(database_path)
            return {"status": "already received"}
        # The worker runs separately. A gateway gets an acknowledgement once its receipt is durable.
        return {"status": "accepted"}

    @app.get("/dead-letters")
    def get_dead_letters():
        return [dict(row) for row in dead_letters(database_path)]

    @app.get("/metrics")
    def get_metrics():
        return metrics(database_path)

    app.state.retry_due_events = lambda: retry_due_events(database_path, should_fail)
    return app


app = create_app()
