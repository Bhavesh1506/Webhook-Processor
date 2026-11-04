"""FastAPI entry point."""

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request

from app.database import initialize
from app.processor import process_event, retry_due_events
from app.repository import dead_letters, increment_duplicate_counter, insert_event, metrics
from app.security import is_valid_signature


def create_app(database_path: str | None = None, webhook_secret: str | None = None, should_fail=lambda _event: False) -> FastAPI:
    database_path = database_path or os.getenv("DATABASE_PATH", "data/webhooks.db")
    webhook_secret = webhook_secret or os.getenv("WEBHOOK_SECRET", "development-secret")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        initialize(database_path)
        stop = asyncio.Event()

        async def retry_worker() -> None:
            while not stop.is_set():
                retry_due_events(database_path, should_fail)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(retry_worker())
        yield
        stop.set()
        await task

    app = FastAPI(title="Idempotent Payment Webhook Processor", lifespan=lifespan)

    @app.post("/webhook")
    async def webhook(request: Request):
        raw_body = await request.body()
        if not is_valid_signature(raw_body, request.headers.get("X-Signature"), webhook_secret):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            event = json.loads(raw_body)
        except json.JSONDecodeError as error:
            raise HTTPException(status_code=400, detail="body must be valid JSON") from error
        required = ("event_id", "type", "amount", "order_id")
        if not isinstance(event, dict) or any(key not in event for key in required):
            raise HTTPException(status_code=422, detail="event_id, type, amount, and order_id are required")

        if not insert_event(database_path, event):
            increment_duplicate_counter(database_path)
            return {"status": "already processed"}
        status = process_event(database_path, event["event_id"], should_fail)
        return {"status": "ok" if status == "processed" else status}

    @app.get("/dead-letters")
    def get_dead_letters():
        return [dict(row) for row in dead_letters(database_path)]

    @app.get("/metrics")
    def get_metrics():
        return metrics(database_path)

    app.state.retry_due_events = lambda: retry_due_events(database_path, should_fail)
    return app


app = create_app()
