"""FastAPI entry point."""

import asyncio
import json
import logging
import math
import os
import sqlite3
from contextlib import asynccontextmanager
from hmac import compare_digest
from threading import Event
from typing import AsyncIterator, Callable

from fastapi import FastAPI, HTTPException, Query, Request

from app.database import initialize, is_busy
from app.processor import retry_due_events
from app.repository import EventConflict, dead_letters, insert_event, metrics
from app.security import is_valid_signature
from app.validation import PaymentEvent, validate_event

logger = logging.getLogger(__name__)
MAX_BODY_BYTES = 64 * 1024


def reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def finite_json_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("JSON number out of range")
    return number


def unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def create_app(
    database_path: str | None = None,
    webhook_secret: str | None = None,
    should_fail: Callable[[PaymentEvent], bool] = lambda _event: False,
    worker_poll_seconds: float | None = 1.0,
    operator_token: str | None = None,
) -> FastAPI:
    db_path = database_path or os.getenv("DATABASE_PATH") or "data/webhooks.db"
    webhook_secret = webhook_secret if webhook_secret is not None else os.getenv("WEBHOOK_SECRET")
    operator_token = operator_token if operator_token is not None else os.getenv("OPERATOR_TOKEN")

    def authorize_operator(request: Request) -> None:
        header = request.headers.get("Authorization", "")
        scheme, _, token = header.partition(" ")
        if not operator_token or scheme.lower() != "bearer" or not compare_digest(
            token.encode("utf-8"), operator_token.encode("utf-8")
        ):
            raise HTTPException(401, "operator authentication required", headers={"WWW-Authenticate": "Bearer"})

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not webhook_secret or not webhook_secret.strip():
            raise RuntimeError("WEBHOOK_SECRET must be configured")
        await asyncio.to_thread(initialize, db_path)
        if worker_poll_seconds is None:
            yield
            return
        stop = asyncio.Event()
        stop_batch = Event()

        async def retry_worker() -> None:
            while not stop.is_set():
                try:
                    await asyncio.to_thread(retry_due_events, db_path, should_fail, stopping=stop_batch.is_set)
                except Exception:
                    logger.exception("Webhook worker poll failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=worker_poll_seconds)
                except asyncio.TimeoutError:
                    pass

        task = asyncio.create_task(retry_worker())
        try:
            yield
        finally:
            stop_batch.set()
            stop.set()
            await task

    app = FastAPI(title="Idempotent Payment Webhook Processor", lifespan=lifespan)

    @app.post("/webhook", status_code=202)
    async def webhook(request: Request) -> dict[str, str]:
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_BODY_BYTES:
                raise HTTPException(413, "webhook body too large")
            body.extend(chunk)
        raw_body = bytes(body)
        signatures = request.headers.getlist("X-Signature")
        if not webhook_secret or len(signatures) != 1 or not is_valid_signature(raw_body, signatures[0], webhook_secret):
            raise HTTPException(status_code=401, detail="invalid signature")
        try:
            payload = json.loads(
                raw_body.decode("utf-8"), parse_constant=reject_json_constant,
                parse_float=finite_json_float, object_pairs_hook=unique_json_object,
            )
        except (ValueError, RecursionError) as error:
            raise HTTPException(status_code=400, detail="body must be valid JSON") from error
        try:
            event = validate_event(payload)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        try:
            inserted = await asyncio.to_thread(insert_event, db_path, event)
        except EventConflict as error:
            raise HTTPException(409, str(error)) from error
        except sqlite3.OperationalError as error:
            if is_busy(error):
                raise HTTPException(503, "database temporarily busy", headers={"Retry-After": "1"}) from error
            raise
        # The worker runs separately. A gateway gets an acknowledgement once its receipt is durable.
        return {"status": "accepted" if inserted else "already received"}

    @app.get("/dead-letters")
    def get_dead_letters(request: Request, limit: int = Query(100, ge=1, le=100), offset: int = Query(0, ge=0, le=2**63 - 1)) -> list[dict[str, object]]:
        authorize_operator(request)
        return [dict(row) for row in dead_letters(db_path, limit, offset)]

    @app.get("/metrics")
    def get_metrics(request: Request) -> dict[str, int]:
        authorize_operator(request)
        return metrics(db_path)

    app.state.retry_due_events = lambda: retry_due_events(db_path, should_fail)
    return app


app = create_app()
