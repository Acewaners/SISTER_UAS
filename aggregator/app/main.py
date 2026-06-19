import logging
import time
from typing import List, Optional, Union

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from . import config, consumer, db
from .models import EventIn, StatsOut

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("aggregator.main")

app = FastAPI(title="Log Aggregator", version="1.0.0")

app.state.start_time = time.time()


@app.on_event("startup")
async def startup() -> None:
    app.state.pool = await db.create_pool()
    app.state.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    app.state.worker_tasks = await consumer.start_workers(app.state.redis, app.state.pool)
    logger.info("aggregator startup complete: %d consumer workers running", len(app.state.worker_tasks))


@app.on_event("shutdown")
async def shutdown() -> None:
    for t in app.state.worker_tasks:
        t.cancel()
    await app.state.pool.close()
    await app.state.redis.close()


class PublishResponse(BaseModel):
    accepted: int
    message: str


@app.post("/publish", status_code=202, response_model=PublishResponse)
async def publish(payload: Union[EventIn, List[EventIn]]):
    events = payload if isinstance(payload, list) else [payload]

    if len(events) == 0:
        raise HTTPException(status_code=422, detail="batch must contain at least one event")

    event_dicts = [e.model_dump() for e in events]

    # 1. audit log write: whole batch in one transaction (batch-atomic).
    try:
        await db.insert_received_batch(app.state.pool, event_dicts)
    except Exception:
        logger.exception("failed to persist received batch")
        raise HTTPException(status_code=500, detail="failed to persist received batch")

    # 2. hand off to broker for async idempotent processing (at-least-once).
    for e in event_dicts:
        await consumer.publish_to_stream(app.state.redis, e)

    return PublishResponse(accepted=len(events), message=f"{len(events)} event(s) accepted for processing")


@app.get("/events")
async def list_events(topic: Optional[str] = None, limit: int = Query(default=100, le=1000)):
    events = await db.get_events(app.state.pool, topic, limit)
    return {"count": len(events), "events": events}


@app.get("/stats", response_model=StatsOut)
async def stats():
    s = await db.get_stats(app.state.pool)
    return StatsOut(
        received=s["received"],
        unique_processed=s["unique_processed"],
        duplicate_dropped=s["duplicate_dropped"],
        topics=s["topics"],
        uptime_seconds=round(time.time() - app.state.start_time, 2),
    )


@app.get("/health")
async def health():
    try:
        async with app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        await app.state.redis.ping()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"not ready: {e}")
