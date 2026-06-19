import json
import logging
from datetime import datetime


import asyncpg

from . import config

logger = logging.getLogger("aggregator.db")
def _to_timestamptz(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

SCHEMA_SQL = """
-- audit log: every event the API accepted at the HTTP boundary, BEFORE dedup.
-- this lets us reconstruct "received" totals even if a worker crashes mid-processing.
CREATE TABLE IF NOT EXISTS received_log (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_received_log_topic ON received_log (topic);

-- the dedup store. (topic, event_id) is UNIQUE -> this single constraint is what
-- makes idempotency atomic under concurrency, see report Bab 8 for rationale.
CREATE TABLE IF NOT EXISTS processed_events (
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_by TEXT,
    PRIMARY KEY (topic, event_id)
);
CREATE INDEX IF NOT EXISTS idx_processed_events_topic ON processed_events (topic);

-- explicit duplicate audit trail (separate from processed_events on purpose,
-- so we can prove *when* and *how many times* a dup was seen).
CREATE TABLE IF NOT EXISTS duplicate_log (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    event_id TEXT NOT NULL,
    detected_by TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn=config.DATABASE_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
    logger.info("database schema ready")
    return pool


async def insert_received_batch(pool: asyncpg.Pool, events: list[dict]) -> None:
    """Atomic batch insert for the audit log: all rows in ONE transaction.
    If anything fails, the whole batch is rolled back (batch-atomic guarantee
    for the *acceptance* step, independent of later idempotent processing)."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                """
                INSERT INTO received_log (topic, event_id, ts, source, payload)
                VALUES ($1, $2, $3, $4, $5)
                """,
                [
                    (e["topic"], e["event_id"], _to_timestamptz(e["timestamp"]), e["source"], json.dumps(e["payload"]))
                    for e in events
                ],
            )


async def process_event(pool: asyncpg.Pool, event: dict, worker_name: str) -> str:
    """The idempotent-consumer core. Returns 'processed' or 'duplicate'.

    The INSERT ... ON CONFLICT DO NOTHING is what gives us atomic dedup:
    Postgres enforces the (topic, event_id) UNIQUE constraint at the storage
    layer, so even under plain READ COMMITTED, two concurrent transactions
    inserting the same key cannot both succeed -- one blocks/loses the race
    and sees the conflict. We don't need SERIALIZABLE here because we have
    no multi-row invariant beyond "this key is unique"; SERIALIZABLE would
    only add retry overhead without closing any additional race.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO processed_events (topic, event_id, ts, source, payload, processed_by)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (topic, event_id) DO NOTHING
                RETURNING topic
                """,
                event["topic"], event["event_id"], _to_timestamptz(event["timestamp"]),
                event["source"], json.dumps(event["payload"]), worker_name,
            )
            if row is not None:
                return "processed"

            await conn.execute(
                "INSERT INTO duplicate_log (topic, event_id, detected_by) VALUES ($1, $2, $3)",
                event["topic"], event["event_id"], worker_name,
            )
            return "duplicate"


async def get_events(pool: asyncpg.Pool, topic: str | None, limit: int) -> list[dict]:
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch(
                "SELECT topic, event_id, ts, source, payload, processed_at FROM processed_events "
                "WHERE topic = $1 ORDER BY processed_at DESC LIMIT $2",
                topic, limit,
            )
        else:
            rows = await conn.fetch(
                "SELECT topic, event_id, ts, source, payload, processed_at FROM processed_events "
                "ORDER BY processed_at DESC LIMIT $1",
                limit,
            )
    return [
        {
            "topic": r["topic"],
            "event_id": r["event_id"],
            "timestamp": r["ts"].isoformat(),
            "source": r["source"],
            "payload": json.loads(r["payload"]),
            "processed_at": r["processed_at"].isoformat(),
        }
        for r in rows
    ]


async def get_stats(pool: asyncpg.Pool) -> dict:
    """Stats are computed via COUNT(*) aggregates rather than manually
    maintained counters. This is a deliberate design choice: it sidesteps
    the lost-update problem entirely (there is no counter row to race on),
    at the cost of an extra scan/index-count per call -- an acceptable
    trade-off at this scale. See report Bab 8 for the alternative
    (UPDATE ... SET count = count + 1) and why we didn't need it."""
    async with pool.acquire() as conn:
        received = await conn.fetchval("SELECT COUNT(*) FROM received_log")
        unique_processed = await conn.fetchval("SELECT COUNT(*) FROM processed_events")
        duplicate_dropped = await conn.fetchval("SELECT COUNT(*) FROM duplicate_log")
        topics = await conn.fetch("SELECT DISTINCT topic FROM processed_events ORDER BY topic")
    return {
        "received": received,
        "unique_processed": unique_processed,
        "duplicate_dropped": duplicate_dropped,
        "topics": [r["topic"] for r in topics],
    }
