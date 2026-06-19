import asyncio
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s publisher: %(message)s")
logger = logging.getLogger("publisher")

TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
NUM_EVENTS = int(os.getenv("NUM_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.30"))  # fraction of sends that are dupes
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
TOPICS = os.getenv("TOPICS", "auth,payments,orders,system").split(",")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_event(topic: str, event_id: str) -> dict:
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": now_iso(),
        "source": "publisher-sim",
        "payload": {"seq": random.randint(1, 10**6), "note": "synthetic load test event"},
    }


def build_event_pool() -> list[dict]:
    """Builds NUM_EVENTS sends, where DUPLICATE_RATE of them re-send an
    event_id (topic, event_id) that was already generated earlier, then
    shuffles so duplicates are scattered through the stream (not just
    sent back-to-back) -- closer to a real at-least-once delivery pattern."""
    unique_count = int(NUM_EVENTS * (1 - DUPLICATE_RATE))
    dup_count = NUM_EVENTS - unique_count

    unique_ids = [(random.choice(TOPICS), str(uuid.uuid4())) for _ in range(unique_count)]
    events = [make_event(topic, eid) for topic, eid in unique_ids]

    for _ in range(dup_count):
        topic, eid = random.choice(unique_ids)
        events.append(make_event(topic, eid))

    random.shuffle(events)
    return events


async def send_batch(client: httpx.AsyncClient, sem: asyncio.Semaphore, batch: list[dict]) -> bool:
    async with sem:
        try:
            resp = await client.post(TARGET_URL, json=batch, timeout=10.0)
            return resp.status_code == 202
        except Exception:
            logger.exception("batch send failed")
            return False


async def main() -> None:
    events = build_event_pool()
    logger.info("generated %d sends (%.0f%% duplicate rate, %d topics)",
                len(events), DUPLICATE_RATE * 100, len(TOPICS))

    batches = [events[i:i + BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]
    sem = asyncio.Semaphore(CONCURRENCY)

    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[send_batch(client, sem, b) for b in batches])
    elapsed = time.perf_counter() - start

    ok = sum(1 for r in results if r)
    logger.info(
        "done: %d/%d batches accepted, %d events sent in %.2fs (%.1f events/sec)",
        ok, len(batches), len(events), elapsed, len(events) / elapsed if elapsed > 0 else 0,
    )


if __name__ == "__main__":
    asyncio.run(main())
