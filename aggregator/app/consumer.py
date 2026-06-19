import asyncio
import json
import logging

import redis.asyncio as aioredis

from . import config, db

logger = logging.getLogger("aggregator.consumer")


async def ensure_stream_and_group(r: aioredis.Redis) -> None:
    try:
        await r.xgroup_create(config.STREAM_NAME, config.CONSUMER_GROUP, id="0", mkstream=True)
        logger.info("created consumer group %s on stream %s", config.CONSUMER_GROUP, config.STREAM_NAME)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info("consumer group %s already exists", config.CONSUMER_GROUP)
        else:
            raise


async def publish_to_stream(r: aioredis.Redis, event: dict) -> None:
    """At-least-once hand-off to the broker, with a small retry/backoff.
    If this keeps failing the event is still safe: it's already durably
    recorded in received_log (Postgres), so nothing is lost -- it just
    won't be deduped/processed until retried (e.g. on next publish of the
    same event_id, or a manual replay from received_log)."""
    backoff = 0.1
    for attempt in range(5):
        try:
            await r.xadd(config.STREAM_NAME, {"data": json.dumps(event)})
            return
        except Exception:
            logger.warning("xadd failed (attempt %d), retrying in %.2fs", attempt + 1, backoff)
            await asyncio.sleep(backoff)
            backoff *= 2
    logger.error("giving up publishing event %s/%s to stream", event.get("topic"), event.get("event_id"))


async def consumer_worker(worker_name: str, r: aioredis.Redis, pool) -> None:
    """One of N concurrent consumers in the same consumer group. Redis
    guarantees a given stream entry is only delivered to ONE consumer in
    the group at a time (until acked or claimed after timeout), and the
    UNIQUE constraint in Postgres guarantees that even if delivery were
    ever duplicated (crash + redelivery), processing stays idempotent."""
    logger.info("worker %s starting", worker_name)
    while True:
        try:
            resp = await r.xreadgroup(
                groupname=config.CONSUMER_GROUP,
                consumername=worker_name,
                streams={config.STREAM_NAME: ">"},
                count=10,
                block=2000,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("worker %s: xreadgroup error, backing off", worker_name)
            await asyncio.sleep(1)
            continue

        if not resp:
            continue

        for _stream, messages in resp:
            for message_id, fields in messages:
                try:
                    event = json.loads(fields["data"])
                    result = await db.process_event(pool, event, worker_name)
                    if result == "duplicate":
                        logger.info(
                            "worker %s: DUPLICATE dropped topic=%s event_id=%s",
                            worker_name, event["topic"], event["event_id"],
                        )
                    else:
                        logger.info(
                            "worker %s: processed topic=%s event_id=%s",
                            worker_name, event["topic"], event["event_id"],
                        )
                    await r.xack(config.STREAM_NAME, config.CONSUMER_GROUP, message_id)
                except Exception:
                    logger.exception("worker %s: failed processing message %s, leaving unacked for retry",
                                      worker_name, message_id)


async def start_workers(r: aioredis.Redis, pool) -> list[asyncio.Task]:
    await ensure_stream_and_group(r)
    tasks = []
    for i in range(config.NUM_WORKERS):
        name = f"{config.SERVICE_NAME}-worker-{i}"
        tasks.append(asyncio.create_task(consumer_worker(name, r, pool)))
    return tasks
