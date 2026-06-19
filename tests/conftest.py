import asyncio
import os
import time
import uuid

import httpx
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8082")


@pytest.fixture(scope="session")
def base_url():
    # fail fast with a clear message instead of a wall of connection errors
    for _ in range(30):
        try:
            r = httpx.get(f"{BASE_URL}/health", timeout=2.0)
            if r.status_code == 200:
                return BASE_URL
        except Exception:
            pass
        time.sleep(1)
    pytest.fail(
        f"aggregator not reachable at {BASE_URL}/health -- "
        "is `docker compose up` running?"
    )


def new_event(topic="test-topic", event_id=None, payload=None):
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": "2026-06-19T10:00:00Z",
        "source": "pytest",
        "payload": payload or {"k": "v"},
    }


def wait_until(predicate, timeout=10.0, interval=0.3):
    """Poll a predicate until True or timeout. Needed because processing
    is asynchronous (API accepts -> broker -> worker processes)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    return last
