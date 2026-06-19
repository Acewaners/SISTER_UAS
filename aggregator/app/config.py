import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgres://user:pass@storage:5432/db")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379")
STREAM_NAME = os.getenv("STREAM_NAME", "events_stream")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "aggregator_workers")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "4"))
SERVICE_NAME = os.getenv("SERVICE_NAME", "aggregator-1")
