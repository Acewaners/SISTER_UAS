from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field, field_validator


class EventIn(BaseModel):
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: str
    source: str = Field(..., min_length=1, max_length=255)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def validate_iso8601(cls, v: str) -> str:
        # Accepts e.g. 2026-06-19T10:00:00Z or with offset
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp must be a valid ISO8601 string") from exc
        return v


# /publish accepts either a single event or a batch (list) of events
PublishPayload = Union[EventIn, List[EventIn]]


class StatsOut(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
