"""Provider-independent match event contract. All timestamps represent UTC instants."""

import hashlib
import json
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class MatchEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=False)
    event_id: str = Field(min_length=1, max_length=180)
    match_id: str = Field(min_length=1, max_length=120)
    provider: Literal["synthetic", "football-data", "pandascore"]
    sport: Literal["football", "valorant"]
    competition: str = Field(min_length=1, max_length=120)
    revision: int = Field(ge=1, strict=True)
    team_a_id: str = Field(min_length=1)
    team_a: str = Field(min_length=1, max_length=100)
    team_b_id: str = Field(min_length=1)
    team_b: str = Field(min_length=1, max_length=100)
    start_time: datetime
    observed_at: datetime
    completed_at: datetime | None = None
    status: Literal["scheduled", "running", "finished", "cancelled", "postponed"]
    score_a: int | None = Field(default=None, ge=0, le=50, strict=True)
    score_b: int | None = Field(default=None, ge=0, le=50, strict=True)
    best_of: int = Field(default=1, ge=1, le=9, strict=True)

    @field_validator("start_time", "observed_at", "completed_at")
    @classmethod
    def utc(cls, value):
        if value is not None:
            if value.tzinfo is None:
                raise ValueError("Timezone-aware timestamps are required")
            return value.astimezone(timezone.utc)
        return value

    @model_validator(mode="after")
    def consistency(self):
        if self.team_a_id == self.team_b_id:
            raise ValueError("A team cannot play itself")
        if self.status == "finished":
            if self.score_a is None or self.score_b is None or self.completed_at is None:
                raise ValueError("Finished matches require scores and completed_at")
            if not self.start_time < self.completed_at <= self.observed_at:
                raise ValueError("Results cannot be available before completion")
            if self.sport == "valorant":
                needed = self.best_of // 2 + 1
                if self.best_of % 2 == 0 or max(self.score_a, self.score_b) != needed:
                    raise ValueError("VALORANT series score does not match best_of")
                if self.score_a == self.score_b or min(self.score_a, self.score_b) >= needed:
                    raise ValueError("A completed VALORANT series has exactly one winner")
        elif self.completed_at is not None:
            raise ValueError("Only finished matches can have completed_at")
        return self

    def row(self):
        row = self.model_dump(mode="json")
        for name in ("start_time", "observed_at", "completed_at"):
            value = getattr(self, name)
            row[name] = value.isoformat(timespec="microseconds").replace("+00:00", "Z") if value else None
        return row


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: dict) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def outcome(match: dict) -> str:
    if match["status"] != "finished":
        raise ValueError("Outcome is unavailable")
    return "a" if match["score_a"] > match["score_b"] else "b" if match["score_b"] > match["score_a"] else "draw"
