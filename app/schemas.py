"""API 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from . import severity as sev


class EventIn(BaseModel):
    source: str = Field(min_length=1, max_length=256, examples=["edge-gateway-01"])
    event_id: str = Field(min_length=1, max_length=256, examples=["evt-0001"])
    occurred_at: datetime = Field(examples=["2026-09-20T08:00:00Z"])
    severity: str = Field(examples=["warning"])

    @field_validator("severity")
    @classmethod
    def _normalize_severity(cls, v: str) -> str:
        try:
            return sev.normalize(v)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc

    @field_validator("occurred_at")
    @classmethod
    def _assume_utc(cls, v: datetime) -> datetime:
        # 无时区的时间按 UTC 处理
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v


class SegmentOut(BaseModel):
    id: int
    source: str
    started_at: datetime
    ended_at: datetime
    max_severity: str
    event_count: int
    revision: int


class IngestResponse(BaseModel):
    deduplicated: bool
    segment: SegmentOut


class SegmentChangeOut(BaseModel):
    """变更流记录：upsert 携带段快照；merged 为被删段墓碑，指向保留段。"""

    change_id: int
    change_type: Literal["upsert", "merged"]
    segment_id: int
    revision: int
    merged_into: int | None = None
    segment: SegmentOut | None = None


class SegmentChangesOut(BaseModel):
    changes: list[SegmentChangeOut]
    # 下一页游标：作为 after 参数继续拉取；无新记录时与入参 after 相同
    next_after: int
